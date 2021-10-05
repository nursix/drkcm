# -*- coding: utf-8 -*-

""" Data Import Tools

    @copyright: 2011-2021 (c) Sahana Software Foundation
    @license: MIT

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

__all__ = ("S3ImportJob",
           "S3ImportItem",
           "S3Duplicate",
           "S3BulkImporter",
           )

import datetime
import json
import pickle
import os
import sys
import uuid

from copy import deepcopy
from io import StringIO, BytesIO
from lxml import etree
from urllib import request as urllib2
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from gluon import current, IS_EMPTY_OR, SQLFORM
from gluon.storage import Storage
from gluon.tools import callback, fetch

from s3dal import Field

from ..controller import S3Request
from ..model import S3Resource
from ..tools import s3_utc, s3_get_foreign_key, s3_has_foreign_key, \
                    s3_str, IS_JSONS3

# =============================================================================
class S3ImportItem(object):
    """ Class representing an import item (=a single record) """

    METHOD = Storage(
        CREATE = "create",
        UPDATE = "update",
        DELETE = "delete",
        MERGE = "merge"
    )

    POLICY = Storage(
        THIS   = "THIS",            # keep local instance
        OTHER  = "OTHER",           # update unconditionally
        NEWER  = "NEWER",           # update if import is newer
        MASTER = "MASTER"           # update if import is master
    )

    # -------------------------------------------------------------------------
    def __init__(self, job):
        """
            Constructor

            @param job: the import job this item belongs to
        """

        self.job = job

        # Locking and error handling
        self.lock = False
        self.error = None

        # Identification
        self.item_id = uuid.uuid4() # unique ID for this item
        self.id = None
        self.uid = None

        # Data elements
        self.table = None
        self.tablename = None
        self.element = None
        self.data = None
        self.original = None
        self.components = []
        self.references = []
        self.load_components = []
        self.load_references = []
        self.parent = None
        self.skip = False

        # Conflict handling
        self.mci = 2
        self.mtime = datetime.datetime.utcnow()
        self.modified = True
        self.conflict = False

        # Allowed import methods
        self.strategy = job.strategy
        # Update and conflict resolution policies
        self.update_policy = job.update_policy
        self.conflict_policy = job.conflict_policy

        # Actual import method
        self.method = None

        self.onvalidation = None
        self.onaccept = None

        # Item import status flags
        self.accepted = None
        self.permitted = False
        self.committed = False

        # Writeback hook for circular references:
        # Items which need a second write to update references
        self.update = []

    # -------------------------------------------------------------------------
    def __repr__(self):
        """ Helper method for debugging """

        _str = "<S3ImportItem %s {item_id=%s uid=%s id=%s error=%s data=%s}>" % \
               (self.table, self.item_id, self.uid, self.id, self.error, self.data)
        return _str

    # -------------------------------------------------------------------------
    def parse(self,
              element,
              original = None,
              table = None,
              tree = None,
              files = None
              ):
        """
            Read data from a <resource> element

            @param element: the element
            @param table: the DB table
            @param tree: the import tree
            @param files: uploaded files

            @return: True if successful, False if not (sets self.error)
        """

        s3db = current.s3db
        xml = current.xml

        ERROR = xml.ATTRIBUTE["error"]

        self.element = element
        if table is None:
            tablename = element.get(xml.ATTRIBUTE["name"])
            table = s3db.table(tablename)
            if table is None:
                self.error = current.ERROR.BAD_RESOURCE
                element.set(ERROR, s3_str(self.error))
                return False
        else:
            tablename = table._tablename

        self.table = table
        self.tablename = tablename

        UID = xml.UID

        if original is None:
            original = S3Resource.original(table, element,
                                           mandatory = self._mandatory_fields())
        elif isinstance(original, str) and UID in table.fields:
            # Single-component update in add-item => load the original now
            query = (table[UID] == original)
            pkeys = set(fname for fname in table.fields if table[fname].unique)
            fields = S3Resource.import_fields(table, pkeys,
                                              mandatory = self._mandatory_fields())
            original = current.db(query).select(limitby=(0, 1), *fields).first()
        else:
            original = None

        postprocess = s3db.get_config(tablename, "xml_post_parse")
        data = xml.record(table, element,
                          files = files,
                          original = original,
                          postprocess = postprocess)

        if data is None:
            self.error = current.ERROR.VALIDATION_ERROR
            self.accepted = False
            if not element.get(ERROR, False):
                element.set(ERROR, s3_str(self.error))
            return False

        self.data = data

        MCI = xml.MCI
        MTIME = xml.MTIME

        self.uid = data.get(UID)
        if original is not None:

            self.original = original
            self.id = original[table._id.name]

            if not current.response.s3.synchronise_uuids and UID in original:
                self.uid = self.data[UID] = original[UID]

        if MTIME in data:
            self.mtime = data[MTIME]
        if MCI in data:
            self.mci = data[MCI]

        #current.log.debug("New item: %s" % self)
        return True

    # -------------------------------------------------------------------------
    def deduplicate(self):
        """
            Detect whether this is an update or a new record
        """

        table = self.table
        if table is None or self.id:
            return

        METHOD = self.METHOD
        CREATE = METHOD["CREATE"]
        UPDATE = METHOD["UPDATE"]
        DELETE = METHOD["DELETE"]
        MERGE = METHOD["MERGE"]

        xml = current.xml
        UID = xml.UID

        data = self.data
        if self.job.second_pass and UID in table.fields:
            uid = data.get(UID)
            if uid and not self.element.get(UID) and not self.original:
                # Previously identified original does no longer exist
                del data[UID]

        mandatory = self._mandatory_fields()

        if self.original is not None:
            original = self.original
        elif self.data:
            original = S3Resource.original(table,
                                           self.data,
                                           mandatory=mandatory,
                                           )
        else:
            original = None

        synchronise_uuids = current.response.s3.synchronise_uuids

        deleted = data[xml.DELETED]
        if deleted:
            if data[xml.REPLACEDBY]:
                self.method = MERGE
            else:
                self.method = DELETE

        self.uid = data.get(UID)

        if original is not None:

            # The original record could be identified by a unique-key-match,
            # so this must be an update
            self.id = original[table._id.name]

            if not deleted:
                self.method = UPDATE

        else:

            if UID in data and not synchronise_uuids:
                # The import item has a UUID but there is no match
                # in the database, so this must be a new record
                self.id = None
                if not deleted:
                    self.method = CREATE
                else:
                    # Nonexistent record to be deleted => skip
                    self.method = DELETE
                    self.skip = True
            else:
                # Use the resource's deduplicator to identify the original
                resolve = current.s3db.get_config(self.tablename, "deduplicate")
                if data and resolve:
                    resolve(self)

            if self.id and self.method in (UPDATE, DELETE, MERGE):
                # Retrieve the original
                fields = S3Resource.import_fields(table,
                                                  data,
                                                  mandatory=mandatory,
                                                  )
                original = current.db(table._id == self.id) \
                                  .select(limitby=(0, 1), *fields).first()

        # Retain the original UUID (except in synchronise_uuids mode)
        if original and not synchronise_uuids and UID in original:
            self.uid = data[UID] = original[UID]

        self.original = original

    # -------------------------------------------------------------------------
    def authorize(self):
        """
            Authorize the import of this item, sets self.permitted
        """

        if not self.table:
            return False

        auth = current.auth
        tablename = self.tablename

        # Check whether self.table is protected
        if not auth.override and tablename.split("_", 1)[0] in auth.PROTECTED:
            return False

        # Determine the method
        METHOD = self.METHOD
        if self.data.deleted is True:
            if self.data.deleted_rb:
                self.method = METHOD["MERGE"]
            else:
                self.method = METHOD["DELETE"]
            self.accepted = True if self.id else False
        elif self.id:
            if not self.original:
                fields = S3Resource.import_fields(self.table, self.data,
                                        mandatory=self._mandatory_fields())
                query = (self.table.id == self.id)
                self.original = current.db(query).select(limitby=(0, 1),
                                                         *fields).first()
            if self.original:
                self.method = METHOD["UPDATE"]
            else:
                self.method = METHOD["CREATE"]
        else:
            self.method = METHOD["CREATE"]

        # Set self.id
        if self.method == METHOD["CREATE"]:
            self.id = 0

        # Authorization
        authorize = current.auth.s3_has_permission
        if authorize:
            self.permitted = authorize(self.method,
                                       tablename,
                                       record_id=self.id)
        else:
            self.permitted = True

        return self.permitted

    # -------------------------------------------------------------------------
    def validate(self):
        """
            Validate this item (=record onvalidation), sets self.accepted
        """

        data = self.data

        if self.accepted is not None:
            return self.accepted
        if data is None or not self.table:
            self.accepted = False
            return False

        xml = current.xml
        ERROR = xml.ATTRIBUTE["error"]

        METHOD = self.METHOD
        DELETE = METHOD.DELETE
        MERGE = METHOD.MERGE

        # Detect update
        if not self.id:
            self.deduplicate()
        if self.accepted is False:
            # Item rejected by deduplicator (e.g. due to ambiguity)
            return False

        # Don't need to validate skipped or deleted records
        if self.skip or self.method in (DELETE, MERGE):
            self.accepted = True if self.id else False
            return True

        # Set dynamic defaults for new records
        if not self.id:
            self._dynamic_defaults(data)

        # Check for mandatory fields
        required_fields = self._mandatory_fields()

        all_fields = list(data.keys())

        failed_references = []
        items = self.job.items
        for reference in self.references:
            resolvable = resolved = True
            entry = reference.entry
            if entry and not entry.id:
                if entry.item_id:
                    item = items[entry.item_id]
                    if item.error:
                        relement = reference.element
                        if relement is not None:
                            # Repeat the errors from the referenced record
                            # in the <reference> element (better reasoning)
                            msg = "; ".join(xml.collect_errors(entry.element))
                            relement.set(ERROR, msg)
                        else:
                            resolvable = False
                        resolved = False
                else:
                    resolvable = resolved = False
            field = reference.field
            if isinstance(field, (tuple, list)):
                field = field[1]
            if resolved:
                all_fields.append(field)
            elif resolvable:
                # Both reference and referenced record are in the XML,
                # => treat foreign key as mandatory, and mark as failed
                if field not in required_fields:
                    required_fields.append(field)
                if field not in failed_references:
                    failed_references.append(field)

        missing = [fname for fname in required_fields
                         if fname not in all_fields]

        original = self.original
        if missing:
            if original:
                missing = [fname for fname in missing
                                 if fname not in original]
            if missing:
                fields = [f for f in missing
                            if f not in failed_references]
                if fields:
                    errors = ["%s: value(s) required" % ", ".join(fields)]
                else:
                    errors = []
                if failed_references:
                    fields = ", ".join(failed_references)
                    errors.append("%s: reference import(s) failed" %
                                  ", ".join(failed_references))
                self.error = "; ".join(errors)
                self.element.set(ERROR, self.error)
                self.accepted = False
                return False

        # Run onvalidation
        form = Storage(method = self.method,
                       vars = data,
                       request_vars = data,
                       # Useless since always incomplete:
                       #record = original,
                       )
        if self.id:
            form.vars.id = self.id

        form.errors = Storage()
        tablename = self.tablename
        key = "%s_onvalidation" % self.method
        get_config = current.s3db.get_config
        onvalidation = get_config(tablename, key,
                       get_config(tablename, "onvalidation"))
        if onvalidation:
            try:
                callback(onvalidation, form, tablename=tablename)
            except:
                from traceback import format_exc
                current.log.error("S3Import %s onvalidation exception:" % tablename)
                current.log.debug(format_exc(10))
        accepted = True
        if form.errors:
            element = self.element
            for k in form.errors:
                e = element.findall("data[@field='%s']" % k)
                if not e:
                    e = element.findall("reference[@field='%s']" % k)
                if not e:
                    e = element
                    form.errors[k] = "[%s] %s" % (k, form.errors[k])
                else:
                    e = e[0]
                e.set(ERROR, s3_str(form.errors[k]))
            self.error = current.ERROR.VALIDATION_ERROR
            accepted = False

        self.accepted = accepted
        return accepted

    # -------------------------------------------------------------------------
    def commit(self, ignore_errors=False):
        """
            Commit this item to the database

            @param ignore_errors: skip invalid components
                                  (still reports errors)
        """

        if self.committed:
            # already committed
            return True

        # If the parent item gets skipped, then skip this item as well
        if self.parent is not None and self.parent.skip:
            return True

        # Globals
        db = current.db
        s3db = current.s3db

        xml = current.xml
        ATTRIBUTE = xml.ATTRIBUTE

        # Methods
        METHOD = self.METHOD
        CREATE = METHOD.CREATE
        UPDATE = METHOD.UPDATE
        DELETE = METHOD.DELETE
        MERGE = METHOD.MERGE

        # Policies
        POLICY = self.POLICY
        THIS = POLICY["THIS"]
        NEWER = POLICY["NEWER"]
        MASTER = POLICY["MASTER"]

        # Constants
        UID = xml.UID
        MCI = xml.MCI
        MTIME = xml.MTIME
        VALIDATION_ERROR = current.ERROR.VALIDATION_ERROR

        # Make item mtime TZ-aware
        self.mtime = s3_utc(self.mtime)

        # Resolve references
        self._resolve_references()

        # Deduplicate and validate
        if not self.validate():
            self.skip = True

            # Notify the error in the parent to have reported in the
            # interactive (2-phase) importer
            # Note that the parent item is already written at this point,
            # so this notification can NOT prevent/rollback the import of
            # the parent item if ignore_errors is True (forced commit), or
            # if the user deliberately chose to import it despite error.
            parent = self.parent
            if parent is not None:
                parent.error = VALIDATION_ERROR
                element = parent.element
                if not element.get(ATTRIBUTE.error, False):
                    element.set(ATTRIBUTE.error, s3_str(parent.error))

            return ignore_errors

        elif self.method not in (MERGE, DELETE) and self.components:
            for component in self.components:
                if component.accepted is False or \
                   component.data is None:
                    component.skip = True
                    # Skip this item on any component validation errors
                    self.skip = True
                    self.error = VALIDATION_ERROR
                    return ignore_errors

        elif self.method in (MERGE, DELETE) and not self.accepted:
            self.skip = True
            # Deletion of non-existent record: ignore silently
            return True

        # Authorize item
        if not self.authorize():
            self.error = "%s: %s, %s, %s" % (current.ERROR.NOT_PERMITTED,
                                             self.method,
                                             self.tablename,
                                             self.id)
            self.skip = True
            return ignore_errors

        # Update the method
        method = self.method

        # Check if import method is allowed in strategy
        strategy = self.strategy
        if not isinstance(strategy, (list, tuple)):
            strategy = [strategy]
        if method not in strategy:
            self.error = current.ERROR.NOT_PERMITTED
            self.skip = True
            return True

        # Check mtime and mci
        table = self.table
        original = self.original
        original_mtime = None
        original_mci = 0
        if original:
            if hasattr(table, MTIME):
                original_mtime = s3_utc(original[MTIME])
            if hasattr(table, MCI):
                original_mci = original[MCI]
            original_deleted = "deleted" in original and original.deleted
        else:
            original_deleted = False

        # Detect conflicts
        job = self.job
        original_modified = True
        self.modified = True
        self.conflict = False
        last_sync = s3_utc(job.last_sync)
        if last_sync:
            if original_mtime and original_mtime < last_sync:
                original_modified = False
            if self.mtime and self.mtime < last_sync:
                self.modified = False
            if self.modified and original_modified:
                self.conflict = True
        if self.conflict and method in (UPDATE, DELETE, MERGE):
            if job.onconflict:
                job.onconflict(self)

        if self.data is not None:
            data = table._filter_fields(self.data, id=True)
        else:
            data = Storage()

        # Update policy
        if isinstance(self.update_policy, dict):
            def update_policy(f):
                setting = self.update_policy
                p = setting.get(f,
                    setting.get("__default__", THIS))
                if p not in POLICY:
                    return THIS
                return p
        else:
            def update_policy(f):
                p = self.update_policy
                if p not in POLICY:
                    return THIS
                return p

        # Log this item
        if callable(job.log):
            job.log(self)

        tablename = self.tablename
        enforce_realm_update = False

        # Update existing record
        if method == UPDATE:

            if original:
                if original_deleted:
                    policy = update_policy(None)
                    if policy == NEWER and \
                       original_mtime and original_mtime > self.mtime or \
                       policy == MASTER and \
                       (original_mci == 0 or self.mci != 1):
                        self.skip = True
                        return True

                for f in list(data.keys()):
                    if f in original:
                        # Check if unchanged
                        if type(original[f]) is datetime.datetime:
                            if s3_utc(data[f]) == s3_utc(original[f]):
                                del data[f]
                                continue
                        else:
                            if data[f] == original[f]:
                                del data[f]
                                continue
                    remove = False
                    policy = update_policy(f)
                    if policy == THIS:
                        remove = True
                    elif policy == NEWER:
                        if original_mtime and original_mtime > self.mtime:
                            remove = True
                    elif policy == MASTER:
                        if original_mci == 0 or self.mci != 1:
                            remove = True
                    if remove:
                        del data[f]

                if original_deleted:
                    # Undelete re-imported records
                    data["deleted"] = False
                    if hasattr(table, "deleted_fk"):
                        data["deleted_fk"] = ""

                    # Set new author stamp
                    if hasattr(table, "created_by"):
                        data["created_by"] = table.created_by.default
                    if hasattr(table, "modified_by"):
                        data["modified_by"] = table.modified_by.default

                    # Restore defaults for foreign keys
                    for fieldname in table.fields:
                        field = table[fieldname]
                        default = field.default
                        if str(field.type)[:9] == "reference" and \
                           fieldname not in data and \
                           default is not None:
                            data[fieldname] = default

                    # Enforce update of realm entity
                    enforce_realm_update = True

            if not self.skip and not self.conflict and \
               (len(data) or self.components or self.references):
                if self.uid and hasattr(table, UID):
                    data[UID] = self.uid
                if MTIME in table:
                    data[MTIME] = self.mtime
                if MCI in data:
                    # retain local MCI on updates
                    del data[MCI]
                query = (table._id == self.id)
                try:
                    db(query).update(**dict(data))
                except:
                    self.error = sys.exc_info()[1]
                    self.skip = True
                    return ignore_errors
                else:
                    self.committed = True
            else:
                # Nothing to update
                self.committed = True

        # Create new record
        elif method == CREATE:

            # Do not apply field policy to UID and MCI
            if UID in data:
                del data[UID]
            if MCI in data:
                del data[MCI]

            for f in data:
                if update_policy(f) == MASTER and self.mci != 1:
                    del data[f]

            if self.skip:
                return True

            elif len(data) or self.components or self.references:

                # Restore UID and MCI
                if self.uid and UID in table.fields:
                    data[UID] = self.uid
                if MCI in table.fields:
                    data[MCI] = self.mci

                # Insert the new record
                try:
                    success = table.insert(**dict(data))
                except:
                    self.error = sys.exc_info()[1]
                    self.skip = True
                    return ignore_errors
                if success:
                    self.id = success
                    self.committed = True

            else:
                # Nothing to create
                self.skip = True
                return True

        # Delete local record
        elif method == DELETE:

            if original:
                if original_deleted:
                    self.skip = True
                policy = update_policy(None)
                if policy == THIS:
                    self.skip = True
                elif policy == NEWER and \
                     (original_mtime and original_mtime > self.mtime):
                    self.skip = True
                elif policy == MASTER and \
                     (original_mci == 0 or self.mci != 1):
                    self.skip = True
            else:
                self.skip = True

            if not self.skip and not self.conflict:

                resource = s3db.resource(tablename, id=self.id)
                # Use cascade=True so that the deletion can be
                # rolled back (e.g. trial phase, subsequent failure)
                success = resource.delete(cascade=True)
                if resource.error:
                    self.error = resource.error
                    self.skip = True
                    return ignore_errors

            return True

        # Merge records
        elif method == MERGE:

            if UID not in table.fields:
                self.skip = True
            elif original:
                if original_deleted:
                    self.skip = True
                policy = update_policy(None)
                if policy == THIS:
                    self.skip = True
                elif policy == NEWER and \
                     (original_mtime and original_mtime > self.mtime):
                    self.skip = True
                elif policy == MASTER and \
                     (original_mci == 0 or self.mci != 1):
                    self.skip = True
            else:
                self.skip = True

            if not self.skip and not self.conflict:

                row = db(table[UID] == data[xml.REPLACEDBY]) \
                                        .select(table._id, limitby=(0, 1)) \
                                        .first()
                if row:
                    original_id = row[table._id]
                    resource = s3db.resource(tablename,
                                             id = [original_id, self.id],
                                             )
                    try:
                        success = resource.merge(original_id, self.id)
                    except:
                        self.error = sys.exc_info()[1]
                        self.skip = True
                        return ignore_errors
                    if success:
                        self.committed = True
                else:
                    self.skip = True

            return True

        else:
            raise RuntimeError("unknown import method: %s" % method)

        # Audit + onaccept on successful commits
        if self.committed:

            # Create a pseudo-form for callbacks
            form = Storage()
            form.method = method
            form.table = table
            form.vars = self.data
            prefix, name = tablename.split("_", 1)
            if self.id:
                form.vars.id = self.id

            # Audit
            current.audit(method, prefix, name,
                          form = form,
                          record = self.id,
                          representation = "xml",
                          )

            # Prevent that record post-processing breaks time-delayed
            # synchronization by implicitly updating "modified_on"
            if MTIME in table.fields:
                modified_on = table[MTIME]
                modified_on_update = modified_on.update
                modified_on.update = None
            else:
                modified_on_update = None

            # Update super entity links
            s3db.update_super(table, form.vars)
            if method == CREATE:
                # Set record owner
                current.auth.s3_set_record_owner(table, self.id)
            elif method == UPDATE:
                # Update realm
                update_realm = enforce_realm_update or \
                               s3db.get_config(table, "update_realm")
                if update_realm:
                    current.auth.set_realm_entity(table, self.id,
                                                  force_update = True,
                                                  )
            # Onaccept
            key = "%s_onaccept" % method
            onaccept = current.deployment_settings.get_import_callback(tablename, key)
            if onaccept:
                callback(onaccept, form, tablename=tablename)

            # Restore modified_on.update
            if modified_on_update is not None:
                modified_on.update = modified_on_update

        # Update referencing items
        if self.update and self.id:
            for u in self.update:

                # The other import item that shall be updated
                item = u.get("item")
                if not item:
                    continue

                # The field in the other item that shall be updated
                field = u.get("field")
                if isinstance(field, (list, tuple)):
                    # The field references something else than the
                    # primary key of this table => look it up
                    pkey, fkey = field
                    query = (table.id == self.id)
                    row = db(query).select(table[pkey], limitby=(0, 1)).first()
                    ref_id = row[pkey]
                else:
                    # The field references the primary key of this table
                    pkey, fkey = None, field
                    ref_id = self.id

                if "refkey" in u:
                    # Target field is a JSON object
                    item._update_objref(fkey, u["refkey"], ref_id)
                else:
                    # Target field is a reference or list:reference
                    item._update_reference(fkey, ref_id)

        return True

    # -------------------------------------------------------------------------
    def _dynamic_defaults(self, data):
        """
            Applies dynamic defaults from any keys in data that start with
            an underscore, used only for new records and only if the respective
            field is not populated yet.

            @param data: the data dict
        """

        for k, v in list(data.items()):
            if k[0] == "_":
                fn = k[1:]
                if fn in self.table.fields and fn not in data:
                    data[fn] = v

    # -------------------------------------------------------------------------
    def _mandatory_fields(self):

        job = self.job

        mandatory = None
        tablename = self.tablename

        mfields = job.mandatory_fields
        if tablename in mfields:
            mandatory = mfields[tablename]

        if mandatory is None:
            mandatory = []
            for field in self.table:
                if field.default is not None:
                    continue
                requires = field.requires
                if requires:
                    if not isinstance(requires, (list, tuple)):
                        requires = [requires]
                    if isinstance(requires[0], IS_EMPTY_OR):
                        continue
                    error = field.validate("")[1]
                    if error:
                        mandatory.append(field.name)
            mfields[tablename] = mandatory

        return mandatory

    # -------------------------------------------------------------------------
    def _resolve_references(self):
        """
            Resolve the references of this item (=look up all foreign
            keys from other items of the same job). If a foreign key
            is not yet available, it will be scheduled for later update.
        """

        table = self.table
        if not table:
            return

        db = current.db
        items = self.job.items
        for reference in self.references:

            entry = reference.entry
            if not entry:
                continue

            field = reference.field

            # Resolve key tuples
            if isinstance(field, (list, tuple)):
                pkey, fkey = field
            else:
                pkey, fkey = ("id", field)

            f = table[fkey]
            if f.type == "json":
                is_json = True
                objref = reference.objref
                if not objref:
                    objref = S3ObjectReferences(self.data.get(fkey))
                refkey = reference.refkey
                if not refkey:
                    continue
            else:
                is_json = False
                refkey = objref = None
                ktablename, _, multiple = s3_get_foreign_key(f)
                if not ktablename:
                    continue

            # Get the lookup table
            if entry.tablename:
                ktablename = entry.tablename
            try:
                ktable = current.s3db[ktablename]
            except AttributeError:
                continue

            # Resolve the foreign key (value)
            item = None
            fk = entry.id
            if entry.item_id:
                item = items[entry.item_id]
                if item:
                    if item.original and \
                       item.original.get("deleted") and \
                       not item.committed:
                        # Original is deleted and has not been updated
                        fk = None
                    else:
                        fk = item.id
            if fk and pkey != "id":
                row = db(ktable._id == fk).select(ktable[pkey],
                                                  limitby=(0, 1)).first()
                if not row:
                    fk = None
                    continue
                else:
                    fk = row[pkey]

            # Update record data
            if fk:
                if is_json:
                    objref.resolve(refkey[0], refkey[1], refkey[2], fk)
                elif multiple:
                    val = self.data.get(fkey, [])
                    if fk not in val:
                        val.append(fk)
                    self.data[fkey] = val
                else:
                    self.data[fkey] = fk
            else:
                if fkey in self.data and not multiple and not is_json:
                    del self.data[fkey]
                if item:
                    update = {"item": self, "field": fkey}
                    if is_json:
                        update["refkey"] = refkey
                    item.update.append(update)

    # -------------------------------------------------------------------------
    def _update_reference(self, field, value):
        """
            Helper method to update a foreign key in an already written
            record. Will be called by the referenced item after (and only
            if) it has been committed. This is only needed if the reference
            could not be resolved before commit due to circular references.

            @param field: the field name of the foreign key
            @param value: the value of the foreign key
        """

        table = self.table
        record_id = self.id

        if not value or not table or not record_id or not self.permitted:
            return

        db = current.db
        update = None

        fieldtype = str(table[field].type)
        if fieldtype.startswith("list:reference"):
            query = (table._id == record_id)
            record = db(query).select(table[field],
                                      limitby = (0, 1),
                                      ).first()
            if record:
                values = record[field]
                if value not in values:
                    values.append(value)
                    update = {field: values}
        else:
            update = {field: value}

        if update:
            if "modified_on" in table.fields:
                update["modified_on"] = table.modified_on
            if "modified_by" in table.fields:
                update["modified_by"] = table.modified_by
            db(table._id == record_id).update(**update)

    # -------------------------------------------------------------------------
    def _update_objref(self, field, refkey, value):
        """
            Update object references in a JSON field

            @param fieldname: the name of the JSON field
            @param refkey: the reference key, a tuple (tablename, uidtype, uid)
            @param value: the foreign key value
        """


        table = self.table
        record_id = self.id

        if not value or not table or not record_id or not self.permitted:
            return

        db = current.db
        query = (table._id == record_id)
        record = db(query).select(table._id,
                                  table[field],
                                  limitby = (0, 1),
                                  ).first()
        if record:
            obj = record[field]

            tn, uidtype, uid = refkey
            S3ObjectReferences(obj).resolve(tn, uidtype, uid, value)

            update = {field: obj}
            if "modified_on" in table.fields:
                update["modified_on"] = table.modified_on
            if "modified_by" in table.fields:
                update["modified_by"] = table.modified_by
            record.update_record(**update)

    # -------------------------------------------------------------------------
    def store(self, item_table=None):
        """
            Store this item in the DB
        """

        if item_table is None:
            return None

        item_id = self.item_id
        db = current.db
        row = db(item_table.item_id == item_id).select(item_table.id,
                                                       limitby=(0, 1)
                                                       ).first()
        if row:
            record_id = row.id
        else:
            record_id = None

        record = Storage(job_id = self.job.job_id,
                         item_id = item_id,
                         tablename = self.tablename,
                         record_uid = self.uid,
                         skip = self.skip,
                         error = self.error or "",
                         )

        if self.element is not None:
            element_str = current.xml.tostring(self.element,
                                               xml_declaration=False)
            record.update(element=element_str)

        self_data = self.data
        if self_data is not None:
            table = self.table
            fields = table.fields
            data = Storage()
            for f in self_data.keys():
                if f not in fields:
                    continue
                field = table[f]
                field_type = str(field.type)
                if field_type == "id" or s3_has_foreign_key(field):
                    continue
                data_ = self_data[f]
                if isinstance(data_, Field):
                    # Not picklable
                    # This is likely to be a modified_on to avoid updating this field, which skipping does just fine too
                    continue
                data.update({f: data_})
            record["data"] = pickle.dumps(data)

        ritems = []
        for reference in self.references:
            field = reference.field
            entry = reference.entry
            store_entry = None
            if entry:
                if entry.item_id is not None:
                    store_entry = {"field": field,
                                   "item_id": str(entry.item_id),
                                   }
                elif entry.uid is not None:
                    store_entry = {"field": field,
                                   "tablename": entry.tablename,
                                   "uid": str(entry.uid),
                                   }
                if store_entry is not None:
                    ritems.append(json.dumps(store_entry))
        if ritems:
            record.update(ritems=ritems)
        citems = [c.item_id for c in self.components]
        if citems:
            record.update(citems=citems)
        if self.parent:
            record.update(parent=self.parent.item_id)
        if record_id:
            db(item_table.id == record_id).update(**record)
        else:
            record_id = item_table.insert(**record)

        return record_id

    # -------------------------------------------------------------------------
    def restore(self, row):
        """
            Restore an item from a item table row. This does not restore
            the references (since this can not be done before all items
            are restored), must call job.restore_references() to do that

            @param row: the item table row
        """

        xml = current.xml

        self.item_id = row.item_id
        self.accepted = None
        self.permitted = False
        self.committed = False
        tablename = row.tablename
        self.id = None
        self.uid = row.record_uid
        self.skip = row.skip
        if row.data is not None:
            self.data = pickle.loads(row.data)
        else:
            self.data = Storage()
        data = self.data
        if xml.MTIME in data:
            self.mtime = data[xml.MTIME]
        if xml.MCI in data:
            self.mci = data[xml.MCI]
        UID = xml.UID
        if UID in data:
            self.uid = data[UID]
        self.element = etree.fromstring(row.element)
        if row.citems:
            self.load_components = row.citems
        if row.ritems:
            self.load_references = [json.loads(ritem) for ritem in row.ritems]
        self.load_parent = row.parent
        s3db = current.s3db
        try:
            table = s3db[tablename]
        except AttributeError:
            self.error = current.ERROR.BAD_RESOURCE
            return False
        else:
            self.table = table
            self.tablename = tablename
        original = S3Resource.original(table, self.data,
                                       mandatory=self._mandatory_fields())
        if original is not None:
            self.original = original
            self.id = original[table._id.name]
            if not current.response.s3.synchronise_uuids and UID in original:
                self.uid = self.data[UID] = original[UID]
        self.error = row.error
        postprocess = s3db.get_config(self.tablename, "xml_post_parse")
        if postprocess:
            postprocess(self.element, self.data)
        if self.error and not self.data:
            # Validation error
            return False
        return True

# =============================================================================
class S3ImportJob():
    """
        Class to import an element tree into the database
    """

    JOB_TABLE_NAME = "s3_import_job"
    ITEM_TABLE_NAME = "s3_import_item"

    # -------------------------------------------------------------------------
    def __init__(self, table,
                 tree=None,
                 files=None,
                 job_id=None,
                 strategy=None,
                 update_policy=None,
                 conflict_policy=None,
                 last_sync=None,
                 onconflict=None):
        """
            Constructor

            @param tree: the element tree to import
            @param files: files attached to the import (for upload fields)
            @param job_id: restore job from database (record ID or job_id)
            @param strategy: the import strategy
            @param update_policy: the update policy
            @param conflict_policy: the conflict resolution policy
            @param last_sync: the last synchronization time stamp (datetime)
            @param onconflict: custom conflict resolver function
        """

        self.error = None # the last error
        self.error_tree = etree.Element(current.xml.TAG.root)

        self.table = table
        self.tree = tree
        self.files = files
        self.directory = Storage()

        self._uidmap = None

        # Mandatory fields
        self.mandatory_fields = Storage()

        self.elements = Storage()
        self.items = Storage()
        self.references = []

        self.job_table = None
        self.item_table = None

        self.count = 0 # total number of records imported
        self.errors = 0 # total number of records in error
        self.created = [] # IDs of created records
        self.updated = [] # IDs of updated records
        self.deleted = [] # IDs of deleted records

        self.log = None

        # Import strategy
        if strategy is None:
            METHOD = S3ImportItem.METHOD
            strategy = [METHOD.CREATE,
                        METHOD.UPDATE,
                        METHOD.DELETE,
                        METHOD.MERGE,
                        ]
        if not isinstance(strategy, (tuple, list)):
            strategy = [strategy]
        self.strategy = strategy

        # Update policy (default=always update)
        if update_policy:
            self.update_policy = update_policy
        else:
            self.update_policy = S3ImportItem.POLICY.OTHER
        # Conflict resolution policy (default=always update)
        if conflict_policy:
            self.conflict_policy = conflict_policy
        else:
            self.conflict_policy = S3ImportItem.POLICY.OTHER

        # Synchronization settings
        self.mtime = None
        self.last_sync = last_sync
        self.onconflict = onconflict

        if job_id:
            self.__define_tables()
            jobtable = self.job_table
            if str(job_id).isdigit():
                query = (jobtable.id == job_id)
            else:
                query = (jobtable.job_id == job_id)
            row = current.db(query).select(jobtable.job_id,
                                           jobtable.tablename,
                                           limitby=(0, 1)).first()
            if not row:
                raise SyntaxError("Job record not found")
            self.job_id = row.job_id
            self.second_pass = True
            if not self.table:
                tablename = row.tablename
                try:
                    table = current.s3db[tablename]
                except AttributeError:
                    pass
        else:
            self.job_id = uuid.uuid4() # unique ID for this job
            self.second_pass = False

    # -------------------------------------------------------------------------
    @property
    def uidmap(self):
        """
            Map uuid/tuid => element, for faster reference lookups
        """

        uidmap = self._uidmap
        tree = self.tree

        if uidmap is None and tree is not None:

            root = tree if isinstance(tree, etree._Element) else tree.getroot()

            xml = current.xml
            UUID = xml.UID
            TUID = xml.ATTRIBUTE.tuid
            NAME = xml.ATTRIBUTE.name

            elements = root.xpath(".//%s" % xml.TAG.resource)
            self._uidmap = uidmap = {UUID: {},
                                     TUID: {},
                                     }
            uuidmap = uidmap[UUID]
            tuidmap = uidmap[TUID]
            for element in elements:
                name = element.get(NAME)
                r_uuid = element.get(UUID)
                if r_uuid and r_uuid not in uuidmap:
                    uuidmap[(name, r_uuid)] = element
                r_tuid = element.get(TUID)
                if r_tuid and r_tuid not in tuidmap:
                    tuidmap[(name, r_tuid)] = element

        return uidmap

    # -------------------------------------------------------------------------
    def add_item(self,
                 element = None,
                 original = None,
                 components = None,
                 parent = None,
                 joinby = None):
        """
            Parse and validate an XML element and add it as new item
            to the job.

            @param element: the element
            @param original: the original DB record (if already available,
                             will otherwise be looked-up by this function)
            @param components: a dictionary of components (as in S3Resource)
                               to include in the job (defaults to all
                               defined components)
            @param parent: the parent item (if this is a component)
            @param joinby: the component join key(s) (if this is a component)

            @return: a unique identifier for the new item, or None if there
                      was an error. self.error contains the last error, and
                      self.error_tree an element tree with all failing elements
                      including error attributes.
        """

        if element in self.elements:
            # element has already been added to this job
            return self.elements[element]

        # Parse the main element
        item = S3ImportItem(self)

        # Update lookup lists
        item_id = item.item_id
        self.items[item_id] = item
        if element is not None:
            self.elements[element] = item_id

        if not item.parse(element,
                          original = original,
                          files = self.files):
            self.error = item.error
            item.accepted = False
            if parent is None:
                self.error_tree.append(deepcopy(item.element))

        else:
            # Now parse the components
            table = item.table

            s3db = current.s3db
            components = s3db.get_components(table, names=components)
            super_keys = s3db.get_super_keys(table)

            cnames = Storage()
            cinfos = Storage()
            for alias in components:

                component = components[alias]

                ctable = component.table
                if ctable._id != "id" and "instance_type" in ctable.fields:
                    # Super-entities cannot be imported to directly => skip
                    continue

                # Determine the keys
                pkey = component.pkey

                if pkey != table._id.name and pkey not in super_keys:
                    # Pseudo-component cannot be imported => skip
                    continue

                if component.linktable:
                    ctable = component.linktable
                    fkey = component.lkey
                else:
                    fkey = component.fkey

                ctablename = ctable._tablename
                if ctablename in cnames:
                    cnames[ctablename].append(alias)
                else:
                    cnames[ctablename] = [alias]

                cinfos[(ctablename, alias)] = Storage(component = component,
                                                      ctable = ctable,
                                                      pkey = pkey,
                                                      fkey = fkey,
                                                      first = True,
                                                      )
            add_item = self.add_item
            xml = current.xml
            UID = xml.UID
            for celement in xml.components(element, names=list(cnames.keys())):

                # Get the component tablename
                ctablename = celement.get(xml.ATTRIBUTE.name, None)
                if not ctablename or ctablename not in cnames:
                    continue

                # Get the component alias (for disambiguation)
                calias = celement.get(xml.ATTRIBUTE.alias, None)
                if calias is None:
                    aliases = cnames[ctablename]
                    if len(aliases) == 1:
                        calias = aliases[0]
                    else:
                        calias = ctablename.split("_", 1)[1]

                if (ctablename, calias) not in cinfos:
                    continue
                else:
                    cinfo = cinfos[(ctablename, calias)]

                component = cinfo.component
                ctable = cinfo.ctable

                pkey = cinfo.pkey
                fkey = cinfo.fkey

                original = None

                if not component.multiple:
                    # Single-component: skip all subsequent items after
                    # the first under the same master record
                    if not cinfo.first:
                        continue
                    cinfo.first = False

                    # Single component = the first component record
                    # under the master record is always the original,
                    # only relevant if the master record exists in
                    # the db and hence item.id is not None
                    if item.id:
                        db = current.db
                        query = (table.id == item.id) & \
                                (table[pkey] == ctable[fkey])
                        if UID in ctable.fields:
                            # Load only the UUID now, parse will load any
                            # required data later
                            row = db(query).select(ctable[UID],
                                                   limitby = (0, 1)
                                                   ).first()
                            if row:
                                original = row[UID]
                        else:
                            # Not nice, but a rare edge-case
                            original = db(query).select(ctable.ALL,
                                                        limitby = (0, 1)
                                                        ).first()

                # Recurse
                item_id = add_item(element = celement,
                                   original = original,
                                   parent = item,
                                   joinby = (pkey, fkey))
                if item_id is None:
                    item.error = self.error
                    self.error_tree.append(deepcopy(item.element))
                else:
                    citem = self.items[item_id]
                    citem.parent = item
                    item.components.append(citem)

            lookahead = self.lookahead
            directory = self.directory

            # Handle references
            table = item.table
            data = item.data
            tree = self.tree

            def schedule(reference):
                """ Schedule a referenced item for implicit import """
                entry = reference.entry
                if entry and entry.element is not None and not entry.item_id:
                    item_id = add_item(element=entry.element)
                    if item_id:
                        entry.item_id = item_id

            # Foreign key fields in table
            if tree is not None:
                fields = [table[f] for f in table.fields]
                rfields = [f for f in fields if s3_has_foreign_key(f)]
                item.references = lookahead(element,
                                            table = table,
                                            fields = rfields,
                                            tree = tree,
                                            directory = directory,
                                            )
                for reference in item.references:
                    schedule(reference)

            references = item.references
            rappend = references.append

            # Parent reference
            if parent is not None:
                entry = Storage(item_id = parent.item_id,
                                element = parent.element,
                                tablename = parent.tablename,
                                )
                rappend(Storage(field = joinby,
                                entry = entry,
                                ))

            # References in JSON field data
            json_references = s3db.get_config(table, "json_references")
            if json_references:
                if json_references is True:
                    # Discover references in any JSON fields
                    fields = table.fields
                else:
                    # Discover references in fields specified by setting
                    fields = json_references
                    if not isinstance(fields, (tuple, list)):
                        fields = [fields]
                for fieldname in fields:
                    value = data.get(fieldname)
                    field = table[fieldname]
                    if value and field.type == "json":
                        objref = S3ObjectReferences(value)
                        for ref in objref.refs:
                            rl = lookahead(None,
                                           tree = tree,
                                           directory = directory,
                                           lookup = ref,
                                           )
                            if rl:
                                reference = rl[0]
                                schedule(reference)
                                rappend(Storage(field = fieldname,
                                                objref = objref,
                                                refkey = ref,
                                                entry = reference.entry,
                                                ))

            # Replacement reference
            deleted = data.get(xml.DELETED, False)
            if deleted:
                fieldname = xml.REPLACEDBY
                replaced_by = data.get(fieldname)
                if replaced_by:
                    rl = lookahead(element,
                                   tree = tree,
                                   directory = directory,
                                   lookup = (table, replaced_by),
                                   )
                    if rl:
                        reference = rl[0]
                        schedule(reference)
                        rappend(Storage(field = fieldname,
                                        entry = reference.entry,
                                        ))

        return item.item_id

    # -------------------------------------------------------------------------
    def lookahead(self,
                  element,
                  table = None,
                  fields = None,
                  tree = None,
                  directory = None,
                  lookup = None):
        """
            Find referenced elements in the tree

            @param element: the element
            @param table: the DB table
            @param fields: the FK fields in the table
            @param tree: the import tree
            @param directory: a dictionary to lookup elements in the tree
                              (will be filled in by this function)
        """

        db = current.db
        s3db = current.s3db

        xml = current.xml
        import_uid = xml.import_uid

        ATTRIBUTE = xml.ATTRIBUTE
        TAG = xml.TAG
        UID = xml.UID

        reference_list = []
        rlappend = reference_list.append

        root = None
        if tree is not None:
            root = tree if isinstance(tree, etree._Element) else tree.getroot()
        uidmap = self.uidmap

        references = [lookup] if lookup else element.findall("reference")
        for reference in references:
            if lookup:
                field = None
                if element is None:
                    tablename, attr, uid = reference
                    ktable = s3db.table(tablename)
                    if ktable is None:
                        continue
                    uids = [import_uid(uid)] if attr == "uuid" else [uid]
                else:
                    tablename = element.get(ATTRIBUTE.name, None)
                    ktable, uid = reference
                    attr = UID
                    uids = [import_uid(uid)]
            else:
                field = reference.get(ATTRIBUTE.field, None)

                # Ignore references without valid field-attribute
                if not field or field not in fields or field not in table:
                    continue

                # Find the key table
                ktablename, _, multiple = s3_get_foreign_key(table[field])
                if not ktablename:
                    continue
                try:
                    ktable = s3db[ktablename]
                except AttributeError:
                    continue

                tablename = reference.get(ATTRIBUTE.resource, None)
                # Ignore references to tables without UID field:
                if UID not in ktable.fields:
                    continue
                # Fall back to key table name if tablename is not specified:
                if not tablename:
                    tablename = ktablename
                # Super-entity references must use the super-key:
                if tablename != ktablename:
                    field = (ktable._id.name, field)
                # Ignore direct references to super-entities:
                if tablename == ktablename and ktable._id.name != "id":
                    continue
                # Get the foreign key
                uids = reference.get(UID, None)
                attr = UID
                if not uids:
                    uids = reference.get(ATTRIBUTE.tuid, None)
                    attr = ATTRIBUTE.tuid
                if uids and multiple:
                    uids = json.loads(uids)
                elif uids:
                    uids = [uids]

            # Find the elements and map to DB records
            relements = []

            # Create a UID<->ID map
            id_map = {}
            if attr == UID and uids:
                if len(uids) == 1:
                    uid = import_uid(uids[0])
                    query = (ktable[UID] == uid)
                    record = db(query).select(ktable.id,
                                              cacheable = True,
                                              limitby = (0, 1),
                                              ).first()
                    if record:
                        id_map[uid] = record.id
                else:
                    uids_ = [import_uid(uid) for uid in uids]
                    query = (ktable[UID].belongs(uids_))
                    records = db(query).select(ktable.id,
                                               ktable[UID],
                                               limitby = (0, len(uids_)),
                                               )
                    for r in records:
                        id_map[r[UID]] = r.id

            if not uids:
                # Anonymous reference: <resource> inside the element
                expr = './/%s[@%s="%s"]' % (TAG.resource,
                                            ATTRIBUTE.name,
                                            tablename,
                                            )
                relements = reference.xpath(expr)
                if relements and not multiple:
                    relements = relements[:1]

            elif root is not None:

                for uid in uids:

                    entry = None

                    # Entry already in directory?
                    if directory is not None:
                        entry = directory.get((tablename, attr, uid))

                    if not entry:
                        e = uidmap[attr].get((tablename, uid)) if uidmap else None
                        if e is not None:
                            # Element in the source => append to relements
                            relements.append(e)
                        else:
                            # No element found, see if original record exists
                            _uid = import_uid(uid)
                            if _uid and _uid in id_map:
                                _id = id_map[_uid]
                                entry = Storage(tablename = tablename,
                                                element = None,
                                                uid = uid,
                                                id = _id,
                                                item_id = None,
                                                )
                                rlappend(Storage(field = field,
                                                 element = reference,
                                                 entry = entry,
                                                 ))
                            else:
                                continue
                    else:
                        rlappend(Storage(field = field,
                                         element = reference,
                                         entry = entry,
                                         ))

            # Create entries for all newly found elements
            for relement in relements:
                uid = relement.get(attr, None)
                if attr == UID:
                    _uid = import_uid(uid)
                    _id = _uid and id_map and id_map.get(_uid, None) or None
                else:
                    _uid = None
                    _id = None
                entry = Storage(tablename = tablename,
                                element = relement,
                                uid = uid,
                                id = _id,
                                item_id = None,
                                )
                # Add entry to directory
                if uid and directory is not None:
                    directory[(tablename, attr, uid)] = entry
                # Append the entry to the reference list
                rlappend(Storage(field = field,
                                 element = reference,
                                 entry = entry,
                                 ))

        return reference_list

    # -------------------------------------------------------------------------
    def load_item(self, row):
        """
            Load an item from the item table (counterpart to add_item
            when restoring a job from the database)
        """

        item = S3ImportItem(self)
        if not item.restore(row):
            self.error = item.error
            if item.load_parent is None:
                self.error_tree.append(deepcopy(item.element))
        # Update lookup lists
        item_id = item.item_id
        self.items[item_id] = item
        return item_id

    # -------------------------------------------------------------------------
    def resolve(self, item_id, import_list):
        """
            Resolve the reference list of an item

            @param item_id: the import item UID
            @param import_list: the ordered list of items (UIDs) to import
        """

        item = self.items[item_id]
        if item.lock or item.accepted is False:
            return False
        references = []
        for reference in item.references:
            ritem_id = reference.entry.item_id
            if ritem_id and ritem_id not in import_list:
                references.append(ritem_id)
        for ritem_id in references:
            item.lock = True
            if self.resolve(ritem_id, import_list):
                import_list.append(ritem_id)
            item.lock = False
        return True

    # -------------------------------------------------------------------------
    def commit(self, ignore_errors=False, log_items=None):
        """
            Commit the import job to the DB

            @param ignore_errors: skip any items with errors
                                  (does still report the errors)
            @param log_items: callback function to log import items
                              before committing them
        """

        ATTRIBUTE = current.xml.ATTRIBUTE
        METHOD = S3ImportItem.METHOD

        # Resolve references
        import_list = []
        for item_id in self.items:
            self.resolve(item_id, import_list)
            if item_id not in import_list:
                import_list.append(item_id)
        # Commit the items
        items = self.items
        count = 0
        errors = 0
        mtime = None
        created = []
        cappend = created.append
        updated = []
        deleted = []
        tablename = self.table._tablename

        self.log = log_items
        failed = False
        for item_id in import_list:
            item = items[item_id]
            error = None

            if item.accepted is not False:
                logged = False
                success = item.commit(ignore_errors=ignore_errors)
            else:
                # Field validation failed
                logged = True
                success = ignore_errors

            if not success:
                failed = True

            error = item.error
            if error:
                current.log.error(error)
                self.error = error
                element = item.element
                if element is not None:
                    if not element.get(ATTRIBUTE.error, False):
                        element.set(ATTRIBUTE.error, s3_str(error))
                    if not logged:
                        self.error_tree.append(deepcopy(element))
                if item.tablename == tablename:
                    errors += 1

            elif item.tablename == tablename:
                count += 1
                if mtime is None or item.mtime > mtime:
                    mtime = item.mtime
                if item.id:
                    if item.method == METHOD.CREATE:
                        cappend(item.id)
                    elif item.method == METHOD.UPDATE:
                        updated.append(item.id)
                    elif item.method in (METHOD.MERGE, METHOD.DELETE):
                        deleted.append(item.id)

        if failed:
            return False

        self.count = count
        self.errors = errors
        self.mtime = mtime
        self.created = created
        self.updated = updated
        self.deleted = deleted
        return True

    # -------------------------------------------------------------------------
    def __define_tables(self):
        """
            Define the database tables for jobs and items
        """

        self.job_table = self.define_job_table()
        self.item_table = self.define_item_table()

    # -------------------------------------------------------------------------
    @classmethod
    def define_job_table(cls):

        db = current.db
        if cls.JOB_TABLE_NAME not in db:
            db.define_table(cls.JOB_TABLE_NAME,
                            Field("job_id", length=128,
                                  unique=True,
                                  notnull=True),
                            Field("tablename"),
                            Field("timestmp", "datetime",
                                  default = datetime.datetime.utcnow()
                                  )
                            )

        return db[cls.JOB_TABLE_NAME]

    # -------------------------------------------------------------------------
    @classmethod
    def define_item_table(cls):

        db = current.db
        if cls.ITEM_TABLE_NAME not in db:
            db.define_table(cls.ITEM_TABLE_NAME,
                            Field("item_id", length=128,
                                  unique=True,
                                  notnull=True),
                            Field("job_id", length=128),
                            Field("tablename", length=128),
                            #Field("record_id", "integer"),
                            Field("record_uid"),
                            Field("skip", "boolean"),
                            Field("error", "text"),
                            Field("data", "blob"),
                            Field("element", "text"),
                            Field("ritems", "list:string"),
                            Field("citems", "list:string"),
                            Field("parent", length=128)
                            )

        return db[cls.ITEM_TABLE_NAME]

    # -------------------------------------------------------------------------
    def store(self):
        """
            Store this job and all its items in the job table
        """

        db = current.db

        self.__define_tables()
        jobtable = self.job_table
        query = jobtable.job_id == self.job_id
        row = db(query).select(jobtable.id, limitby=(0, 1)).first()
        if row:
            record_id = row.id
        else:
            record_id = None
        record = Storage(job_id=self.job_id)
        try:
            tablename = self.table._tablename
        except AttributeError:
            pass
        else:
            record.update(tablename=tablename)
        for item in self.items.values():
            item.store(item_table=self.item_table)
        if record_id:
            db(jobtable.id == record_id).update(**record)
        else:
            record_id = jobtable.insert(**record)

        return record_id

    # -------------------------------------------------------------------------
    def get_tree(self):
        """
            Reconstruct the element tree of this job
        """

        if self.tree is not None:
            return self.tree
        else:
            xml = current.xml
            ATTRIBUTE = xml.ATTRIBUTE
            UID = xml.UID
            root = etree.Element(xml.TAG.root)
            for item in self.items.values():
                element = item.element
                if element is not None and not item.parent:
                    if item.tablename == self.table._tablename or \
                       element.get(UID, None) or \
                       element.get(ATTRIBUTE.tuid, None):
                        root.append(deepcopy(element))
            return etree.ElementTree(root)

    # -------------------------------------------------------------------------
    def delete(self):
        """
            Delete this job and all its items from the job table
        """

        db = current.db

        #current.log.debug("Deleting job ID=%s" % self.job_id)

        self.__define_tables()
        item_table = self.item_table
        query = item_table.job_id == self.job_id
        db(query).delete()
        job_table = self.job_table
        query = job_table.job_id == self.job_id
        db(query).delete()

    # -------------------------------------------------------------------------
    def restore_references(self):
        """
            Restore the job's reference structure after loading items
            from the item table
        """

        db = current.db
        UID = current.xml.UID

        for item in self.items.values():
            for citem_id in item.load_components:
                if citem_id in self.items:
                    item.components.append(self.items[citem_id])
            item.load_components = []
            for ritem in item.load_references:
                field = ritem["field"]
                if "item_id" in ritem:
                    item_id = ritem["item_id"]
                    if item_id in self.items:
                        _item = self.items[item_id]
                        entry = Storage(tablename=_item.tablename,
                                        element=_item.element,
                                        uid=_item.uid,
                                        id=_item.id,
                                        item_id=item_id)
                        item.references.append(Storage(field=field,
                                                       entry=entry))
                else:
                    _id = None
                    uid = ritem.get("uid", None)
                    tablename = ritem.get("tablename", None)
                    if tablename and uid:
                        try:
                            table = current.s3db[tablename]
                        except AttributeError:
                            continue
                        if UID not in table.fields:
                            continue
                        query = table[UID] == uid
                        row = db(query).select(table._id,
                                               limitby=(0, 1)).first()
                        if row:
                            _id = row[table._id.name]
                        else:
                            continue
                        entry = Storage(tablename = ritem["tablename"],
                                        element=None,
                                        uid = ritem["uid"],
                                        id = _id,
                                        item_id = None)
                        item.references.append(Storage(field=field,
                                                       entry=entry))
            item.load_references = []
            if item.load_parent is not None:
                parent = self.items[item.load_parent]
                if parent is None:
                    # Parent has been removed
                    item.skip = True
                else:
                    item.parent = parent
                item.load_parent = None

# =============================================================================
class S3ObjectReferences(object):
    """
        Utility to discover and resolve references in a JSON object;
        handles both uuid- and tuid-based references

        - traverses the object to find dict items of any of these formats:
                "$k_<name>": {"r": <tablename>, "u": <uuid>}
                "$k_<name>": {"@resource": <tablename>, "@uuid": <uuid>}
                "$k_<name>": {"r": <tablename>, "t": <tuid>}
                "$k_<name>": {"@resource": <tablename>, "@tuid": <tuid>}

        - resolve() replaces them with:
                "<name>": <db_id>

        @example:
            # Get a list of all references in obj
            refs = S3ObjectReferences(obj).refs
        @example
            # Resolve a reference in obj
            S3ObjectReferences(obj).resolve("req_req", "uuid", "REQ1", 57)
    """

    TABLENAME_KEYS = ("@resource", "r")
    UUID_KEYS = ("@uuid", "u")
    TUID_KEYS = ("@tuid", "t")

    def __init__(self, obj):
        """
            Constructor

            @param obj: the object to inspect (parsed)
        """

        self.obj = obj

        self._refs = None
        self._objs = None

    # -------------------------------------------------------------------------
    @property
    def refs(self):
        """
            List of references discovered in the object (lazy property)

            @returns: a list of tuples (tablename, uidtype, uid)
        """

        if self._refs is None:
            self._refs = []
            self._objs = {}
            self._traverse(self.obj)
        return self._refs

    # -------------------------------------------------------------------------
    @property
    def objs(self):
        """
            A dict with pointers to the references inside the object

            @returns: a dict {(tablename, uidtype, uid): (obj, key)}
        """

        if self._objs is None:
            self._refs = []
            self._objs = {}
            self._traverse(self.obj)
        return self._objs

    # -------------------------------------------------------------------------
    def _traverse(self, obj):
        """
            Traverse a (possibly nested) object and find all references,
            populates self.refs and self.objs

            @param obj: the object to inspect
        """

        refs = self._refs
        objs = self._objs

        if type(obj) is list:
            for item in obj:
                self._traverse(item)

        elif type(obj) is dict:

            for key, value in obj.items():

                if key[:3] == "$k_" and type(value) is dict:

                    tablename = uid = uid_type = None

                    for k in self.TABLENAME_KEYS:
                        tablename = value.get(k)
                        if tablename:
                            break
                    if tablename:
                        for k in self.UUID_KEYS:
                            uid = value.get(k)
                            if uid:
                                uid_type = "uuid"
                                break
                    if tablename and not uid:
                        for k in self.TUID_KEYS:
                            uid = value.get(k)
                            if uid:
                                uid_type = "tuid"
                                break
                    if not tablename or not uid:
                        self._traverse(value)
                    else:
                        ref = (tablename, uid_type, uid)
                        if ref not in objs:
                            refs.append(ref)
                            objs[ref] = [(obj, key)]
                        else:
                            objs[ref].append((obj, key))
                else:
                    self._traverse(value)

    # -------------------------------------------------------------------------
    def resolve(self, tablename, uidtype, uid, value):
        """
            Resolve a reference in self.obj with the given value; will
            resolve all occurences of the reference

            @param tablename: the referenced table
            @param uidtype: the type of uid (uuid or tuid)
            @param uid: the uuid or tuid
            @param value: the value to resolve the reference
        """

        items = self.objs.get((tablename, uidtype, uid))
        if items:
            for obj, key in items:
                if len(key) > 3:
                    obj[key[3:]] = value
                obj.pop(key, None)

# =============================================================================
class S3Duplicate(object):
    """ Standard deduplicator method """

    def __init__(self,
                 primary = None,
                 secondary = None,
                 ignore_case = True,
                 ignore_deleted = False,
                 noupdate = False,
                 ):
        """
            Constructor

            @param primary: list or tuple of primary fields to find a
                            match, must always match (mandatory, defaults
                            to "name" field)
            @param secondary: list or tuple of secondary fields to
                              find a match, must match if values are
                              present in the import item
            @param ignore_case: ignore case for string/text fields
            @param ignore_deleted: do not match deleted records
            @param noupdate: match, but do not update

            @ToDo: Fuzzy option to do a LIKE search
        """

        if not primary:
            primary = ("name",)
        self.primary = set(primary)

        if not secondary:
            self.secondary = set()
        else:
            self.secondary = set(secondary)

        self.ignore_case = ignore_case
        self.ignore_deleted = ignore_deleted
        self.noupdate = noupdate

    # -------------------------------------------------------------------------
    def __call__(self, item):
        """
            Entry point for importer

            @param item: the import item

            @return: the duplicate Row if match found, otherwise None

            @raise SyntaxError: if any of the query fields doesn't exist
                                in the item table
        """

        data = item.data
        table = item.table

        query = None
        error = "Invalid field for duplicate detection: %s (%s)"

        # Primary query (mandatory)
        primary = self.primary
        for fname in primary:

            if fname not in table.fields:
                raise SyntaxError(error % (fname, table))

            field = table[fname]
            value = data.get(fname)

            q = self.match(field, value)
            query = q if query is None else query & q

        # Secondary queries (optional)
        secondary = self.secondary
        for fname in secondary:

            if fname not in table.fields:
                raise SyntaxError(error % (fname, table))

            field = table[fname]
            value = data.get(fname)
            if value:
                query &= self.match(field, value)

        # Ignore deleted records?
        if self.ignore_deleted and "deleted" in table.fields:
            query &= (table.deleted == False)

        # Find a match
        duplicate = current.db(query).select(table._id,
                                             limitby = (0, 1)
                                             ).first()

        if duplicate:
            # Match found: Update import item
            item.id = duplicate[table._id]
            if not data.deleted:
                item.method = item.METHOD.UPDATE
            if self.noupdate:
                item.skip = True

        # For uses outside of imports:
        return duplicate

    # -------------------------------------------------------------------------
    def match(self, field, value):
        """
            Helper function to generate a match-query

            @param field: the Field
            @param value: the value

            @return: a Query
        """

        ftype = str(field.type)
        ignore_case = self.ignore_case

        if ignore_case and \
           hasattr(value, "lower") and ftype in ("string", "text"):
            # NB Must convert to unicode before lower() in order to correctly
            #    convert certain unicode-characters (e.g. İ=>i, or Ẽ=>ẽ)
            # => PostgreSQL LOWER() on Windows may not convert correctly, (same for SQLite)
            #    which seems to be a locale issue:
            #    http://stackoverflow.com/questions/18507589/the-lower-function-on-international-characters-in-postgresql
            # => works fine on Debian servers if the locale is a .UTF-8 before
            #    the Postgres cluster is created
            query = (field.lower() == s3_str(value).lower())
        else:
            query = (field == value)

        return query

# =============================================================================
class S3BulkImporter(object):
    """
        Import CSV files of data to pre-populate the database.
        Suitable for use in Testing, Demos & Simulations

        http://eden.sahanafoundation.org/wiki/DeveloperGuidelines/PrePopulate
    """

    def __init__(self):
        """ Constructor """

        import csv
        from xml.sax.saxutils import unescape

        self.csv = csv
        self.unescape = unescape
        self.tasks = []
        # Some functions refer to a different resource
        self.alternateTables = {
            "hrm_group_membership": {"tablename": "pr_group_membership",
                                     "prefix": "pr",
                                     "name": "group_membership"},
            "hrm_person": {"tablename": "pr_person",
                           "prefix": "pr",
                           "name": "person"},
            "member_person": {"tablename": "pr_person",
                              "prefix": "pr",
                              "name": "person"},
            }
        # Keep track of which resources have been customised so we don't do this twice
        self.customised = []
        self.errorList = []
        self.resultList = []

    # -------------------------------------------------------------------------
    def load_descriptor(self, path):
        """
            Load the descriptor file and then all the import tasks in that file
            into the task property.
            The descriptor file is the file called tasks.cfg in path.
            The file consists of a comma separated list of:
            module, resource name, csv filename, xsl filename.
        """

        source = open(os.path.join(path, "tasks.cfg"), "r")
        values = self.csv.reader(source)
        for details in values:
            if details == []:
                continue
            prefix = details[0][0].strip('" ')
            if prefix == "#": # comment
                continue
            if prefix == "*": # specialist function
                self.extract_other_import_line(path, details)
            else: # standard CSV importer
                self.extract_csv_import_line(path, details)

    # -------------------------------------------------------------------------
    def extract_csv_import_line(self, path, details):
        """
            Extract the details for a CSV Import Task
        """

        argCnt = len(details)
        if argCnt == 4 or argCnt == 5:
            # Remove any spaces and enclosing double quote
            mod = details[0].strip('" ')
            res = details[1].strip('" ')
            folder = current.request.folder

            csvFileName = details[2].strip('" ')
            if csvFileName[:7] == "http://":
                csv = csvFileName
            else:
                (csvPath, csvFile) = os.path.split(csvFileName)
                if csvPath != "":
                    path = os.path.join(folder,
                                        "modules",
                                        "templates",
                                        csvPath)
                    # @todo: deprecate this block once migration completed
                    if not os.path.exists(path):
                        # Non-standard location (legacy template)?
                        path = os.path.join(folder,
                                            "private",
                                            "templates",
                                            csvPath)
                csv = os.path.join(path, csvFile)

            xslFileName = details[3].strip('" ')
            templateDir = os.path.join(folder,
                                       "static",
                                       "formats",
                                       "s3csv")
            # Try the module directory in the templates directory first
            xsl = os.path.join(templateDir, mod, xslFileName)
            if os.path.exists(xsl) == False:
                # Now try the templates directory
                xsl = os.path.join(templateDir, xslFileName)
                if os.path.exists(xsl) == False:
                    # Use the same directory as the csv file
                    xsl = os.path.join(path, xslFileName)
                    if os.path.exists(xsl) == False:
                        self.errorList.append(
                        "Failed to find a transform file %s, Giving up." % xslFileName)
                        return

            if argCnt == 5:
                extra_data = details[4]
            else:
                extra_data = None
            self.tasks.append([1, mod, res, csv, xsl, extra_data])
        else:
            self.errorList.append(
            "prepopulate error: job not of length 4, ignored: %s" % str(details))

    # -------------------------------------------------------------------------
    def extract_other_import_line(self, path, details):
        """
            Store a single import job into the tasks property
            *,function,filename,*extraArgs
        """

        function = details[1].strip('" ')
        filepath = None
        if len(details) >= 3:
            filename = details[2].strip('" ')
            if filename != "":
                (subfolder, filename) = os.path.split(filename)
                if subfolder != "":
                    path = os.path.join(current.request.folder,
                                        "modules",
                                        "templates",
                                        subfolder)
                    # @todo: deprecate this block once migration completed
                    if not os.path.exists(path):
                        # Non-standard location (legacy template)?
                        path = os.path.join(current.request.folder,
                                            "private",
                                            "templates",
                                            subfolder)
                filepath = os.path.join(path, filename)

        if len(details) >= 4:
            extraArgs = details[3:]
        else:
            extraArgs = None

        self.tasks.append((2, function, filepath, extraArgs))

    # -------------------------------------------------------------------------
    def execute_import_task(self, task):
        """
            Execute each import job, in order
        """

        # Disable min_length for password during prepop
        current.auth.ignore_min_password_length()

        start = datetime.datetime.now()
        if task[0] == 1:
            s3db = current.s3db
            response = current.response
            error_string = "prepopulate error: file %s missing"
            # Store the view
            view = response.view

            #current.log.debug("Running job %s %s (filename=%s transform=%s)" % (task[1],
            #                                                                    task[2],
            #                                                                    task[3],
            #                                                                    task[4],
            #                                                                    ))

            prefix = task[1]
            name = task[2]
            tablename = "%s_%s" % (prefix, name)
            if tablename in self.alternateTables:
                details = self.alternateTables[tablename]
                if "tablename" in details:
                    tablename = details["tablename"]
                s3db.table(tablename)
                if "loader" in details:
                    loader = details["loader"]
                    if loader is not None:
                        loader()
                if "prefix" in details:
                    prefix = details["prefix"]
                if "name" in details:
                    name = details["name"]

            try:
                resource = s3db.resource(tablename)
            except AttributeError:
                # Table cannot be loaded
                self.errorList.append("WARNING: Unable to find table %s import job skipped" % tablename)
                return

            # Check if the source file is accessible
            filename = task[3]
            if filename[:7] == "http://":
                req = urllib2.Request(url=filename)
                try:
                    f = urlopen(req)
                except HTTPError as e:
                    self.errorList.append("Could not access %s: %s" % (filename, e.read()))
                    return
                except:
                    self.errorList.append(error_string % filename)
                    return
                else:
                    csv = f
            else:
                try:
                    csv = open(filename, "rb")
                except IOError:
                    self.errorList.append(error_string % filename)
                    return

            # Check if the stylesheet is accessible
            try:
                S = open(task[4], "r")
            except IOError:
                self.errorList.append(error_string % task[4])
                return
            else:
                S.close()

            if tablename not in self.customised:
                # Customise the resource
                customise = current.deployment_settings.customise_resource(tablename)
                if customise:
                    request = S3Request(prefix, name, current.request)
                    customise(request, tablename)
                    self.customised.append(tablename)

            extra_data = None
            if task[5]:
                try:
                    extradata = self.unescape(task[5], {"'": '"'})
                    extradata = json.loads(extradata)
                    extra_data = extradata
                except:
                    self.errorList.append("WARNING:5th parameter invalid, parameter %s ignored" % task[5])
            auth = current.auth
            auth.rollback = True
            try:
                # @todo: add extra_data and file attachments
                resource.import_xml(csv,
                                    format = "csv",
                                    stylesheet = task[4],
                                    extra_data = extra_data,
                                    )
            except SyntaxError as e:
                self.errorList.append("WARNING: import error - %s (file: %s, stylesheet: %s)" %
                                     (e, filename, task[4]))
                auth.rollback = False
                return

            if not resource.error:
                current.db.commit()
            else:
                # Must roll back if there was an error!
                error = resource.error
                self.errorList.append("%s - %s: %s" % (
                                      task[3], resource.tablename, error))
                errors = current.xml.collect_errors(resource)
                if errors:
                    self.errorList.extend(errors)
                current.db.rollback()

            auth.rollback = False

            # Restore the view
            response.view = view
            end = datetime.datetime.now()
            duration = end - start
            csvName = task[3][task[3].rfind("/") + 1:]
            duration = '{:.2f}'.format(duration.total_seconds())
            msg = "%s imported (%s sec)" % (csvName, duration)
            self.resultList.append(msg)
            current.log.debug(msg)

    # -------------------------------------------------------------------------
    def execute_special_task(self, task):
        """
            Execute import tasks which require a custom function,
            such as import_role
        """

        start = datetime.datetime.now()
        s3 = current.response.s3
        if task[0] == 2:
            fun = task[1]
            filepath = task[2]
            extraArgs = task[3]
            if filepath is None:
                if extraArgs is None:
                    error = s3[fun]()
                else:
                    error = s3[fun](*extraArgs)
            elif extraArgs is None:
                error = s3[fun](filepath)
            else:
                error = s3[fun](filepath, *extraArgs)
            if error:
                self.errorList.append(error)
            end = datetime.datetime.now()
            duration = end - start
            duration = '{:.2f}'.format(duration.total_seconds())
            msg = "%s completed (%s sec)" % (fun, duration)
            self.resultList.append(msg)
            current.log.debug(msg)

    # -------------------------------------------------------------------------
    @staticmethod
    def _lookup_pe(entity):
        """
            Convert an Entity to a pe_id
            - helper for import_role
            - assumes org_organisation.name unless specified
            - entity needs to exist already
        """

        if "=" in entity:
            pe_type, value =  entity.split("=")
        else:
            pe_type = "org_organisation.name"
            value = entity
        pe_tablename, pe_field =  pe_type.split(".")

        table = current.s3db.table(pe_tablename)
        record = current.db(table[pe_field] == value).select(table.pe_id,
                                                             limitby = (0, 1)
                                                             ).first()
        try:
            pe_id = record.pe_id
        except AttributeError:
            current.log.warning("import_role cannot find pe_id for %s" % entity)
            pe_id = None

        return pe_id

    # -------------------------------------------------------------------------
    def import_role(self, filename):
        """
            Import Roles from CSV
        """

        # Check if the source file is accessible
        try:
            openFile = open(filename, "r")
        except IOError:
            return "Unable to open file %s" % filename

        auth = current.auth
        acl = auth.permission
        create_role = auth.s3_create_role

        def parseACL(_acl):
            permissions = _acl.split("|")
            acl_value = 0
            for permission in permissions:
                if permission == "READ":
                    acl_value |= acl.READ
                if permission == "CREATE":
                    acl_value |= acl.CREATE
                if permission == "UPDATE":
                    acl_value |= acl.UPDATE
                if permission == "DELETE":
                    acl_value |= acl.DELETE
                if permission == "REVIEW":
                    acl_value |= acl.REVIEW
                if permission == "APPROVE":
                    acl_value |= acl.APPROVE
                if permission == "PUBLISH":
                    acl_value |= acl.PUBLISH
                if permission == "ALL":
                    acl_value |= acl.ALL
            return acl_value

        reader = self.csv.DictReader(openFile)
        roles = {}
        acls = {}
        args = {}
        for row in reader:
            if row != None:
                row_get = row.get
                role = row_get("role")
                desc = row_get("description", "")
                rules = {}
                extra_param = {}
                controller = row_get("controller")
                if controller:
                    rules["c"] = controller
                fn = row_get("function")
                if fn:
                    rules["f"] = fn
                table = row_get("table")
                if table:
                    rules["t"] = table
                oacl = row_get("oacl")
                if oacl:
                    rules["oacl"] = parseACL(oacl)
                uacl = row_get("uacl")
                if uacl:
                    rules["uacl"] = parseACL(uacl)
                #org = row_get("org")
                #if org:
                #    rules["organisation"] = org
                #facility = row_get("facility")
                #if facility:
                #    rules["facility"] = facility
                entity = row_get("entity")
                if entity:
                    if entity == "any":
                        # Pass through as-is
                        pass
                    else:
                        # NB Entity here is *not* hierarchical!
                        try:
                            entity = int(entity)
                        except ValueError:
                            entity = self._lookup_pe(entity)
                    rules["entity"] = entity
                flag = lambda s: bool(s) and s.lower() in ("1", "true", "yes")
                hidden = row_get("hidden")
                if hidden:
                    extra_param["hidden"] = flag(hidden)
                system = row_get("system")
                if system:
                    extra_param["system"] = flag(system)
                protected = row_get("protected")
                if protected:
                    extra_param["protected"] = flag(protected)
                uid = row_get("uid")
                if uid:
                    extra_param["uid"] = uid
            if role in roles:
                acls[role].append(rules)
            else:
                roles[role] = [role, desc]
                acls[role] = [rules]
            if len(extra_param) > 0 and role not in args:
                args[role] = extra_param
        for rulelist in roles.values():
            if rulelist[0] in args:
                create_role(rulelist[0],
                            rulelist[1],
                            *acls[rulelist[0]],
                            **args[rulelist[0]])
            else:
                create_role(rulelist[0],
                            rulelist[1],
                            *acls[rulelist[0]])

    # -------------------------------------------------------------------------
    def import_user(self, filename):
        """
            Import Users from CSV with an import Prep
        """

        current.response.s3.import_prep = current.auth.s3_import_prep

        current.s3db.add_components("auth_user",
                                    auth_masterkey = "user_id",
                                    )

        user_task = [1,
                     "auth",
                     "user",
                     filename,
                     os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  "s3csv",
                                  "auth",
                                  "user.xsl"
                                  ),
                     None
                     ]
        self.execute_import_task(user_task)

    # -------------------------------------------------------------------------
    def import_feed(self, filename):
        """
            Import RSS Feeds from CSV with an import Prep
        """

        stylesheet = os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  "s3csv",
                                  "msg",
                                  "rss_channel.xsl"
                                  )

        # 1st import any Contacts
        current.response.s3.import_prep = current.s3db.pr_import_prep
        user_task = [1,
                     "pr",
                     "contact",
                     filename,
                     stylesheet,
                     None
                     ]
        self.execute_import_task(user_task)

        # Then import the Channels
        user_task = [1,
                     "msg",
                     "rss_channel",
                     filename,
                     stylesheet,
                     None
                     ]
        self.execute_import_task(user_task)

    # -------------------------------------------------------------------------
    def import_image(self,
                     filename,
                     tablename,
                     idfield,
                     imagefield
                     ):
        """
            Import images, such as a logo or person image

            filename     a CSV list of records and filenames
            tablename    the name of the table
            idfield      the field used to identify the record
            imagefield   the field to where the image will be added

            Example:
            bi.import_image ("org_logos.csv", "org_organisation", "name", "logo")
            and the file org_logos.csv may look as follows
            id                            file
            Sahana Software Foundation    sahanalogo.jpg
            American Red Cross            icrc.gif
        """

        # Check if the source file is accessible
        try:
            openFile = open(filename, "r", encoding="utf-8")
        except IOError:
            return "Unable to open file %s" % filename

        prefix, name = tablename.split("_", 1)

        reader = self.csv.DictReader(openFile)

        db = current.db
        s3db = current.s3db
        audit = current.audit
        table = s3db[tablename]
        idfield = table[idfield]
        base_query = (table.deleted == False)
        fieldnames = [table._id.name,
                      imagefield
                      ]
        # https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L1947
        for field in table:
            if field.name not in fieldnames and field.writable is False \
                and field.update is None and field.compute is None:
                fieldnames.append(field.name)
        fields = [table[f] for f in fieldnames]

        # Get callbacks
        get_config = s3db.get_config
        onvalidation = get_config(tablename, "update_onvalidation") or \
                       get_config(tablename, "onvalidation")
        onaccept = get_config(tablename, "update_onaccept") or \
                   get_config(tablename, "onaccept")
        update_realm = get_config(tablename, "update_realm")
        if update_realm:
            set_realm_entity = current.auth.set_realm_entity
        update_super = s3db.update_super

        for row in reader:
            if row != None:
                # Open the file
                image = row["file"]
                try:
                    # Extract the path to the CSV file, image should be in
                    # this directory, or relative to it
                    path = os.path.split(filename)[0]
                    imagepath = os.path.join(path, image)
                    openFile = open(imagepath, "rb")
                except IOError:
                    current.log.error("Unable to open image file %s" % image)
                    continue
                image_source = BytesIO(openFile.read())
                # Get the id of the resource
                query = base_query & (idfield == row["id"])
                record = db(query).select(limitby = (0, 1),
                                          *fields).first()
                try:
                    record_id = record.id
                except AttributeError:
                    current.log.error("Unable to get record %s of the resource %s to attach the image file to" % (row["id"], tablename))
                    continue
                # Create and accept the form
                form = SQLFORM(table, record, fields=["id", imagefield])
                form_vars = Storage()
                form_vars._formname = "%s/%s" % (tablename, record_id)
                form_vars.id = record_id
                source = Storage()
                source.filename = imagepath
                source.file = image_source
                form_vars[imagefield] = source
                if form.accepts(form_vars, onvalidation=onvalidation):
                    # Audit
                    audit("update", prefix, name, form=form,
                          record=record_id, representation="csv")

                    # Update super entity links
                    update_super(table, form_vars)

                    # Update realm
                    if update_realm:
                        set_realm_entity(table, form_vars, force_update=True)

                    # Execute onaccept
                    callback(onaccept, form, tablename=tablename)
                else:
                    for (key, error) in form.errors.items():
                        current.log.error("error importing logo %s: %s %s" % (image, key, error))

    # -------------------------------------------------------------------------
    @staticmethod
    def import_font(url):
        """
            Install a Font
        """

        if url == "unifont":
            #url = "http://unifoundry.com/pub/unifont-7.0.06/font-builds/unifont-7.0.06.ttf"
            #url = "http://unifoundry.com/pub/unifont-10.0.07/font-builds/unifont-10.0.07.ttf"
            url = "http://unifoundry.com/pub/unifont/unifont-13.0.01/font-builds/unifont-13.0.01.ttf"
            # Rename to make version upgrades be transparent
            filename = "unifont.ttf"
            extension = "ttf"
        else:
            filename = url.split("/")[-1]
            filename, extension = filename.rsplit(".", 1)

            if extension not in ("ttf", "gz", "zip"):
                current.log.warning("Unsupported font extension: %s" % extension)
                return

            filename = "%s.ttf" % filename

        fontPath = os.path.join(current.request.folder, "static", "fonts")
        if os.path.exists(os.path.join(fontPath, filename)):
            current.log.warning("Using cached copy of %s" % filename)
            return

        # Download as we have no cached copy

        # Copy the current working directory to revert back to later
        cwd = os.getcwd()

        # Set the current working directory
        os.chdir(fontPath)
        try:
            _file = fetch(url)
        except URLError as exception:
            current.log.error(exception)
            # Revert back to the working directory as before.
            os.chdir(cwd)
            return

        if extension == "gz":
            import tarfile
            tf = tarfile.open(fileobj = StringIO(_file))
            tf.extractall()

        elif extension == "zip":
            import zipfile
            zf = zipfile.ZipFile(StringIO(_file))
            zf.extractall()

        else:
            f = open(filename, "wb")
            f.write(_file)
            f.close()

        # Revert back to the working directory as before.
        os.chdir(cwd)

    # -------------------------------------------------------------------------
    def import_remote_csv(self, url, prefix, resource, stylesheet):
        """ Import CSV files from remote servers """

        extension = url.split(".")[-1]
        if extension not in ("csv", "zip"):
            current.log.error("error importing remote file %s: invalid extension" % (url))
            return

        # Copy the current working directory to revert back to later
        cwd = os.getcwd()

        # Shortcut
        os_path = os.path
        os_path_exists = os_path.exists
        os_path_join = os_path.join

        # Create the working directory
        TEMP = os_path_join(cwd, "temp")
        if not os_path_exists(TEMP): # use web2py/temp/remote_csv as a cache
            import tempfile
            TEMP = tempfile.gettempdir()
        tempPath = os_path_join(TEMP, "remote_csv")
        if not os_path_exists(tempPath):
            try:
                os.mkdir(tempPath)
            except OSError:
                current.log.error("Unable to create temp folder %s!" % tempPath)
                return

        filename = url.split("/")[-1]
        if extension == "zip":
            filename = filename.replace(".zip", ".csv")
        if os_path_exists(os_path_join(tempPath, filename)):
            current.log.warning("Using cached copy of %s" % filename)
        else:
            # Download if we have no cached copy
            # Set the current working directory
            os.chdir(tempPath)
            try:
                _file = fetch(url)
            except URLError as exception:
                current.log.error(exception)
                # Revert back to the working directory as before.
                os.chdir(cwd)
                return

            if extension == "zip":
                # Need to unzip
                import zipfile
                try:
                    myfile = zipfile.ZipFile(StringIO(_file))
                except zipfile.BadZipfile as exception:
                    # e.g. trying to download through a captive portal
                    current.log.error(exception)
                    # Revert back to the working directory as before.
                    os.chdir(cwd)
                    return
                files = myfile.infolist()
                for f in files:
                    filename = f.filename
                    extension = filename.split(".")[-1]
                    if extension == "csv":
                        _file = myfile.read(filename)
                        _f = open(filename, "w")
                        _f.write(_file)
                        _f.close()
                        break
                myfile.close()
            else:
                f = open(filename, "w")
                f.write(_file)
                f.close()

            # Revert back to the working directory as before.
            os.chdir(cwd)

        task = [1, prefix, resource,
                os_path_join(tempPath, filename),
                os_path_join(current.request.folder,
                             "static",
                             "formats",
                             "s3csv",
                             prefix,
                             stylesheet
                             ),
                None
                ]
        self.execute_import_task(task)

    # -------------------------------------------------------------------------
    @staticmethod
    def import_script(filename):
        """
            Run a custom Import Script

            @ToDo: Report Errors during Script run to console better
        """

        from gluon.cfs import getcfs
        from gluon.compileapp import build_environment
        from gluon.restricted import restricted

        environment = build_environment(current.request, current.response, current.session)
        environment["current"] = current
        environment["auth"] = current.auth
        environment["db"] = current.db
        environment["gis"] = current.gis
        environment["s3db"] = current.s3db
        environment["settings"] = current.deployment_settings

        code = getcfs(filename, filename, None)
        restricted(code, environment, layer=filename)

    # -------------------------------------------------------------------------
    def import_task(self,
                    task_name,
                    args_json = None,
                    vars_json = None
                    ):
        """
            Import a Scheduled Task
        """

        # Store current value of Bulk
        bulk = current.response.s3.bulk
        # Set Bulk to true for this parse
        current.response.s3.bulk = True
        validator = IS_JSONS3()
        if args_json:
            task_args, error = validator(args_json)
            if error:
                self.errorList.append(error)
                return
        else:
            task_args = []
        if vars_json:
            all_vars, error = validator(vars_json)
            if error:
                self.errorList.append(error)
                return
        else:
            all_vars = {}
        # Restore bulk setting
        current.response.s3.bulk = bulk

        kwargs = {}
        task_vars = {}
        options = ("function_name",
                   "start_time",
                   "next_run_time",
                   "stop_time",
                   "repeats",
                   "period", # seconds
                   "timeout", # seconds
                   "enabled", # None = Enabled
                   "group_name",
                   "ignore_duplicate",
                   "sync_output",
                   )
        for var in all_vars:
            if var in options:
                kwargs[var] = all_vars[var]
            else:
                task_vars[var] = all_vars[var]

        current.s3task.schedule_task(task_name.split(os.path.sep)[-1], # Strip the path
                                     args = task_args,
                                     vars = task_vars,
                                     **kwargs
                                     )

    # -------------------------------------------------------------------------
    def import_xml(self,
                   filepath,
                   prefix,
                   resourcename,
                   dataformat,
                   source_type = None,
                   ):
        """
            Import XML data using an XSLT: static/formats/<dataformat>/import.xsl
            Setting the source_type is possible
        """

        # Remove any spaces and enclosing double quote
        prefix = prefix.strip('" ')
        resourcename = resourcename.strip('" ')

        try:
            source = open(filepath, "rb")
        except IOError:
            error_string = "prepopulate error: file %s missing"
            self.errorList.append(error_string % filepath)
            return

        stylesheet = os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  dataformat,
                                  "import.xsl")
        try:
            xslt_file = open(stylesheet, "r")
        except IOError:
            error_string = "prepopulate error: file %s missing"
            self.errorList.append(error_string % stylesheet)
            return
        else:
            xslt_file.close()

        tablename = "%s_%s" % (prefix, resourcename)
        resource = current.s3db.resource(tablename)

        if tablename not in self.customised:
            # Customise the resource
            customise = current.deployment_settings.customise_resource(tablename)
            if customise:
                request = S3Request(prefix, resourcename, current.request)
                customise(request, tablename)
                self.customised.append(tablename)

        auth = current.auth
        auth.rollback = True
        try:
            resource.import_xml(source,
                                stylesheet = stylesheet,
                                source_type = source_type,
                                )
        except SyntaxError as e:
            self.errorList.append("WARNING: import error - %s (file: %s, stylesheet: %s/import.xsl)" %
                                 (e, filepath, dataformat))
            auth.rollback = False
            return

        if not resource.error:
            current.db.commit()
        else:
            # Must roll back if there was an error!
            error = resource.error
            self.errorList.append("%s - %s: %s" % (
                                  filepath, tablename, error))
            errors = current.xml.collect_errors(resource)
            if errors:
                self.errorList.extend(errors)
            current.db.rollback()

        auth.rollback = False

    # -------------------------------------------------------------------------
    def perform_tasks(self, path):
        """
            Load and then execute the import jobs that are listed in the
            descriptor file (tasks.cfg)
        """

        self.load_descriptor(path)
        for task in self.tasks:
            if task[0] == 1:
                self.execute_import_task(task)
            elif task[0] == 2:
                self.execute_special_task(task)

# END =========================================================================
