"""
    Cascading Merge of Records

    Copyright: 2012-2024 (c) Sahana Software Foundation

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

from gluon import current
from gluon.storage import Storage

from s3dal import Field, original_tablename

from ..tools import s3_get_foreign_key

# =============================================================================
class MergeProcess:
    """
        Low-level process to merge two records; can be subclassed and extended
        with resource-specific actions before and after the actual merge to
        handle non-generalizable relationships (to do so, configure the class
        for the table as "merge_process")

        Notes:
            - Any exceptions during the process will immediately roll back
              the current DB transaction, so this should always be called
              in an isolated transaction without any other relevant writes;
              in particular one should not run multiple merges within a
              single translation (e.g. from the CLI)
            - MergeProcesses can produce unexpected inconsistencies in
              complex models, which then require additional cleaning up.
              Therefore it is not recommended to expose this functionality
              to users with insufficient knowledge/privileges to perform
              such cleanups
            - In many cases, it is safer and will maintain better traceability
              to archive or otherwise deactivate duplicate records rather than
              merging them - this tool should therefore be used with much
              consideration and not as a routine means to fix user mistakes
    """

    def __init__(self, resource, main=True):
        """
            Args:
                resource: the resource
                main: indicator for recursive calls
        """

        self.resource = resource
        self.main = main

    # -------------------------------------------------------------------------
    @classmethod
    def merge(cls, resource, original_id, duplicate_id, replace=None, update=None, main=True):
        """
            Instantiates and starts a merge process for the resource,
            applying the "merge" table setting for customized merge
            processes

            Args:
                resource: the target CRUDResource
                original_id: the record ID of the original (to keep)
                duplicate_id: the record ID of the duplicate (to merge and remove)
                replace: list of field names to replace values in the original
                         with those from the duplicate
                update: dict of {fieldname: value} with additional updates to
                        apply to the original
                main: indicator for recursive calls

            Returns:
                success True|False

            Raises:
                NotImplementedError: if no suitable merge process for the
                                     resource is available (e.g. immutable
                                     resources)
                S3PermissionError: if the current user is not permitted to
                                   perform the merge
                KeyError: if either of the records cannot be found
                RuntimeError: for any other error during the process

            Note:
                This method can only be run for master resources, not for
                components.
        """

        if resource.get_config("immutable"):
            process = None
        else:
            process = resource.get_config("merge_process", cls)

        if not process:
            # Merge not permitted
            raise NotImplementedError("No merge process available for this resource")

        merge = process(resource, main=main)
        try:
            return merge(original_id, duplicate_id, replace=replace, update=update)
        except Exception:
            current.db.rollback()
            raise

    # -------------------------------------------------------------------------
    def __call__(self,
                 original_id,
                 duplicate_id,
                 replace = None,
                 update = None,
                 ):
        """
            Merges a duplicate record into its original and removes the
            duplicate, updating all references in the database.

            Args:
                original_id: the ID of the original record
                duplicate_id: the ID of the duplicate record
                replace: list fields names for which to replace the
                         values in the original record with the values
                         of the duplicate
                update: dict of {field:value} to update the final record
                main: internal indicator for recursive calls

            Returns:
                success True|False

            Note:
                The merge process should normally be instantiated and run
                via MergeProcess.merge; otherwise the caller must take care
                to choose the correct process class for the target resource
                and to roll back the DB transaction if any Exception occurs.
        """

        db = current.db

        resource = self.resource
        table = resource.table
        tablename = resource.tablename

        # Check that merge is permitted for this resource
        if resource.get_config("immutable"):
            raise RuntimeError("Must not merge %s records" % tablename)

        # Check that merge is run from a master resource
        if resource.parent:
            raise RuntimeError("Must not merge from component")

        # Check permissions
        auth = current.auth
        has_permission = auth.s3_has_permission
        permitted = has_permission("update", table, record_id=original_id) and \
                    has_permission("delete", table, record_id=duplicate_id)
        if not permitted:
            raise auth.permission.error("Operation not permitted")

        # Load all models, including lazy tables
        s3db = current.s3db
        if self.main:
            s3db.load_all_models()
        if db._lazy_tables:
            # Must roll out all lazy tables to detect dependencies
            for tn in list(db._LAZY_TABLES.keys()):
                db[tn]

        # Load the two records
        original = None
        duplicate = None
        query = table._id.belongs([original_id, duplicate_id])
        if "deleted" in table.fields:
            query &= (table.deleted == False)
        rows = db(query).select(table.ALL, limitby=(0, 2))
        for row in rows:
            record_id = row[table._id]
            if str(record_id) == str(original_id):
                original = row
                original_id = row[table._id]
            elif str(record_id) == str(duplicate_id):
                duplicate = row
                duplicate_id = row[table._id]
        msg = "Record not found: %s.%s"
        if original is None:
            raise KeyError(msg % (tablename, original_id))
        if duplicate is None:
            raise KeyError(msg % (tablename, duplicate_id))

        # Prepare the merge process
        self.prepare(original, duplicate)

        # Is this a super-entity?
        is_super_entity = table._id.name != "id" and "instance_type" in table.fields

        # Update all references
        self.update_references(original,
                               duplicate,
                               is_super_entity=is_super_entity,
                               replace=replace,
                               update=update,
                               )

        # Merge super-entity records
        self.merge_super(original, duplicate, replace=replace, update=update)

        # Merge and update original data
        self.update_data(original, duplicate, replace=replace, update=update)

        # Delete the duplicate
        if not is_super_entity:
            self.merge_realms(table, original, duplicate)
            self.delete_record(table, duplicate_id, replaced_by=original_id)

        # Perform any cleanups as necessary
        self.cleanup()

        # Success
        return True

    # -------------------------------------------------------------------------
    def prepare(self, original, duplicate):
        """
            Prepares the merge process; this can be overridden in subclasses
            to add resource-specific actions that need to be run before the
            actual merge

            Args:
                original: the original record
                duplicate: the duplicate record
        """

        # Nothing to do in the default MergeProcess
        pass

    # -------------------------------------------------------------------------
    def cleanup(self):
        """
            Performs cleanup actions at the end of the merge process;
            this can be overridden in subclasses to add resource-specific
            actions that can only be done after the actual merge
        """

        # Nothing to do in the default MergeProcess
        pass

    # -------------------------------------------------------------------------
    def update_references(self,
                          original,
                          duplicate,
                          is_super_entity=False,
                          replace=None,
                          update=None,
                          ):
        """
            Re-links referencing records from the duplicate to the original

            Args:
                original: the original record (Row)
                duplicate: the duplicate record (Row)
                is_super_entity: resource is a super-entity
                replace: list of field names to replace original values
                         with duplicate ones
                update: dict of {field: value} to update the original record
                        and/or related records

            Note:
                Records in tables configured as immutable will not be re-linked.

        """

        db = current.db
        s3db = current.s3db

        resource = self.resource
        table = resource.table
        tablename = resource.tablename

        single = self.get_single_components(resource)
        referenced_by = self.get_referenced_by(table)

        update_record = self.update_record
        delete_record = self.delete_record

        define_resource = s3db.resource
        for referee in referenced_by:

            if isinstance(referee, Field):
                tn, fn = referee.tablename, referee.name
            else:
                tn, fn = referee

            # Referencing table and field must exist
            rtable = db[tn] if tn in db else None
            if not rtable or fn not in rtable.fields:
                continue

            # Skip immutable tables
            otn = original_tablename(rtable)
            if s3db.get_config(otn, "immutable"):
                continue

            # Skip instance types of super-entities
            se = s3db.get_config(otn, "super_entity")
            if is_super_entity and \
               (isinstance(se, (list, tuple)) and tablename in se or se == tablename):
                continue

            # Handle single-components (one-to-one relationships)
            if tn in single:
                for component in single[tn]:

                    if fn == component.fkey:

                        # Single component => must reduce to one record
                        join = component.get_join()
                        pkey = component.pkey
                        lkey = component.lkey or component.fkey

                        # Get the component records
                        query = (table[pkey] == original[pkey]) & join
                        osub = db(query).select(limitby=(0, 1)).first()
                        query = (table[pkey] == duplicate[pkey]) & join
                        dsub = db(query).select(limitby=(0, 1)).first()

                        ctable = component.table
                        ctable_id = ctable._id

                        if dsub is None:
                            # No duplicate => skip this step
                            continue

                        elif not osub:
                            # No original => re-link the duplicate
                            dsub_id = dsub[ctable_id]
                            data = {lkey: original[pkey]}
                            update_record(ctable, dsub_id, dsub, data)

                        elif component.linked is not None:
                            # Duplicate link => remove it
                            dsub_id = dsub[ctable_id]
                            delete_record(ctable, dsub_id)

                        else:
                            # Two records => merge them
                            osub_id = osub[ctable_id]
                            dsub_id = dsub[ctable_id]
                            cresource = define_resource(component.tablename)
                            cresource.merge(osub_id, dsub_id,
                                            replace = replace,
                                            update = update,
                                            main = False,
                                            )

            # Find the foreign key
            rfield = rtable[fn]
            ktablename, key, multiple = s3_get_foreign_key(rfield)
            if not ktablename:
                if str(rfield.type) == "integer":
                    # Virtual reference
                    key = table._id.name
                else:
                    continue

            # Find the referencing records
            if multiple:
                query = rtable[fn].contains(duplicate[key])
            else:
                query = rtable[fn] == duplicate[key]
            rows = db(query).select(rtable._id, rtable[fn])

            # Update the referencing records
            for row in rows:
                if not multiple:
                    data = {fn:original[key]}
                else:
                    keys = [k for k in row[fn] if k != duplicate[key]]
                    if original[key] not in keys:
                        keys.append(original[key])
                    data = {fn: keys}
                update_record(rtable, row[rtable._id], row, data)

    # -------------------------------------------------------------------------
    @staticmethod
    def get_single_components(resource):
        """
            Finds all single-components of the target resource, so that
            their records can be merged rather than just re-linked (see
            limitations for this logic below)

            Returns:
                a dict {tablename: [CRUDResource, ...]}, i.e. component
                resources grouped by component table name

            Notes:
            - This is only reliable as far as the relevant component
              declarations have actually happened before calling merge:
              Where that happens in a controller (or customise_*) other
              than the one merge is being run from, those components may
              be treated as multiple instead!
            - Filtered components will never be deduplicated automatically,
              even if they are declared single
            - For single-components that are linked via link table, this
              will return the link resource instead (i.e. the links will
              be deduplicated, but not the linked records)
            - If a single component is not bound by a direct foreign key
              constraint, then it will not be picked up by update_references,
              regardless what this function finds
        """

        table = resource.table

        single = {}
        hooks = current.s3db.get_hooks(table)[1]
        if hooks:
            for alias, hook in hooks.items():
                if hook.multiple or hook.filterby:
                    continue
                component = resource.components.get(alias)
                if not component:
                    # E.g. module disabled
                    continue

                if component.link:
                    component = component.link

                ctablename = component.tablename
                if ctablename in single:
                    single[ctablename].append(component)
                else:
                    single[ctablename] = [component]

        return single

    # -------------------------------------------------------------------------
    @staticmethod
    def get_referenced_by(table):
        """
            Looks up referencing fields in other tables

            Args:
                table - the Table of the target resource

            Returns:
                List of field names (prefixed with their respective table name)

            Note:
                - Apart from direct foreign key constraints, this function
                  will also find list:reference types automatically
                - References to the primary key without foreign key constraint
                  (virtual references) must be declared in the table configuration
                  as "referenced_by"
                - Indirect references via key table (e.g. super-entity) will
                  not be picked up by this function, and it is invalid to declare
                  them as virtual references; it may though be possible to
                  declare them as components of the super-entity itself, and
                  then they will be picked up by the merge_super sub-process
                  instead
        """

        tablename = original_tablename(table)

        # Find all referencing fields
        referenced_by = list(table._referenced_by)

        # Append virtual references
        virtual_references = current.s3db.get_config(tablename, "referenced_by")
        if virtual_references:
            referenced_by.extend(virtual_references)

        # Find and append list:references
        for t in current.db:
            for f in t:
                ftype = str(f.type)
                if ftype[:14] == "list:reference" and \
                   ftype[15:15+len(tablename)] == tablename:
                    referenced_by.append((t._tablename, f.name))

        return referenced_by

    # -------------------------------------------------------------------------
    def merge_super(self, original, duplicate, replace=None, update=None):
        """
            Recursive sub-process to merge all corresponding super-entity
            entries belonging to the target records

            Args:
                original: the original Row
                duplicate: the duplicate Row
                replace: list of field names to replace original values
                         with duplicate ones
                update: dict of {field: value} to update the original record
                        and/or related records

            Notes:
                This will not actually remove the duplicate entry (which
                would be invalid), but only re-link all records referencing
                it
        """

        resource = self.resource
        table = resource.table
        tablename = resource.tablename

        s3db = current.s3db

        pkey = table._id.name
        original_id = original[pkey]
        duplicate_id = duplicate[pkey]

        super_entities = resource.get_config("super_entity")
        if not super_entities:
            return

        if not isinstance(super_entities, (list, tuple)):
            super_entities = [super_entities]

        for super_entity in super_entities:

            super_table = s3db.table(super_entity)
            if not super_table:
                continue
            superkey = super_table._id.name

            skey_o = original[superkey]
            if not skey_o:
                msg = "No %s found in %s.%s" % (superkey, tablename, original_id)
                current.log.warning(msg)
                s3db.update_super(table, original)
                skey_o = original[superkey]
            if not skey_o:
                continue
            skey_d = duplicate[superkey]
            if not skey_d:
                msg = "No %s found in %s.%s" % (superkey, tablename, duplicate_id)
                current.log.warning(msg)
                continue

            sresource = s3db.resource(super_entity)
            sresource.merge(skey_o, skey_d, replace=replace, update=update, main=False)

    # -------------------------------------------------------------------------
    def update_data(self, original, duplicate, replace=None, update=None):
        """
            Performs the update of the original

            Args:
                original: the original Row
                duplicate: the duplicate Row
                replace: list of field names to replace original values
                         with duplicate ones
                update: dict of {field: value} to update the original record
                        and/or related records

        """

        table = self.resource.table

        pkey = table._id.name
        original_id = original[pkey]
        duplicate_id = duplicate[pkey]

        update_record = self.update_record

        data = Storage()

        fieldname = self.fieldname
        if replace:
            for k in replace:
                fn = fieldname(k)
                if fn and fn in duplicate:
                    data[fn] = duplicate[fn]
        if update:
            for k, v in update.items():
                fn = fieldname(k)
                if fn in table.fields:
                    data[fn] = v
        if len(data):
            r = None
            p = Storage([(fn, "__deduplicate_%s__" % fn)
                         for fn in data
                         if table[fn].unique and \
                            table[fn].type == "string" and \
                            data[fn] == duplicate[fn]])
            if p:
                r = Storage([(fn, original[fn]) for fn in p])
                update_record(table, duplicate_id, duplicate, p)
            update_record(table, original_id, original, data)
            if r:
                update_record(table, duplicate_id, duplicate, r)

    # -------------------------------------------------------------------------
    @staticmethod
    def update_record(table, record_id, row, data):
        """
            Updates a record, including post-processing the update

            Args:
                table: the target Table
                record_id: the target record ID
                row: the target Row
                data: a dict {fieldname: value} with the update

            Raises:
                RuntimeError: if the update failed
        """

        form = Storage(vars = Storage([(f, row[f])
                              for f in table.fields if f in row]))
        form.vars.update(data)
        try:
            current.db(table._id==row[table._id]).update(**data)
        except Exception:
            raise RuntimeError("Could not update %s.%s" % (table._tablename, record_id))

        s3db = current.s3db
        s3db.update_super(table, form.vars)
        current.auth.s3_set_record_owner(table, row[table._id], force_update=True)
        s3db.onaccept(table, form, method="update")

    # -------------------------------------------------------------------------
    @staticmethod
    def delete_record(table, record_id, replaced_by=None):
        """
            Deletes a duplicate record

            Args:
                table: the target Table
                record_id: the ID of the record to delete
                replaced_by: the ID of the record that replaces the deleted
                             record, for tracing

            Raises:
                RuntimeError: if the deletion failed
        """

        if replaced_by is not None:
            replaced_by = {str(record_id): replaced_by}

        resource = current.s3db.resource(table, id=record_id)
        success = resource.delete(replaced_by=replaced_by, cascade=True)

        if not success:
            raise RuntimeError("Could not delete %s.%s (%s)" % \
                  (resource.tablename, record_id, resource.error))

    # -------------------------------------------------------------------------
    @staticmethod
    def merge_realms(table, original, duplicate):
        """
            Merge the realms of two person entities (update all
            realm_entities in all records from duplicate to original)

            Args:
                table: the table original and duplicate belong to
                original: the original record
                duplicate: the duplicate record
        """

        if "pe_id" not in table.fields:
            return

        original_pe_id = original["pe_id"]
        duplicate_pe_id = duplicate["pe_id"]

        db = current.db

        for t in db:
            if "realm_entity" in t.fields:

                query = (t.realm_entity == duplicate_pe_id)
                if "deleted" in t.fields:
                    query &= (t.deleted == False)
                db(query).update(realm_entity = original_pe_id)

    # -------------------------------------------------------------------------
    def fieldname(self, key):
        """
            Resolves the given key from replace/update parameters into
            a field name

            Args:
                key: the key (see Notes)
            Returns:
                the field name, or None if the key does not apply to the
                current target resource

            Notes:
                - a field name without prefix applies only to the master
                  resource, i.e. the resource the merge process is run from
                - a field name with a component alias as prefix (e.g.
                  "person_details.nationality") applies only to a component
                  with that alias
                - a field name with ~ as prefix applies to all components,
                  but not to the master resource
        """

        fn = None
        if "." in key:
            alias, fn = key.split(".", 1)
            if alias not in ("~", self.resource.alias):
                fn = None
        elif self.main:
            fn = key
        return fn

# END =========================================================================
