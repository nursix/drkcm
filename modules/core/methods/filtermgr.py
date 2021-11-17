"""
    Saved Filters Manager

    Copyright: 2013-2021 (c) Sahana Software Foundation

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

__all__ = ("S3Filter",
           )

import json

from gluon import current
from gluon.storage import Storage
from gluon.tools import callback

from ..tools import JSONSEPARATORS

from .base import CRUDMethod

# =============================================================================
class S3Filter(CRUDMethod):
    """ Back-end for filter forms """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point for REST interface

            Args:
                r: the CRUDRequest
                attr: additional controller parameters
        """

        representation = r.representation
        output = None

        if representation == "options":
            # Return the filter options as JSON
            output = self._options(r, **attr)

        elif representation == "json":
            if r.http == "GET":
                # Load list of saved filters
                output = self._load(r, **attr)
            elif r.http == "POST":
                if "delete" in r.get_vars:
                    # Delete a filter
                    output = self._delete(r, **attr)
                else:
                    # Save a filter
                    output = self._save(r, **attr)
            else:
                r.error(405, current.ERROR.BAD_METHOD)

        elif representation == "html":
            output = self._form(r, **attr)

        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    def _form(self, r, **attr):
        """
            Get the filter form for the target resource as HTML snippet
                - GET filter.html

            Args:
                r: the CRUDRequest
                attr: additional controller parameters
        """

        r.error(501, current.ERROR.NOT_IMPLEMENTED)

    # -------------------------------------------------------------------------
    def _options(self, r, **attr):
        """
            Get the updated options for the filter form for the target
            resource as JSON.
                - GET filter.options

            Args:
                r: the CRUDRequest
                attr: additional controller parameters (ignored currently)

            Note:
                These use a fresh resource, so filter vars are not respected.
                s3.filter if respected, so if you need to filter the options, then
                can apply filter vars to s3.filter in customise() if the controller
                is not the same as the calling one!
        """

        resource = self.resource

        options = {}

        filter_widgets = resource.get_config("filter_widgets", None)
        if filter_widgets:
            fresource = current.s3db.resource(resource.tablename,
                                              filter = current.response.s3.filter,
                                              )

            for widget in filter_widgets:
                if hasattr(widget, "ajax_options"):
                    opts = widget.ajax_options(fresource)
                    if opts and isinstance(opts, dict):
                        options.update(opts)

        options = json.dumps(options, separators=JSONSEPARATORS)
        current.response.headers["Content-Type"] = "application/json"
        return options

    # -------------------------------------------------------------------------
    @staticmethod
    def _delete(r, **attr):
        """
            Delete a filter, responds to POST filter.json?delete=

            Args:
                r: the CRUDRequest
                attr: additional controller parameters
        """

        # Authorization, get pe_id
        auth = current.auth
        if auth.s3_logged_in():
            pe_id = current.auth.user.pe_id
        else:
            pe_id = None
        if not pe_id:
            r.unauthorised()

        # Read the source
        source = r.body
        source.seek(0)

        try:
            data = json.load(source)
        except ValueError:
            # Syntax error: no JSON data
            r.error(400, current.ERROR.BAD_SOURCE)

        # Try to find the record
        db = current.db
        s3db = current.s3db

        table = s3db.pr_filter
        record = None
        record_id = data.get("id")
        if record_id:
            query = (table.id == record_id) & \
                    (table.pe_id == pe_id)
            record = db(query).select(table.id,
                                      limitby = (0, 1)
                                      ).first()
        if not record:
            r.error(404, current.ERROR.BAD_RECORD)

        resource = s3db.resource("pr_filter", id = record_id)
        success = resource.delete(format = r.representation)

        if not success:
            r.error(400, resource.error)

        current.response.headers["Content-Type"] = "application/json"
        return current.xml.json_message(deleted = record_id)

    # -------------------------------------------------------------------------
    def _save(self, r, **attr):
        """
            Save a filter, responds to POST filter.json

            Args:
                r: the CRUDRequest
                attr: additional controller parameters
        """

        # Authorization, get pe_id
        auth = current.auth
        if auth.s3_logged_in():
            pe_id = current.auth.user.pe_id
        else:
            pe_id = None
        if not pe_id:
            r.unauthorised()

        # Read the source
        source = r.body
        source.seek(0)

        try:
            data = json.load(source)
        except ValueError:
            r.error(501, current.ERROR.BAD_SOURCE)

        # Try to find the record
        db = current.db
        s3db = current.s3db

        table = s3db.pr_filter
        record_id = data.get("id")
        record = None
        if record_id:
            query = (table.id == record_id) & \
                    (table.pe_id == pe_id)
            record = db(query).select(table.id,
                                      limitby = (0, 1)
                                      ).first()
            if not record:
                r.error(404, current.ERROR.BAD_RECORD)

        # Build new record
        filter_data = {
            "pe_id": pe_id,
            "controller": r.controller,
            "function": r.function,
            "resource": self.resource.tablename,
            "deleted": False,
        }

        title = data.get("title")
        if title is not None:
            filter_data["title"] = title

        description = data.get("description")
        if description is not None:
            filter_data["description"] = description

        query = data.get("query")
        if query is not None:
            filter_data["query"] = json.dumps(query)

        url = data.get("url")
        if url is not None:
            filter_data["url"] = url

        # Store record
        onaccept = None
        form = Storage(vars=filter_data)
        if record:
            success = db(table.id == record_id).update(**filter_data)
            if success:
                current.audit("update", "pr", "filter", form, record_id, "json")
                info = {"updated": record_id}
                onaccept = s3db.get_config(table, "update_onaccept",
                           s3db.get_config(table, "onaccept"))
        else:
            success = table.insert(**filter_data)
            if success:
                record_id = success
                current.audit("create", "pr", "filter", form, record_id, "json")
                info = {"created": record_id}
                onaccept = s3db.get_config(table, "update_onaccept",
                           s3db.get_config(table, "onaccept"))

        if onaccept is not None:
            form.vars["id"] = record_id
            callback(onaccept, form)

        # Success/Error response
        xml = current.xml
        if success:
            msg = xml.json_message(**info)
        else:
            msg = xml.json_message(False, 400)
        current.response.headers["Content-Type"] = "application/json"
        return msg

    # -------------------------------------------------------------------------
    def _load(self, r, **attr):
        """
            Load filters
                - GET filter.json or GET filter.json?load=<id>

            Args:
                r: the CRUDRequest
                attr: additional controller parameters
        """

        db = current.db
        table = current.s3db.pr_filter

        # Authorization, get pe_id
        auth = current.auth
        if auth.s3_logged_in():
            pe_id = current.auth.user.pe_id
        else:
            pe_id = None
        if not pe_id:
            r.unauthorised()

        # Build query
        query = (table.deleted == False) & \
                (table.resource == self.resource.tablename) & \
                (table.pe_id == pe_id)

        # Any particular filters?
        load = r.get_vars.get("load")
        if load:
            record_ids = [i for i in load.split(",") if i.isdigit()]
            if record_ids:
                if len(record_ids) > 1:
                    query &= table.id.belongs(record_ids)
                else:
                    query &= table.id == record_ids[0]
        else:
            record_ids = None

        # Retrieve filters
        rows = db(query).select(table.id,
                                table.title,
                                table.description,
                                table.query,
                                )

        # Pack filters
        filters = []
        for row in rows:
            filters.append({
                "id": row.id,
                "title": row.title,
                "description": row.description,
                "query": json.loads(row.query) if row.query else [],
            })

        # JSON response
        current.response.headers["Content-Type"] = "application/json"
        return json.dumps(filters, separators=JSONSEPARATORS)

# END =========================================================================
