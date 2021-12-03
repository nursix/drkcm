"""
    Interactive Spreadsheet Importer

    Copyright: 2011-2021 (c) Sahana Software Foundation

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

__all__ = ("SpreadsheetImporter",
           )

import os
import sys

from lxml import etree

from gluon import current, redirect, URL, \
                  IS_FILE, IS_NOT_EMPTY, SQLFORM, \
                  A, B, DIV, INPUT, LI, P, TABLE, TBODY, TD, TFOOT, TH, TR, UL

from s3dal import Field

from ..tools import get_crud_string, s3_mark_required, s3_str, s3_addrow

from .base import CRUDMethod

# Supported spreadsheet formats {extension:format}
FORMATS = {"csv": "csv",
           "xls": "xls",
           "xlsx": "xlsx",
           "xslm": "xslx",
           }

# =============================================================================
class SpreadsheetImporter(CRUDMethod):
    """
        Interactive Spreadsheet Importer
    """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Full-page method

            Args:
                r: the CRUDRequest
                attr: controller parameters (see below)

            Keyword Args:
                csv_extra_fields: add values to each row in the CSV,
                                  a list of dicts of one of these formats:
                                    {label, field} - get the field value by adding
                                                    adding the field to the upload
                                                    form (a Field instance)
                                    {label, value} - add a fixed value
                csv_stylesheet: path to the XSLT transformation stylesheet,
                                    - the stylesheet name (as string)
                                      => static/formats/s3csv/<controller>/<name.xsl>
                                    - a tuple to construct a path
                                      => relative to static/formats/s3csv
                csv_template: path elements to construct a link for download
                              of a CSV template, like
                                /static/formats/<format>/<prefix>/<name.ext>
                                - just "name.ext" (as string)
                                  => format defaults to s3csv
                                  => prefix defaults to current controller
                                  => ext defaults to "csv" if omitted
                                - a tuple ("format", "prefix", "name.ext")
        """

        # Target table for the data import
        tablename = self.tablename

        # Check authorization
        has_permission = current.auth.s3_has_permission
        authorised = has_permission("create", tablename) or \
                     has_permission("update", tablename)
        if not authorised:
            r.unauthorised()

        if r.http == "GET":
            if r.representation == "aadata" or "job_id" in r.get_vars:
                # Pagination/filter request from items list
                job_id = r.get_vars.get("job_id")
                output = self.select_items(job_id, r, **attr)
            else:
                # Show upload form
                output = self.upload(r, **attr)
        elif r.http == "POST":
            if r.post_vars.get("job_id"):
                # Commit selected items
                output = self.commit(r, **attr)
            else:
                # Process upload form (trial import) and show items list
                output = self.upload(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    # Workflow (upload => select_items => commit)
    # -------------------------------------------------------------------------
    def upload(self, r, **attr):
        """
            Request/submit upload form

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        resource = self.resource

        form = self.upload_form(r, **attr)

        if form.accepts(r.post_vars,
                        current.session,
                        ):

            # Process extra fields
            extra_fields = attr.get("csv_extra_fields")
            if extra_fields:
                extra_data = self.get_extra_data(form, extra_fields)
            else:
                extra_data = None

            # Get transformation stylesheet (file path)
            stylesheet = self.get_stylesheet(r, **attr)
            if not stylesheet:
                r.error(501, "Data format configuration not found")

            # Determine source file location and format
            fname = form.vars.file
            fpath = os.path.join(current.request.folder, "uploads", "imports", fname)
            ext = os.path.splitext(fname)[1]
            fmt = FORMATS.get(ext[1:]) if ext else None
            if not fmt:
                r.error(400, "Unsupported file type")

            # Arguments for XSLT stylesheet (from GET vars)
            args = {}
            mode = r.get_vars.get("xsltmode")
            if mode:
                args["mode"] = mode

            # Import
            try:
                with open(fpath, "r", encoding="utf-8") as source:
                    job_id = self.import_from_source(resource,
                                                     source,
                                                     fmt = fmt,
                                                     stylesheet = stylesheet,
                                                     extra_data = extra_data,
                                                     #commit = False,
                                                     **args,
                                                     )
            except ValueError:
                error = {"error": sys.exc_info()[1]}
                current.session.error = current.T("Import failed (%(error)s)") % error
                redirect(r.url(method="import"))

            # Remove the uploaded file
            try:
                os.remove(fpath)
            except OSError:
                pass
            output = self.select_items(job_id, r, **attr)
        else:
            output = {"form": form,
                      "title": get_crud_string(self.tablename, "title_upload"),
                      }
            current.response.view = self._view(r, "create.html")

        return output

    # -------------------------------------------------------------------------
    def select_items(self, job_id, r, **attr):
        """
            View a pending import job after trial phase and select items to commit
                - provides a table of import items
                - pre-selects all items without error
                - submitting the selection goes to commit()

            Args:
                job_id: the import job UUID (or None to read from request vars)
                r: the CRUDRequest
                attr: controller parameters
        """

        T = current.T

        if job_id is None:
            job_id = r.vars.get("job_id")
        if not job_id:
            r.error(400, T("No import job specified"))

        s3db = current.s3db
        s3 = current.response.s3

        itable = s3db.s3_import_item

        field = itable.element
        field.represent = self.element_represent

        # Target resource tablename
        ttablename = r.resource.tablename

        from ..resource import FS
        query = (FS("job_id") == job_id) & \
                (FS("tablename") == ttablename)
        iresource = s3db.resource(itable, filter=query)

        # Get a list of the records that have an error of None
        query =  (itable.job_id == job_id) & \
                 (itable.tablename == r.resource.tablename)
        rows = current.db(query).select(itable.id, itable.error)
        select_list = []
        error_list = []
        for row in rows:
            if row.error:
                error_list.append(str(row.id))
            else:
                select_list.append("%s" % row.id)

        representation = r.representation
        get_vars = r.get_vars

        # Datatable Filter
        list_fields = ["id", "element", "error"]
        if representation == "aadata":
            searchq, orderby, left = iresource.datatable_filter(list_fields, get_vars)
            if searchq is not None:
                iresource.add_filter(searchq)
        else:
            orderby, left = None, None
        if not orderby:
            orderby, left = ~iresource.table.error, None

        # Pagination
        if representation == "aadata":
            start, limit = self._limits(get_vars)
        else:
            start, limit = None, 0

        # How many records per page?
        settings = current.deployment_settings
        display_length = settings.get_ui_datatables_pagelength()
        if not limit:
            limit = 2 * display_length

        # Generate datatable
        dt, totalrows = iresource.datatable(fields = list_fields,
                                            left = left,
                                            start = start,
                                            limit = limit,
                                            orderby = orderby,
                                            list_id = "import-items",
                                            )

        dt_bulk_actions = [current.T("Import")]

        if representation == "aadata":
            # Pagination request (Ajax)
            displayrows = totalrows
            totalrows = iresource.count()
            draw = int(get_vars.draw or 0)

            output = dt.json(totalrows,
                             displayrows,
                             draw,
                             dt_bulk_actions = dt_bulk_actions,
                             )
        else:
            # Initial HTML response
            displayrows = totalrows

            # Generate formkey and store in session
            import uuid
            formkey = uuid.uuid4()
            current.session["_formkey[%s/%s]" % (ttablename, job_id)] = str(formkey)

            ajax_url = "/%s/%s/%s/import.aadata?job_id=%s" % (r.application,
                                                              r.controller,
                                                              r.function,
                                                              job_id,
                                                              )

            # Generate the datatable HTML
            s3.no_formats = True

            items =  dt.html(totalrows,
                             displayrows,
                             dt_formkey = formkey,
                             dt_pagination = True,
                             dt_pageLength = display_length,
                             dt_base_url = r.url(method="import", vars={"job_id": job_id}),
                             dt_permalink = None,
                             dt_ajax_url = ajax_url,
                             dt_bulk_actions = dt_bulk_actions,
                             dt_bulk_selected = select_list,
                             dt_styles = {"dtwarning": error_list},
                             )

            # Append the job_id to the datatable form
            job = INPUT(_type = "hidden",
                        _name = "job_id",
                        _value = "%s" % job_id,
                        )
            items.append(job)

            # Add toggle-button for item details
            SHOW = T("Display Details")
            HIDE = T("Hide Details")
            s3.actions = [{"label": s3_str(SHOW),
                           "_class": "action-btn toggle-item",
                           },
                          ]
            script = '''$('#import-items').on('click','.toggle-item',function(){b=$(this);$('.import-item-details',b.closest('tr')).toggle().each(function(){b.text($(this).is(':visible')?'%s':'%s')})})'''
            s3.jquery_ready.append(script % (HIDE, SHOW))

            # View
            current.response.view = self._view(r, "list.html")
            output = {"title": T("Select records to import"),
                      "items": items,
                      }

        return output

    # -------------------------------------------------------------------------
    def commit(self, r, **attr):
        """
            Commit the selected items (coming from select_items())

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        T = current.T

        # Get the import job ID
        post_vars = r.post_vars
        job_id = post_vars.get("job_id")
        if not job_id:
            r.error(400, T("Missing import job ID"))

        # Verify formkey
        tablename = r.resource.tablename
        formkey = current.session.get("_formkey[%s/%s]" % (tablename, job_id))
        if not formkey or post_vars.get("_formkey") != formkey:
            r.unauthorised()

        # Check that the job exists
        s3db = current.s3db
        jtable = s3db.s3_import_job
        query = (jtable.job_id == job_id)
        job = current.db(query).select(jtable.id,
                                       limitby = (0, 1),
                                       ).first()
        if not job:
            r.error(404, T("Import job not found"))

        # Items selected in the items list
        selected = post_vars.get("selected", [])
        if isinstance(selected, str):
            selected = [item for item in selected.split(",") if item.strip()]

        # Apply selection mode
        mode = post_vars.get("mode")
        if mode == "Inclusive":
            select_items = selected
        elif mode == "Exclusive":
            itable = s3db.s3_import_item
            query = (itable.job_id == job_id) & \
                    (itable.tablename == tablename)
            if selected:
                query &= ~(itable.id.belongs(set(selected)))
            rows = current.db(query).select(itable.id)
            select_items = [str(row.id) for row in rows]
        else:
            r.error(400, T("Invalid select mode"))

        # Commit the job (will also delete the job)
        result = self.commit_import_job(r.resource,
                                        job_id = job_id,
                                        select_items = select_items,
                                        )

        # Create result message
        msg = "%s - %s - %s" % (T("%s records imported") % result["imported"],
                                T("%s records ignored") % result["skipped"],
                                T("%s records in error") % result["errors"],
                                )

        if result["errors"] != 0:
            current.session.error = msg
        elif result["skipped"] != 0:
            current.session.warning = msg
        else:
            current.session.confirmation = msg

        redirect(r.url(method="import", vars={}))

    # -------------------------------------------------------------------------
    # Utility methods
    # -------------------------------------------------------------------------
    def upload_form(self, r, **attr):
        """
            Construct the upload form

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                FORM
        """

        T = current.T

        request = current.request
        response = current.response
        settings = current.deployment_settings

        uploadfolder = os.path.join(request.folder, "uploads", "imports")

        # Form Fields
        formfields = [Field("file", "upload",
                            label = T("File"),
                            requires = [IS_NOT_EMPTY(error_message = T("Select a file"),
                                                     ),
                                        IS_FILE(extension = ["csv", "xls", "xlsx"],
                                                error_message = T("Unsupported file format"),
                                                ),
                                        ],
                            comment = T("Upload a file formatted according to the Template."),
                            uploadfolder = uploadfolder,
                            ),
                      ]

        # Add extra fields
        extra_fields = attr.get("csv_extra_fields")
        if extra_fields:
            for item in extra_fields:
                field = item.get("field")
                if not field:
                    continue
                label = item.get("label")
                if label:
                    field.label = label
                field.readable = field.writable = True
                formfields.append(field)

        # Generate labels (and mark required fields in the process)
        labels, has_required = s3_mark_required(formfields)
        response.s3.has_required = has_required

        # Form buttons
        SUBMIT = T("Upload")
        buttons = [INPUT(_type = "submit",
                         _value = SUBMIT,
                         ),
                   ]

        # Construct the form
        formstyle = settings.get_ui_formstyle()
        response.form_label_separator = ""
        form = SQLFORM.factory(table_name = "import_upload",
                               record = None,
                               labels = labels,
                               message = T("Import file uploaded"),
                               separator = "",
                               showid = False,
                               submit_button = SUBMIT,
                               upload = uploadfolder,
                               formstyle = formstyle,
                               buttons = buttons,
                               *formfields)

        # Identify form for CSS
        form.add_class("import_upload")

        template_url = self.get_template_url(r, **attr)
        if template_url:
            link = A(T("Download Template"), _href=template_url)
            s3_addrow(form, "", link, None, formstyle, "download_template__row", position=0)

        return form

    # -------------------------------------------------------------------------
    @staticmethod
    def get_template_url(r, **attr):
        """
            Get a download URL for the CSV template

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                URL (or None if no CSV template can be downloaded)
        """

        prefix, name = r.controller, r.function

        # Add the CSV template download link
        args = ["s3csv"]

        template = attr.get("csv_template", True)
        if template is True:
            args.extend([prefix, "%s.csv" % name])

        else:
            if isinstance(template, (tuple, list)):
                path, template = template[:-1], template[-1]
            elif isinstance(template, str):
                path = [prefix]
            else:
                return None

            args.extend(path)

            if os.path.splitext(template)[1][1:] not in FORMATS:
                # Assume CSV if no known spreadsheet extension found
                template = "%s.csv" % template
            args.append(template)

        fpath = os.path.join(r.folder, "static", "formats", *args)
        try:
            open(fpath, "r")
        except IOError:
            url = None
        else:
            url = URL(c="static", f="formats", args=args)

        return url

    # -------------------------------------------------------------------------
    @staticmethod
    def get_extra_data(upload_form, extra_fields):
        """
            Extract extra column data from the upload form

            Args:
                upload_form: the upload FORM
                extra_fields: the extra-fields specification

            Returns:
                dict {column_label: value}
        """

        form_vars = upload_form.vars
        extra_data = {}

        for f in extra_fields:

            # The column label
            label = f.get("label")
            if not label:
                continue

            field = f.get("field")
            if field:
                # Read value from upload form
                if field.name in form_vars:
                    data = form_vars[field.name]
                else:
                    # Fall back to field default
                    data = field.default
                value = data

                # Check if the field has options
                options = None
                requires = field.requires
                if not isinstance(requires, (list, tuple)):
                    requires = [requires]
                for validator in requires:
                    if hasattr(validator, "options"):
                        options = validator.options()
                        break

                # If the field has options, convert the selected
                # value into its representation string
                if options:
                    options = dict(options)
                    k = str(data)
                    if k in options:
                        value = options[k]
                        if hasattr(value, "m"):
                            value = value.m

            else:
                # A fixed value
                value = f.get("value")
                if value is None:
                    continue

            extra_data[label] = value

        return extra_data

    # -------------------------------------------------------------------------
    @staticmethod
    def get_stylesheet(r, **attr):
        """
            Get the XSLT transformation stylesheet

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                the path to the XSLT stylesheet
        """

        prefix, name = r.controller, r.function
        csv_stylesheet = attr.get("csv_stylesheet")

        path = os.path.join(r.folder, r.XSLT_PATH, "s3csv")
        ext = r.XSLT_EXTENSION

        if csv_stylesheet:
            if isinstance(csv_stylesheet, (tuple, list)):
                stylesheet = os.path.join(path, *csv_stylesheet)
            else:
                stylesheet = os.path.join(path, prefix, csv_stylesheet)
        else:
            xslt_filename = "%s.%s" % (name, ext)
            stylesheet = os.path.join(path, prefix, xslt_filename)

        if not os.path.exists(stylesheet):
            current.log.error("XSLT stylesheet not found: %s" % stylesheet)
            stylesheet = None

        return stylesheet

    # -------------------------------------------------------------------------
    @classmethod
    def import_from_source(cls,
                           resource,
                           source,
                           fmt = "csv",
                           stylesheet = None,
                           extra_data = None,
                           commit = False,
                           **args,
                           ):
        """
            Import spreadsheet data into a resource

            Args:
                resource: the target resource
                source: the source (file-like object)
                fmt: the source file format (in connection with source)
                extra_data: extra data to add to source rows (in connection with source)
                commit: whether to commit the import immediately (in connection with source)
                args: additional stylesheet args

            Returns:
                import job UUID
        """

        result = resource.import_xml(source,
                                     source_type = fmt,
                                     extra_data = extra_data,
                                     commit = commit,
                                     ignore_errors = True,
                                     stylesheet = stylesheet,
                                     **args)

        job_id = result.job_id
        if not job_id and result.error:
            raise ValueError(result.error)

        return job_id

    # -------------------------------------------------------------------------
    def element_represent(self, value):
        """
            Represent the import item XML element as details in the import
            item datatable

            Args:
                value: the XML element (as string)

            Returns:
                DIV containing a representation of the element
        """

        try:
            element = etree.fromstring(value)
        except (etree.ParseError, etree.XMLSyntaxError):
            return DIV(value)

        s3db = current.s3db
        table = s3db[element.get("name")]

        output = DIV()
        details = TABLE(_class="import-item-details")

        # Field values in main record
        header, rows = self.item_details(table, element)
        if header is not None:
            output.append(header)

        # Add component details, if present
        components = element.findall("resource")
        for component in components:
            ctablename = component.get("name")
            ctable = s3db.table(ctablename)
            if not ctable:
                continue
            cdetails = self.item_details(ctable, component, prefix=True)[1]
            rows.extend(cdetails)

        if rows:
            details.append(TBODY(rows))

        # Add error messages, if present
        errors = current.xml.collect_errors(element)
        if errors:
            details.append(TFOOT(TR(TH("%s:" % current.T("Errors")),
                                    TD(UL([LI(e) for e in errors])))))

        if rows == [] and components == []:
            # No field data in the main record, nor components
            # => target table containing only references?
            refdetail = TABLE(_class = "import-item-details")
            references = element.findall("reference")
            for reference in references:
                resource = reference.get("resource")
                tuid = reference.get("tuid")
                refdetail.append(TR(TD(resource), TD(tuid)))
            output.append(refdetail)
        else:
            output.append(details)

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def item_details(cls, table, element, prefix=False):
        """
            Show details of an import item

            Args:
                table: the table
                element: the S3XML resource-element
                prefix: prefix field names with the table name

            Returns:
                tuple (P(header), [TR(detail), ...])
        """

        header = None
        first_string = True
        header_text = lambda f, v: P(B("%s: " % f), v)

        details = []
        tablename = table._tablename

        for data_element in element.findall("data"):

            # Get the field name
            fname = data_element.get("field")

            # Skip unspecified, non-existent and WKT fields
            if not fname or fname not in table.fields or fname == "wkt":
                continue

            # Get the field and field type
            ftype = str(table[fname].type)

            # Decode the value
            value = data_element.get("value")
            if value is None:
                value = current.xml.xml_decode(data_element.text)
            value = s3_str(value)

            # Set main detail (header)
            if fname == "name":
                header = header_text(fname, value)
                first_string = False
            elif ftype == "string" and first_string:
                header = header_text(fname, value)
                first_string = False
            elif not header:
                header = header_text(fname, value)

            # Append detail to details table
            label = "%s.%s:" % (tablename, fname) if prefix else "%s:" % fname
            details.append(TR(TH(label), TD(value)))

        return (header, details)

    # -------------------------------------------------------------------------
    @classmethod
    def commit_import_job(cls,
                          resource,
                          job_id = None,
                          select_items = None,
                          ):
        """
            Commit a pending import job

            Args:
                resource: the target resource
                job_id: the import job ID
                selected_items: IDs of selected item

            Returns:
                import statistics, a dict {total, imported, skipped, errors}
        """

        db = current.db

        # Count matching top-level items in job
        itable = current.s3db.s3_import_item
        query = (itable.job_id == job_id) & \
                (itable.tablename == resource.tablename) & \
                (itable.parent == None)
        total = db(query).count()

        # Count selected top-level items in job
        query &= itable.id.belongs(select_items)
        selected = db(query).count()

        # Commit the job
        result = resource.import_xml(None,
                                     job_id = job_id,
                                     select_items = select_items,
                                     ignore_errors = True,
                                     )

        return {"total": total,
                "imported": result.count,
                "skipped": max(total - selected, 0),
                "errors": result.failed,
                }

# END =========================================================================
