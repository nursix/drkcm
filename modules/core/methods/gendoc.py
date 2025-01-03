"""
    Mailmerge Document Generator

    Copyright: 2024-2024 (c) Sahana Software Foundation

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

__all__ = ("GenerateDocument",
           )

import os

from io import BytesIO

from gluon import current, redirect, URL, A, DIV, P, UL, LI
from gluon.contenttype import contenttype
from gluon.streamer import DEFAULT_CHUNK_SIZE

from ..tools import s3_str, s3_format_fullname
from .base import CRUDMethod

# =============================================================================
class GenerateDocument(CRUDMethod):
    """ Generate DOCX documents from template with data from record """

    def apply_method(self, r, **attr):
        """
            Controller entry point, applies the method

            Args:
                r: the CRUDRequest
                attr: controller options for this request
        """

        # Error if mailmerge not available
        try:
            from mailmerge import MailMerge
        except ImportError:
            r.error(501, current.T("Docx-mailmerge library not installed"))

        output = None

        if r.http == "GET":
            if r.method == "templates":
                if r.representation == "html":
                    output = self.templates(r, **attr)
                else:
                    r.error(415, current.ERROR.BAD_FORMAT)
            elif r.method == "template":
                if r.representation == "docx":
                    output = self.document_from_template(r, **attr)
                else:
                    r.error(415, current.ERROR.BAD_FORMAT)
            else:
                r.error(404, current.ERROR.BAD_ENDPOINT)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def templates(self, r, **attr):
        """
            Returns a dialog to select a template

            Args:
                r: the CRUDRequest
                attr: controller options for this request
        """

        output = {}

        T = current.T
        output["title"] = "" #"%s:" % T("Select Template")

        templates = self.get_templates(r)
        if not templates:
            buttons = P(T("No document templates found"))
        else:
            person_id = r.id
            buttons = UL()
            bappend = buttons.append
            for t in templates:
                bappend(LI(A(t.name,
                             #_class = "action-btn",
                             _href = URL(args = [person_id, "template.docx"],
                                         vars = {"template": t.id},
                                         ),
                             _target = "_top",
                             )))

        output["item"] = DIV(buttons, _style="padding:1rem;")
        current.response.view = "plain.html"

        return output

    # -------------------------------------------------------------------------
    def document_from_template(self, r, **attr):
        """
            Generates the document from the selected template, using data
            from the context record

            Args:
                r: the CRUDRequest
                attr: controller options for this request
        """

        T = current.T

        try:
            from mailmerge import MailMerge
        except ImportError:
            r.error(501, current.T("Docx-mailmerge library not installed"))

        output = None

        person_id = r.id
        if not person_id:
            current.session.error = T("No Person selected")
            redirect(URL(args = None))

        # Find Template
        document_id = r.get_vars.get("template")
        if not document_id:
            r.error(400, current.ERROR.BAD_REQUEST)

        template_name, template_path = self.get_template(r, document_id)
        if not template_path:
            r.error(404, T("Template not found"))

        # Extract Data
        # TODO move into method
        resource = r.resource
        mailmerge_fields = current.deployment_settings.get_doc_mailmerge_fields()
        selectors = list(mailmerge_fields.values())

        # Always include the primary key of the resource
        if resource._id.name not in selectors:
            selectors = [resource._id.name] + selectors

        data = resource.select(selectors, represent=True, show_links=False)
        record = data.rows[0]
        rfields = {rfield.selector: rfield for rfield in data.rfields}

        # Format Data
        NONE = current.messages["NONE"]
        prefix = resource.prefix_selector

        doc_data = {}
        for key, selector in mailmerge_fields.items():
            if callable(selector):
                for k, v in selector(resource, record).items():
                    doc_data["%s_%s" % (key, k)] = s3_str(v)
            elif selector == "current_user.name":
                user = current.auth.user
                if user:
                    username = s3_format_fullname(fname = user.first_name,
                                                  lname = user.last_name,
                                                  )
                else:
                    username = T("Unknown User")
                doc_data[key] = s3_str(username)
            else:
                rfield = rfields.get(prefix(selector))
                if rfield:
                    value = record[rfield.colname]
                    doc_data[key] = s3_str(value)
                else:
                    doc_data[key] = NONE

        # Merge
        filename = "%s_%s.docx" % (template_name, person_id)
        stream = BytesIO()
        with MailMerge(template_path) as document:
            document.merge(**doc_data)
            document.write(stream)
        stream.seek(0)

        # Response headers
        disposition = "attachment; filename=\"%s\"" % filename
        response = current.response
        response.headers["Content-Type"] = contenttype(".docx")
        response.headers["Content-disposition"] = disposition

        # Output
        output = response.stream(stream,
                                 chunk_size = DEFAULT_CHUNK_SIZE,
                                 request = r,
                                 )

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def configure(cls, tablename):
        """
            Configure this method for a table

            Args:
                tablename: the table name
        """

        # TODO Currently not supported for any table other than pr_person
        if tablename != "pr_person":
            raise NotImplementedError("GenerateDocument not supported for %s" % tablename)

        s3db = current.s3db

        s3db.set_method(tablename, method="templates", action=cls)
        s3db.set_method(tablename, method="template", action=cls)

    # -------------------------------------------------------------------------
    @classmethod
    def get_templates(cls, r):
        """
            Looks up available document templates for the current user

            Args:
                r - the context CRUDRequest; unused in base class, but
                    can be used by subclasses for context-dependent lookups

            Returns:
                doc_document Rows (id, name)
        """

        query = cls.template_query(r)
        if query is None:
            return None

        table = current.s3db.doc_document
        templates = current.db(query).select(table.id,
                                             table.name,
                                             orderby = table.name,
                                             )
        return templates

    # -------------------------------------------------------------------------
    @classmethod
    def get_template(cls, r, document_id):
        """
            Looks up a particular document template

            Args:
                r - the context CRUDRequest; unused in base class, but
                    can be used by subclasses for context-dependent lookups
                document_id - the doc_document record ID

            Returns:
                tuple (template_name, file_path)
        """

        query = cls.template_query(r)
        if query is None:
            return None, None

        table = current.s3db.doc_document
        query = (table.id == document_id) & query

        template = current.db(query).select(table.file,
                                            table.name,
                                            limitby = (0, 1),
                                            ).first()
        if not template:
            return None, None

        # Get the path to the file
        path = table.file.uploadfolder
        if path:
            path = os.path.join(path, template.file)
        else:
            path = os.path.join(r.folder, "uploads", template.file)

        return template.name, path

    # -------------------------------------------------------------------------
    @staticmethod
    def template_query(r):
        """
            Returns a query for doc_document to look up templates

            Args:
                r - the context CRUDRequest; unused in base class, but can
                    be used by subclasses for context-dependent lookups

            Returns:
                Query
        """

        auth = current.auth
        if auth.user:
            user_organisation = auth.user.organisation_id
        else:
            return None

        s3db = current.s3db
        root_org = s3db.org_root_organisation(user_organisation)

        table = s3db.doc_document
        query = (table.organisation_id == root_org) & \
                (table.is_template == True) & \
                (table.deleted == False)

        return query

# END =========================================================================
