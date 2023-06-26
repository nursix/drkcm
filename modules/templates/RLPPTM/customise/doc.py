"""
    DOC module customisations for RLPPTM

    License: MIT
"""

from gluon import current, IS_IN_SET

from core import get_form_record_id

from ..helpers import WorkflowOptions

# -------------------------------------------------------------------------
# Status for uploaded documents
DOC_STATUS = WorkflowOptions(("NEW", "New", "lightblue"),
                             ("EVIDENCE", "Audit Evidence", "red"),
                             ("RELEASED", "Released", "lightgreen"),
                             selectable = ("NEW", "EVIDENCE", "RELEASED"),
                             represent = "status",
                             none = "NEW",
                             )

# -------------------------------------------------------------------------
def doc_document_resource(r, tablename):

    T = current.T

    s3db = current.s3db
    table = s3db.doc_document

    # Custom label for date-field, default not writable
    field = table.date
    field.label = T("Uploaded on")
    field.writable = False

    # Hide URL field
    field = table.url
    field.readable = field.writable = False

    # Custom label for name-field
    field = table.name
    field.label = T("Title")

    # Set default organisation_id
    doc_set_default_organisation(r)

    # Add custom onaccept
    s3db.add_custom_callback("doc_document", "onaccept", doc_document_onaccept)
    s3db.add_custom_callback("doc_document", "ondelete", doc_document_ondelete)

    if r.controller == "org" or r.function == "organisation":

        # Configure status-field
        field = table.status
        if current.auth.s3_has_role("AUDITOR"):
            field.readable = field.writable = True
            field.requires = IS_IN_SET(DOC_STATUS.selectable(True),
                                       zero = None,
                                       sort = False,
                                       )
            field.represent = DOC_STATUS.represent
            status = "status"
        else:
            field.readable = field.writable = False
            status = None

        # List fields
        list_fields = ["name",
                       "file",
                       "date",
                       status,
                       "comments",
                       ]
        s3db.configure("doc_document",
                       list_fields = list_fields,
                       )

# -------------------------------------------------------------------------
def doc_document_onaccept(form):
    """
        Custom onaccept routine for documents:
            - alter ownership according to status
            - update document availability in the audit status of the org
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    table = current.s3db.doc_document

    row = current.db(table.id == record_id).select(table.id,
                                                   table.organisation_id,
                                                   table.status,
                                                   table.created_by,
                                                   table.owned_by_user,
                                                   limitby = (0, 1),
                                                   ).first()
    if row:
        update = {}
        if row.status == "EVIDENCE":
            AUDITOR = current.auth.get_role_id("AUDITOR")
            update = {"owned_by_user": None,
                      "owned_by_group": AUDITOR,
                      }
        else:
            ORG_ADMIN = current.auth.get_system_roles().ORG_ADMIN
            if not row.owned_by_user:
                update["owned_by_user"] = table.created_by
            update["owned_by_group"] = ORG_ADMIN
        if update:
            row.update_record(**update)

        if row.organisation_id:
            from ..models.org import TestProvider
            TestProvider(row.organisation_id).update_audit()

# -------------------------------------------------------------------------
def doc_document_ondelete(row):
    """
        Custom ondelete routine for documents:
            - update document availability in the audit status of the org
    """

    if row.organisation_id:
        from ..models.org import TestProvider
        TestProvider(row.organisation_id).update_audit()

# -------------------------------------------------------------------------
def doc_set_default_organisation(r):
    """
        Sets the correct default organisation_id for doc_document from
        the upload context (e.g. organisation or facility)

        Args:
            r - the current CRUDRequest
    """

    table = current.s3db.doc_document

    organisation_id = None

    if r.controller == "org":

        record = r.record
        if record:
            if r.function == "organisation":
                organisation_id = record.id
            elif r.function == "facility":
                organisation_id = record.organisation_id

    if organisation_id:
        table.organisation_id.default = organisation_id

# END =========================================================================
