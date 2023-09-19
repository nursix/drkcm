"""
    DOC module customisations for MRCMS

    License: MIT
"""

from gluon import current, IS_NOT_EMPTY

from core import represent_file

# -------------------------------------------------------------------------
def doc_image_resource(r, tablename):

    T = current.T

    s3db = current.s3db
    table = s3db.doc_image

    # Disable author-field
    field = table.person_id
    field.readable = field.writable = False

    # Hide URL field
    field = table.url
    field.readable = field.writable = False

    # Custom label for name-field, make mandatory
    field = table.name
    field.label = T("Title")
    field.requires = [IS_NOT_EMPTY(), field.requires]

    # Set default organisation_id
    doc_set_default_organisation(r, table=table)

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

    # Custom label for name-field, make mandatory
    field = table.name
    field.label = T("Title")
    field.requires = [IS_NOT_EMPTY(), field.requires]

    # Represent as symbol+size rather than file name
    field = table.file
    field.represent = represent_file()

    # Set default organisation_id
    doc_set_default_organisation(r, table=table)

    # List fields
    list_fields = ["name",
                   "file",
                   "date",
                   "comments",
                   ]
    s3db.configure("doc_document",
                   list_fields = list_fields,
                   )

# -------------------------------------------------------------------------
def doc_set_default_organisation(r, table=None):
    """
        Sets the correct default organisation_id for documents/images from
        the upload context (e.g. organisation, shelter)

        Args:
            r - the current CRUDRequest
    """

    if table is None:
        table = current.s3db.doc_document

    organisation_id = None

    record = r.record
    if record:
        fields = {"org_organisation": "id",
                  "cr_shelter": "organisation_id",
                  }
        fieldname = fields.get(r.resource.tablename)
        if fieldname:
            organisation_id = record[fieldname]

    if organisation_id:
        table.organisation_id.default = organisation_id

# END =========================================================================
