"""
    DOC module customisations for RLPPTM

    License: MIT
"""

from gluon import current

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

    if r.controller == "org" or r.function == "organisation":

        # List fields
        list_fields = ["name",
                       "file",
                       "date",
                       "comments",
                       ]
        s3db.configure("doc_document",
                       list_fields = list_fields,
                       )

# END =========================================================================
