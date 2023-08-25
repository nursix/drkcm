# Database upgrade script
#
# RLPPTM Template Version 1.19.4
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/RLPPTM/upgrade/reset_orgtype.py
#
import sys

#from core import S3Duplicate
from templates.RLPPTM.config import TESTSTATIONS
from templates.RLPPTM.models.org import TestProvider

# Override auth (disables all permission checks)
auth.override = True

# Initialize failed-flag
failed = False

# Info
def info(msg):
    sys.stderr.write("%s" % msg)
def infoln(msg):
    sys.stderr.write("%s\n" % msg)

# Load models for tables
otable = s3db.org_organisation
ttable = s3db.org_organisation_type
rtable = s3db.org_requirements
gtable = s3db.org_group
mtable = s3db.org_group_membership
ltable = s3db.org_organisation_organisation_type
vtable = s3db.org_verification
dtable = s3db.doc_document

# Paths
#IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
#TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "RLPPTM")

# Verification requirements
# - make sure type names match database exactly

# Types for which document upload is required
DREQ = {"Apotheke",
        "Hilfsorganisation",
        "Hilfsorganisation (kommunal beauftragt)",
        "Rotes Kreuz",
        }
# Types for which IDs (e.g. BSNr) must be provided in the comments-field
# of the organisation record
IREQ = {"Arztpraxis (Vertragsarztpraxis)",
        "Zahnarztpraxis",
        }

# -----------------------------------------------------------------------------
# Fix end date of commissions
#
if not failed:
    info("Reset organisation type verification status...")

    # Types for which organisation type verification is required
    types = DREQ | IREQ

    # Make sure types have the verifreq flag set
    join = ttable.on((ttable.id == rtable.organisation_type_id) & \
                     (ttable.name.belongs(types)) & \
                     (ttable.deleted == False))
    query = (rtable.deleted == False)
    requirements = db(query)._select(rtable.id, join=join)
    db(rtable.id.belongs(requirements)).update(verifreq = True,
                                               modified_on = rtable.modified_on,
                                               modified_by = rtable.modified_by,
                                               )

    # Require type in types list
    query = (ttable.name.belongs(types)) & \
            (ttable.deleted == False)
    type_set = db(query)._select(ttable.id)

    # Require organisation in TESTSTATIONS group
    join = [mtable.on((mtable.organisation_id == vtable.organisation_id) & \
                      (mtable.deleted == False)),
            gtable.on((gtable.id == mtable.group_id) & \
                      (gtable.name == TESTSTATIONS) & \
                      (gtable.deleted == False)),
            #ltable.on((ltable.organisation_id == vtable.organisation_id) & \
                      #(ltable.organisation_type_id.belongs(type_set)) & \
                      #(ltable.deleted == False)),
            ]

    # Type subsets
    query = (ttable.name.belongs(DREQ)) & (ttable.deleted == False)
    dreq = db(query)._select(ttable.id)
    query = (ttable.name.belongs(IREQ)) & (ttable.deleted == False)
    ireq = db(query)._select(ttable.id)

    # Look for verified or accepted type verifications
    query = (vtable.orgtype.belongs(("VERIFIED", "ACCEPT"))) & \
            (vtable.deleted == False)

    review, revise = set(), set()

    # Check organisations for which documents are required
    tjoin = [ltable.on((ltable.organisation_id == vtable.organisation_id) & \
                       (ltable.organisation_type_id.belongs(dreq)) & \
                       (ltable.deleted == False)),
             ]
    left = dtable.on((dtable.organisation_id == vtable.organisation_id) & \
                     (dtable.deleted == False))
    rows = db(query).select(vtable.id,
                            dtable.id,
                            join = join + tjoin,
                            left = left
                            )
    for row in rows:
        if row.doc_document.id:
            review.add(row.org_verification.id)
        else:
            revise.add(row.org_verification.id)

    # Check organisations for which identification is required
    tjoin = [ltable.on((ltable.organisation_id == vtable.organisation_id) & \
                       (ltable.organisation_type_id.belongs(ireq)) & \
                       (ltable.deleted == False)),
             otable.on((otable.id == vtable.organisation_id) & \
                       (otable.deleted == False)),
             ]
    rows = db(query).select(vtable.id,
                            otable.comments,
                            join = join + tjoin,
                            )
    for row in rows:
        if row.org_organisation.comments:
            review.add(row.org_verification.id)
        else:
            revise.add(row.org_verification.id)

    # For verifications to review:
    # - reset type verification and overall status to REVIEW
    # - but do not suspend commissions
    updated = db(vtable.id.belongs(review)).update(orgtype = "REVIEW",
                                                   status = "REVIEW",
                                                   modified_on = vtable.modified_on,
                                                   modified_by = vtable.modified_by,
                                                   )

    # For verifications to revise:
    # - set type verification to REVISE
    # - suspend commissions normally
    suspended = 0
    revise -= review
    rows = db(vtable.id.belongs(revise)).select(vtable.id,
                                                vtable.organisation_id,
                                                )
    for row in rows:
        row.update_record(orgtype="REVISE",
                          modified_on = vtable.modified_on,
                          modified_by = vtable.modified_by,
                          )
        TestProvider(row.organisation_id).update_verification()
        suspended += 1

    infoln("...done (%s records set for review, %s commissions suspended)" % (updated, suspended))

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")

# END =========================================================================
