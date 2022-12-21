# Database upgrade script
#
# RLPPTM Template Version 1.19.4
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/RLPPTM/upgrade/reinstcom.py
#
import sys

#from core import S3Duplicate
from templates.RLPPTM.config import TESTSTATIONS
from templates.RLPPTM.models.org import TestProvider, TestStation

# Override auth (disables all permission checks)
auth.override = True

# Initialize failed-flag
failed = False

# Info
def info(msg):
    sys.stderr.write("%s" % msg)
    sys.stderr.flush()
def infoln(msg):
    sys.stderr.write("%s\n" % msg)
    sys.stderr.flush()

# Load models for tables
otable = s3db.org_organisation
ttable = s3db.org_organisation_type
ltable = s3db.org_organisation_organisation_type
gtable = s3db.org_group
mtable = s3db.org_group_membership
vtable = s3db.org_verification
ctable = s3db.org_commission
dtable = s3db.doc_document

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
    info("Reinstate commissions suspended for organisation type verification...")

    today = datetime.datetime.utcnow().date()

    # Require organisation is linked to certain types, linked to the test
    # stations group, and their verification is under review only for the
    # organisation type verification
    join = [otable.on(otable.id == ctable.organisation_id),
            ltable.on((ltable.organisation_id == otable.id) & \
                      (ltable.deleted == False)),
            #ttable.on((ttable.id == ltable.organisation_type_id) & \
            #          (ttable.name.belongs(IREQ | DREQ))),
            mtable.on((mtable.organisation_id == otable.id) & \
                      (mtable.deleted == False)),
            gtable.on((gtable.id == mtable.group_id) & \
                      (gtable.name == TESTSTATIONS) & \
                      (gtable.deleted == False)),
            vtable.on((vtable.organisation_id == otable.id) & \
                      (vtable.status == "REVIEW") & \
                      (vtable.orgtype == "REVIEW") & \
                      (vtable.mgrinfo.belongs(("VERIFIED", "ACCEPT"))) & \
                      (vtable.mpav.belongs(("VERIFIED", "ACCEPT"))) & \
                      (vtable.deleted == False)),
            ]

    # Require commission is current and suspended for pending verification
    query = (ctable.status == "SUSPENDED") & \
            (ctable.status_reason == "N/V") & \
            ((ctable.date == None) | (ctable.date <= today)) & \
            ((ctable.end_date == None) | (ctable.end_date >= today)) & \
            (ctable.deleted == False)

    review = set()

    # For organisation types requiring ID numbers in the comments fields
    # - check the comments field is not empty
    cjoin = [ttable.on((ttable.id == ltable.organisation_type_id) & \
                       (ttable.name.belongs(IREQ))),
             ]
    rows = db(query).select(ctable.id,
                            otable.comments,
                            join = join + cjoin,
                            )
    for row in rows:
        if row.org_organisation.comments:
            review.add(row.org_commission.id)
            info(".")
        else:
            info("-")

    # For organisation types requiring documents to be uploaded
    # - check that documents have been uploaded
    cjoin = [ttable.on((ttable.id == ltable.organisation_type_id) & \
                       (ttable.name.belongs(DREQ))),
             ]
    left = dtable.on((dtable.organisation_id == otable.id) & \
                     (dtable.deleted == False))
    rows = db(query).select(ctable.id,
                            dtable.id,
                            join = join + cjoin,
                            left = left,
                            )
    for row in rows:
        if row.doc_document.id:
            review.add(row.org_commission.id)
            info(".")
        else:
            info("-")

    # Reinstate commissions temporarily
    query = ctable.id.belongs(review)
    updated = db(query).update(status = "CURRENT",
                               status_reason = None,
                               status_date = None,
                               modified_on = ctable.modified_on,
                               modified_by = ctable.modified_by,
                               )

    # Reinstate all test stations belonging to these orgs
    # that had been unlisted due to the suspended commission
    rows = db(query).select(ctable.organisation_id)
    for row in rows:
        TestStation.update_all(row.organisation_id,
                               public = "Y",
                               reason = ("SUSPENDED", "COMMISSION"),
                               )

    infoln("...done (%s commissions reinstated)" % updated)

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
