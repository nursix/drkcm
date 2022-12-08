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
ttable = s3db.org_organisation_type
rtable = s3db.org_requirements
gtable = s3db.org_group
mtable = s3db.org_group_membership
ltable = s3db.org_organisation_organisation_type
vtable = s3db.org_verification

# Paths
#IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
#TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "RLPPTM")

# -----------------------------------------------------------------------------
# Fix end date of commissions
#
if not failed:
    info("Reset organisation type verification status...")

    types = {"Zahnarztpraxis",
             "Arztpraxis (Vertragsarztpraxis)",
             "Apotheke",
             "Hilfsorganisation",
             }

    # Require type in types list and type verification required
    join = rtable.on((rtable.organisation_type_id == ttable.id) & \
                     (rtable.verifreq == True) & \
                     (rtable.deleted == False))
    query = (ttable.name.belongs(types)) & \
            (ttable.deleted == False)
    type_set = db(query)._select(ttable.id, join=join)

    # Require organisation in TESTSTATIONS group
    join = [mtable.on((mtable.organisation_id == vtable.organisation_id) & \
                      (mtable.deleted == False)),
            gtable.on((gtable.id == mtable.group_id) & \
                      (gtable.name == TESTSTATIONS) & \
                      (gtable.deleted == False)),
            ltable.on((ltable.organisation_id == vtable.organisation_id) & \
                      (ltable.organisation_type_id.belongs(type_set)) & \
                      (ltable.deleted == False)),
            ]

    # Require type verification complete (or accepted)
    query = (vtable.orgtype.belongs(("VERIFIED", "ACCEPT"))) & \
            (vtable.deleted == False)
    vset = db(query)._select(vtable.id, join=join)

    # Reset type verification and status to REVIEW
    # - but do not suspend commissions just for that
    updated = db(vtable.id.belongs(vset)).update(orgtype = "REVIEW",
                                                 status = "REVIEW",
                                                 modified_on = vtable.modified_on,
                                                 modified_by = vtable.modified_by,
                                                 )

    infoln("...done (%s records updated)" % updated)

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
