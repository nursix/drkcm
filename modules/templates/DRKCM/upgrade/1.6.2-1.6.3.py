# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.6.2 => 1.6.3
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.6.2-1.6.3.py
#
import datetime
import sys
#from s3 import S3DateTime

#from gluon.storage import Storage
#from gluon.tools import callback

# Override auth (disables all permission checks)
auth.override = True

# Failed-flag
failed = False

# Info
def info(msg):
    sys.stderr.write("%s" % msg)
def infoln(msg):
    sys.stderr.write("%s\n" % msg)

# Load models for tables
otable = s3db.org_organisation
stable = s3db.cr_shelter
cstable = s3db.dvr_case_status
ctable = s3db.dvr_case

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Link LEA cases to LEA site
#
if not failed:
    info("Link LEA cases to LEA site")

    # Find LEA Organisation
    query = (otable.name == "LEA Ellwangen") & \
            (otable.deleted == False)
    row = db(query).select(otable.id, limitby = (0, 1)).first()
    if row:
        organisation_id = row.id
    else:
        failed = True
        infoln("...failed (Organisation not found)")

if not failed:
    query = (stable.organisation_id == organisation_id) & \
            (stable.name == "Landeserstaufnahmeeinrichtung Ellwangen (LEA)") & \
            (stable.deleted == False)
    row = db(query).select(stable.site_id, limitby = (0, 1)).first()
    if row:
        site_id = row.site_id
    else:
        failed = True
        infoln("...failed (Site not found)")

if not failed:
    query = (cstable.is_closed == False) & \
            (cstable.deleted == False)
    rows = db(query).select(cstable.id)
    status_ids = {row.id for row in rows}
    if not status_ids:
        infoln("...failed (No open case statuses found)")
        failed = True

if not failed:
    query = (ctable.organisation_id == organisation_id) & \
            (ctable.site_id == None) & \
            (ctable.status_id.belongs(status_ids)) & \
            (ctable.archived == False) & \
            (ctable.deleted == False)
    cases = db(query).select(ctable.id)
    updated = 0
    for case in cases:
        info(".")
        case.update_record(site_id = site_id,
                           modified_on = ctable.modified_on,
                           modified_by = ctable.modified_by,
                           )
        try:
            s3db.onaccept(ctable, case, method="update")
        except:
            failed = True
            infoln("...failed (Exception in onaccept)")
            infoln(sys.exc_info()[1])
            break
        else:
            updated += 1
    if not failed:
        infoln("...done (%s cases updated)" % updated)

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
