# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.4.6 => 1.4.7
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.4.6-1.4.7.py
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
ctable = s3db.dvr_case
atable = s3db.dvr_response_action

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Use due-dates of responses as date
# Limit to cases of DRK-MA
#
if not failed:
    info("Set response dates from due-dates")

    # Get the organisation_id of the DRK-MA root organisation
    root_organisation_id = None
    query = (otable.name == "DRK Kreisverband Mannheim") & \
            (otable.deleted == False)
    row = db(query).select(otable.id, limitby=(0, 1)).first()
    if not row:
        infoln("...failed")
        infoln("Organisation 'DRK Kreisverband Mannheim' not found")
        failed = True
    else:
        root_organisation_id = row.id

if not failed:
    # Get the organisation_ids of all branches of DRK-MA
    query = (otable.root_organisation == root_organisation_id) & \
            (otable.deleted == False)
    rows = db(query).select(otable.id)
    organisation_ids = {row.id for row in rows}

    # Get the person_ids of all cases of DRK-MA
    query = (ctable.organisation_id.belongs(organisation_ids)) & \
            (ctable.deleted == False)
    rows = db(query).select(ctable.person_id)
    person_ids = {row.person_id for row in rows}

    # Set date to date_due where date is None
    query = (atable.date == None) & \
            (atable.deleted == False)
    try:
        result = db(query).update(date = atable.date_due,
                                  modified_on = atable.modified_on,
                                  modified_by = atable.modified_by,
                                  )
    except Exception as e:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        infoln("...done (%s records updated)" % result)

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
