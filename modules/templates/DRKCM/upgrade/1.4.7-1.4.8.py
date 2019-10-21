# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.4.7 => 1.4.8
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.4.7-1.4.8.py
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
stable = s3db.dvr_response_status
atable = s3db.dvr_response_action

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Set start_date/end_date for response actions
#
if not failed:
    info("Set response start/end dates")

    query = (stable.is_closed == True) & \
            (stable.deleted == False)
    rows = db(query).select(stable.id)
    closed = {row.id for row in rows}

    # Get all responses with a date but no start_date
    query = (atable.date != None) & \
            (atable.start_date == None) & \
            (atable.deleted == False)
    rows = db(query).select(atable.id,
                            atable.created_on,
                            atable.date,
                            atable.hours,
                            atable.start_date,
                            atable.end_date,
                            atable.status_id,
                            )
    updated = 0
    for row in rows:

        hours = row.hours
        if hours:
            interval = (hours * 60 // 5) * 5
        else:
            interval = 30

        start_date = datetime.datetime.combine(row.date, row.created_on.time())
        if row.status_id in closed:
            start_date = start_date - datetime.timedelta(minutes = interval - 5)
        start_date = start_date.replace(minute = start_date.minute // 5 * 5,
                                        second = 0,
                                        microsecond = 0,
                                        )
        end_date = start_date + datetime.timedelta(minutes = interval) \
                              - datetime.timedelta(seconds = 1)

        try:
            result = row.update_record(start_date = start_date,
                                       end_date = end_date,
                                       )
        except Exception as e:
            infoln("...failed")
            infoln(sys.exc_info()[1])
            failed = True
            break
        else:
            if not result:
                infoln("...failed")
                failed = True
                break
            else:
                info(".")
                updated += 1

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
