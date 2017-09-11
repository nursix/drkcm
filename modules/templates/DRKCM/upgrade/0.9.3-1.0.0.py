# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 0.9.3 => 1.0.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/0.9.3-1.0.0.py
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
    print >> sys.stderr, msg,
def infoln(msg):
    print >> sys.stderr, msg

# Load models for tables
#ctable = s3db.dvr_case

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Set up org_site_check scheduler task
#
if not failed:
    info("Set up org_site_check task")

    tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    try:
        s3task.schedule_task("org_site_check", ["all"], {},
                             function_name = "org_site_check",
                             start_time = tomorrow.replace(hour = 2,
                                                           minute = 0,
                                                           second = 0,
                                                           microsecond = 0,
                                                           ),
                             period = 86400,
                             repeats = 0,
                             timeout = 600,
                             )
    except Exception, e:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        infoln("...done")

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    print >> sys.stderr, "UPGRADE FAILED - Action rolled back."
else:
    db.commit()
    print >> sys.stderr, "UPGRADE SUCCESSFUL."
