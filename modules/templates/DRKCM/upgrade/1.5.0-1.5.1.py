# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.5.0 => 1.5.1
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.5.0-1.5.1.py
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

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Upgrade user roles
#
if not failed:
    info("Upgrade user roles")

    bi = s3base.S3BulkImporter()
    filename = os.path.join(TEMPLATE_FOLDER, "auth_roles.csv")

    with open(filename, "r") as File:
        try:
            bi.import_role(filename)
        except Exception, e:
            infoln("...failed")
            infoln(sys.exc_info()[1])
            failed = True
        else:
            infoln("...done")

# -----------------------------------------------------------------------------
# Upgrade response statuses
#
if not failed:
    info("Upgrade response statuses")

    # Rename response status "Waiting" into "On Hold"
    query = (stable.name == "Waiting")
    db(query).update(name="On Hold")

    # Import new statuses
    stylesheet = os.path.join(IMPORT_XSLT_FOLDER, "dvr", "response_status.xsl")
    filename = os.path.join(TEMPLATE_FOLDER, "dvr_response_status.csv")

    # Import, fail on any errors
    try:
        with open(filename, "r") as File:
            resource = s3db.resource("dvr_response_status")
            resource.import_xml(File, format="csv", stylesheet=stylesheet)
    except:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        if resource.error:
            infoln("...failed")
            infoln(resource.error)
            failed = True
        else:
            infoln("...done")

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
