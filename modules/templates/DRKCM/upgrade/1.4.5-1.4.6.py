# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.4.5 => 1.4.6
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.4.5-1.4.6.py
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
atable = s3db.dvr_response_action
ttable = s3db.dvr_response_type

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Remove existing dvr_response_action.response_type_id
#
if not failed:
    info("Unlink existing response type references")

    query = (atable.response_type_id != None)
    try:
        result = db(query).update(response_type_id = None)
    except Exception as e:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        infoln("...done (%s records updated)" % result)

# -----------------------------------------------------------------------------
# Remove existing dvr_response_type records
#
if not failed:
    info("Remove existing response types")

    query = (ttable.id > 0)
    try:
        result = db(query).delete()
    except Exception as e:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        infoln("...done (%s records deleted)" % result)

# -----------------------------------------------------------------------------
# Import new dvr_response_types
#
if not failed:
    info("Import new response types")

    # File and Stylesheet Paths
    stylesheet = os.path.join(IMPORT_XSLT_FOLDER, "dvr", "response_type.xsl")
    filename = os.path.join(TEMPLATE_FOLDER, "dvr_response_type.csv")

    # Import, fail on any errors
    try:
        with open(filename, "r") as File:
            resource = s3db.resource("dvr_response_type")
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
