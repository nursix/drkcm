# Database upgrade script
#
# DRKCM Template Version 2.1.1 => 2.2.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/2.1.1-2.2.0.py
#
import sys

#from gluon.storage import Storage
#from gluon.tools import callback
#from core import S3Duplicate

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
#ftable = s3db.org_facility

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Deploy new note types
#
if not failed:
    info("Deploy new note types")

    # File and Stylesheet Paths
    stylesheet = os.path.join(IMPORT_XSLT_FOLDER, "dvr", "note_type.xsl")
    filename = os.path.join(TEMPLATE_FOLDER, "dvr_note_type.csv")

    # Import, fail on any errors
    try:
        with open(filename, "r") as File:
            resource = s3db.resource("dvr_note_type")
            result = resource.import_xml(File,
                                         source_type = "csv",
                                         stylesheet = stylesheet,
                                         )
    except:
        infoln("...failed")
        infoln(sys.exc_info()[1])
        failed = True
    else:
        if result.error:
            infoln("...failed")
            infoln(result.error)
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
