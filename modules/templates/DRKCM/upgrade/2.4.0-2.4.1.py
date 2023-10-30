# Database upgrade script
#
# DRKCM Template Version 2.4.0 => 2.4.1
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/2.4.0-2.4.1.py
#
import sys

#from core import S3Duplicate

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
rtable = s3db.s3_permission

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Deploy vulnerability types
#
if not failed:
    info("Deploy vulnerability types")

    # File and Stylesheet Paths
    stylesheet = os.path.join(IMPORT_XSLT_FOLDER, "dvr", "vulnerability_type.xsl")
    filename = os.path.join(TEMPLATE_FOLDER, "dvr_vulnerability_type.csv")

    # Import, fail on any errors
    try:
        with open(filename, "r") as File:
            resource = s3db.resource("dvr_vulnerability_type")
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
# Upgrade user roles
#
if not failed:
    info("Upgrade user roles")

    bi = s3base.BulkImporter()
    filename = os.path.join(TEMPLATE_FOLDER, "auth_roles.csv")

    try:
        error = bi.import_roles(filename)
    except Exception as e:
        error = sys.exc_info()[1] or "unknown error"
    if error:
        infoln("...failed")
        infoln(error)
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
