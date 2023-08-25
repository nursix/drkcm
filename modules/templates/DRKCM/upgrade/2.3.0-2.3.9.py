# Database upgrade script
#
# DRKCM Template Version 2.3.0 => 2.3.9
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/2.3.0-2.3.9.py
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
# Update permission rules for dvr_vulnerability_type
#
if not failed:
    info("Update permission rules")

    updated = 0

    # Update page rules
    cquery = (rtable.controller == "dvr")
    query = cquery & (rtable.function == "vulnerability_type")
    updated += db(query).update(function="diagnosis",
                                modified_on = rtable.modified_on,
                                )
    query = cquery & (rtable.function == "vulnerability_type_case_activity")
    updated += db(query).update(function="diagnosis_suspected",
                                modified_on = rtable.modified_on,
                                )
    query = cquery & (rtable.function == "diagnosis_case_activity")
    updated += db(query).update(function="diagnosis_confirmed",
                                modified_on = rtable.modified_on,
                                )

    # Update table rules
    query = (rtable.tablename == "dvr_vulnerability_type")
    updated += db(query).update(tablename="dvr_diagnosis",
                                modified_on = rtable.modified_on,
                                )
    query = (rtable.tablename == "dvr_vulnerability_type_case_activity")
    updated += db(query).update(tablename="dvr_diagnosis_suspected",
                                modified_on = rtable.modified_on,
                                )
    query = (rtable.tablename == "dvr_diagnosis_case_activity")
    updated += db(query).update(tablename="dvr_diagnosis_confirmed",
                                modified_on = rtable.modified_on,
                                )

    infoln("...done (%s rules updated)" % updated)

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
