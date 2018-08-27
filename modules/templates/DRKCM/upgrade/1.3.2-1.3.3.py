# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.3.2 => 1.3.3
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.3.2-1.3.3.py
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
#ctable = s3db.dvr_case
rtable = s3db.auth_group
ltable = s3db.pr_person_user
htable = s3db.hrm_human_resource
ptable = s3db.pr_person

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
# For all users with an HR record, assign the STAFF role
#
if not failed:

    info("Assign staff role to HRs")

    # Get all users with an HR record
    join = [ptable.on((ptable.pe_id == ltable.pe_id) & \
                      (ptable.deleted == False)),
            htable.on((htable.person_id == ptable.id) & \
                      (htable.deleted == False))
            ]
    query = (ltable.deleted == False)
    rows = db(query).select(ltable.user_id,
                            join = join,
                            )
    updated = 0
    if rows:
        # Get the role ID of STAFF
        query = (rtable.uuid == "STAFF")
        staff_role = db(query).select(rtable.id, limitby = (0, 1)).first()
        if staff_role:
            info(".")
            staff_role = staff_role.id
            assign_role = auth.s3_assign_role
            for row in rows:
                info("+")
                try:
                    assign_role(row.user_id, staff_role)
                    updated += 1
                except Exception, e:
                    infoln("...failed")
                    infoln(sys.exc_info()[1])
                    failed = True
                    break
    if not failed:
        infoln("...done (%s users assigned)" % updated)

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
