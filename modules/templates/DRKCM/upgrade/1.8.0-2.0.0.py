# Database upgrade script
#
# DRKCM Template Version 1.8.0 => 2.0.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.8.0-2.0.0.py
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
# Update organisation_id for all case documents
#
if not failed:
    info("Fix document owner organisations")

    dtable = s3db.doc_document
    ctable = s3db.dvr_case
    atable = s3db.dvr_case_activity
    gtable = s3db.pr_group

    left = [ctable.on(ctable.doc_id == dtable.doc_id),
            atable.on(atable.doc_id == dtable.doc_id),
            gtable.on(gtable.doc_id == dtable.doc_id),
            ]
    query = ((ctable.id != None) | (atable.id != None) | (gtable.id != None)) & \
            (dtable.deleted == False)
    rows = db(query).select(dtable.id,
                            dtable.organisation_id,
                            ctable.id,
                            ctable.organisation_id,
                            atable.id,
                            gtable.id,
                            left = left,
                            )
    updated = 0
    info("...")
    for row in rows:
        document = row.doc_document
        case = row.dvr_case
        activity = row.dvr_case_activity
        group = row.pr_group

        if case.id:
            pass
        elif activity.id:
            query = (atable.id == activity.id) & \
                    (ctable.person_id == atable.person_id) & \
                    (ctable.deleted == False)
            case = db(query).select(ctable.id,
                                    ctable.organisation_id,
                                    orderby = ctable.created_on,
                                    limitby = (0, 1),
                                    ).first()
        elif group.id:
            mtable = s3db.pr_group_membership
            query = (mtable.group_id == gtable.id) & \
                    (mtable.deleted == False) & \
                    (ctable.person_id == mtable.person_id) & \
                    (ctable.deleted == False)
            case = db(query).select(ctable.id,
                                    ctable.organisation_id,
                                    orderby = ctable.created_on,
                                    limitby = (0, 1),
                                    ).first()
        if case and case.organisation_id and \
           document.organisation_id != case.organisation_id:
            document.update_record(organisation_id = case.organisation_id)
            updated += 1
            info("+")
        else:
            info(".")

    infoln("...done (%s records updated)" % updated)

# -----------------------------------------------------------------------------
# Update realm_entity for all documents
#
if not failed:
    info("Update realm entity for all documents")

    auth.set_realm_entity(dtable, dtable.deleted==False, force_update=True)
    infoln("...done")

# -----------------------------------------------------------------------------
# Establish indexes for permission table
#
if not failed:
    info("Add indexes to permissions table")

    auth.permission.create_indexes()
    infoln("...done")

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
        except Exception as e:
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
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
