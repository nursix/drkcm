# Script to import BSNR
#
# RLPPTM Template Version 1.21.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/RLPPTM/tools/import_bsnr.py
#
import json
import sys

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
utable = s3db.auth_user
otable = s3db.org_organisation
ptable = s3db.project_project
ltable = s3db.project_organisation

# File to import
filename = "bsnr.xlsx"

# -----------------------------------------------------------------------------
def import_prep(tree):
    """
        Resolve organisation_id references OrgId=>UUID
    """

    db = current.db
    s3db = current.s3db

    otable = s3db.org_organisation
    ttable = s3db.org_organisation_tag

    join = otable.on(otable.id == ttable.organisation_id)
    query = (ttable.tag == "OrgID") & (ttable.deleted == False)

    elements = tree.getroot().xpath("/s3xml//resource[@name='org_bsnr']/reference[@field='organisation_id']")
    looked_up = {}
    for element in elements:

        org_id = element.get("org_id")
        if not org_id:
            continue

        if org_id in looked_up:
            uid = looked_up[org_id]
        else:
            record = db((ttable.value == org_id) & query).select(otable.uuid,
                                                                join = join,
                                                                limitby = (0, 1),
                                                                ).first()
            uid = record.uuid if record else None

        looked_up[org_id] = uid
        if uid:
            element.set("uuid", uid)

# -----------------------------------------------------------------------------
# Import BSNR
#
if not failed:
    info("Import BSNR...")

    folder = current.request.folder
    template = os.path.join(folder, "modules", "templates", "RLPPTM")

    current.response.s3.import_prep=import_prep
    stylesheet = os.path.join(template, "formats", "import", "org_bsnr.xsl")

    try:
        with open(filename, "rb") as source:
            resource = s3db.resource("org_bsnr")
            result = resource.import_xml(source,
                                         source_type = "xlsx",
                                         stylesheet = stylesheet,
                                         commit = True,
                                         ignore_errors = True,
                                         )
    except IOError:
        failed = True
        infoln("...failed (cannot read file %s)" % filename)
    else:
        infoln("...done (%s created, %s updated, %s errors)" % \
                    (len(result.created),
                     len(result.updated),
                     result.failed,
                     ),
               )

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")

# END =========================================================================
