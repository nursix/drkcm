# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.7.1 => 1.8.0
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.7.1-1.8.0.py
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
dtable = s3db.doc_document

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Make sure all documents have a name
#
if not failed:
    info("Add missing document titles")

    query = ((dtable.name == None) | (dtable.name == "")) & \
            (dtable.file != None) & \
            (dtable.deleted == False)
    rows = db(query).select(dtable.id,
                            dtable.file,
                            )
    updated = 0
    for row in rows:
        # Use the original file name as title
        prop = dtable.file.retrieve_file_properties(row.file)
        name = prop.get("filename")
        if name:
            row.update_record(name = name,
                              modified_by = dtable.modified_by,
                              modified_on = dtable.modified_on,
                              )
            updated += 1
    infoln("...done (%s documents updated)" % updated)

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
