# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.3.4 => 1.3.5
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.3.4-1.3.5.py
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
ltable = s3db.dvr_response_action_theme
catable = s3db.dvr_case_activity

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
if not failed:
    info("Re-link response actions to persons")


    left = catable.on(catable.id == atable.case_activity_id)
    query = (atable.person_id == None) & \
            (atable.deleted == False)
    rows = db(query).select(atable.id,
                            catable.person_id,
                            left = left,
                            )
    updated = 0
    for row in rows:
        info(".")
        action_id = row.dvr_response_action.id
        person_id = row.dvr_case_activity.person_id

        if person_id:
            success = db(atable.id == action_id).update(person_id = person_id)
            if success:
                updated += 1
            else:
                infoln("...failed (for record #%s)" % action_id)
                failed = True
                break
    if not failed:
        infoln("...done (%s records updated)" % updated)

# -----------------------------------------------------------------------------
if not failed:
    info("Install response action theme links")

    # Get all response actions
    query = (atable.deleted == False)
    rows = db(query).select(atable.id,
                            atable.response_theme_ids,
                            )

    # Get all selected themes
    selected = set()
    for row in rows:
        response_theme_ids = row.response_theme_ids
        if response_theme_ids:
            action_id = row.id
            for theme_id in response_theme_ids:
                selected.add((action_id, theme_id))

    # Get all existing theme links
    query = (ltable.deleted == False)
    links = db(query).select(ltable.action_id,
                             ltable.theme_id,
                             )
    linked = set((link.action_id, link.theme_id) for link in links)

    # Remove obsolete links
    removed = 0
    obsolete = linked - selected
    for action_id, theme_id in obsolete:
        query = (ltable.action_id == action_id) & \
                (ltable.theme_id == theme_id) & \
                (ltable.deleted == False)
        success = db(query).delete()
        if success:
            removed += 1
        else:
            infoln("...failed")
            failed = True
            break

    # Add missing links
    if not failed:
        new = 0
        added = selected - linked
        for action_id, theme_id in added:
            link_id = ltable.insert(action_id = action_id,
                                    theme_id = theme_id,
                                    )
            if link_id:
                new += 1;
            else:
                infoln("...failed")
                failed = True
                break
    if not failed:
        infoln("...done (%s links removed, %s links added)" % (removed, new))

# -----------------------------------------------------------------------------
if not failed:
    info("Upgrade response statuses")

    # File and Stylesheet Paths
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
