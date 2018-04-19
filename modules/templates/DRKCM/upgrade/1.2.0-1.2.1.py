# -*- coding: utf-8 -*-
#
# Database upgrade script
#
# DRKCM Template Version 1.2.0 => 1.2.1
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/1.2.0-1.2.1.py
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
ctable = s3db.dvr_case
otable = s3db.org_organisation
stable = s3db.org_sector
ltable = s3db.org_sector_organisation

IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Install Org<=>Sector links
#
if not failed:
    info("Installing org-sector links")

    # All sector IDs
    query = (stable.deleted == False)
    rows = db(query).select(stable.id)
    sector_ids = set(row.id for row in rows)

    # Add organisation IDs
    query = (otable.deleted == False)
    rows = db(query).select(otable.id)
    organisation_ids = set(row.id for row in rows)

    added = 0
    for organisation_id in organisation_ids:

        # Check if there already are any sector links for this
        # organisation, and if so, then skip this organisation
        query = (ltable.organisation_id == organisation_id) & \
                (ltable.deleted == False)
        row = db(query).select(ltable.id, limitby=(0, 1)).first()
        if row:
            continue

        # Link this organisation to all existing sectors
        for sector_id in sector_ids:
            data = {"organisation_id": organisation_id,
                    "sector_id": sector_id,
                    }
            try:
                link_id = ltable.insert(**data)
            except:
                failed = True
                break
            else:
                if link_id:
                    added += 1
                    data["id"] = link_id
                    auth.s3_set_record_owner(ltable, data)
                else:
                    failed = True
                    break
        if failed:
            break

    if not failed:
        infoln("...done (%s links added)" % added)
    else:
        infoln("...failed")

# -----------------------------------------------------------------------------
# Migrate response actions from types to themes
#
if not failed:
    info("Migrating Response Actions to Themes")

    # Get all organisations (organisation_id, organisation_pe_id)
    query = (otable.deleted == False)
    rows = db(query).select(otable.id,
                            otable.pe_id,
                            )
    orgs = {row.id: row.pe_id for row in rows}

    ttable = s3db.dvr_response_type
    ptable = ttable.with_alias("dvr_parent_response_type")
    thtable = s3db.dvr_response_theme

    # Get all response types and their parent types
    left = ptable.on(ptable.id == ttable.parent)
    query = (ttable.deleted == False)
    rows = db(query).select(ttable.id,
                            ttable.name,
                            ptable.id,
                            ptable.name,
                            left = left,
                            )

    # Extract the leaf types
    parent_types = set(row[ptable.id] for row in rows)
    leaf_types = [(row[ttable.id], row[ttable.name], row[ptable.name])
                  for row in rows if row[ttable.id] not in parent_types]

    # Create themes corresponding to types
    themes = {}
    created = 0
    for type_id, name, parent_name in leaf_types:

        # Check if there is a second occurence of that name
        single = True
        for item in leaf_types:
            if item[0] != type_id and item[1] == name:
                single = False
                break

        # If there are multiple types with this name,
        # then include the parent name in the new name
        if not single:
            new_name = "%s (%s)" % (s3_str(name), s3_str(parent_name))
        else:
            new_name = name

        # Create a theme for each organisation
        for organisation_id in orgs:
            org_pe_id = orgs[organisation_id]
            if org_pe_id not in themes:
                replace = themes[org_pe_id] = {}
            else:
                replace = themes[org_pe_id]

            theme_id = thtable.insert(name = new_name,
                                      organisation_id = organisation_id,
                                      )
            if theme_id:
                created += 1
                auth.s3_set_record_owner(thtable, theme_id)
                replace[type_id] = theme_id
            else:
                failed = True
                break

    # Map existing response actions to the new themes
    if not failed:
        updated = 0

        rtable = s3db.dvr_response_action
        query = (rtable.deleted == False)
        rows = db(query).select(rtable.id,
                                rtable.response_type_id,
                                rtable.response_theme_ids,
                                rtable.realm_entity,
                                )
        for row in rows:
            if row.response_theme_ids:
                continue

            replace = themes.get(row.realm_entity)
            if replace:
                theme_id = replace.get(row.response_type_id)
                if theme_id:
                    row.update_record(response_theme_ids = [theme_id])
                    updated += 1

    if not failed:
        infoln("...done (%s themes created, %s response actions updated)" %
               (created, updated))
    else:
        infoln("...failed")

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
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
