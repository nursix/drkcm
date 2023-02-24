# Script to restore missing project links
#
# RLPPTM Template Version 1.20.3
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R restore_project_links.py
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

# -----------------------------------------------------------------------------
# Restore project links
#
if not failed:
    info("Restore project links...")

    # Get the TESTS_PUBLIC project id
    query = (ptable.code == "TESTS-PUBLIC") & \
            (ptable.deleted == False)
    project = db(query).select(ptable.id, limitby=(0, 1)).first()
    if not project:
        failed = True
        infoln("...failed (project not found)")

    # Find deleted project links matching that project ID
    expr = '%%"project_id": %s%%' % project.id
    query = (ltable.deleted == True) & \
            (ltable.deleted_fk.like(expr))
    rows = db(query).select(ltable.id,
                            ltable.deleted_fk,
                            )
    seen, authors = set(), set()
    skipped, restored = 0, 0
    for row in rows:
        deleted_fk = json.loads(row.deleted_fk)

        # Load and validate project_id and organisation_id
        project_id = deleted_fk.get("project_id")
        organisation_id = deleted_fk.get("organisation_id")
        if not project_id or not organisation_id:
            continue
        if project_id != project.id:
            continue
        if organisation_id in seen:
            continue

        # Verify that organisation still exists
        query = (otable.id == organisation_id) & \
                (otable.deleted == False)
        organisation = db(query).select(otable.id, limitby=(0, 1)).first()
        if not organisation:
            continue
        seen.add(organisation.id)

        # Check for existing project links of this organisation
        query = (ltable.organisation_id == organisation.id) & \
                (ltable.project_id != None) & \
                (ltable.deleted == False)
        if db(query).select(ltable.id, limitby=(0, 1)).first():
            info(".")
            skipped += 1
            continue

        # Prepare update
        update = {"organisation_id": organisation.id,
                  "project_id": project.id,
                  "deleted": False,
                  "deleted_fk": None,
                  }

        # Check that authors still exist
        for fn in ("created_by", "modified_by", "owned_by_user"):
            user_id = deleted_fk.get(fn)
            if user_id not in authors:
                query = (utable.id == user_id) & (utable.deleted == False)
                if db(query).select(utable.id, limitby=(0, 1)).first():
                    authors.add(user_id)
                else:
                    continue
            update[fn] = user_id

        # Restore the link
        row.update_record(**update)
        auth.set_realm_entity(ltable, row, force_update=True)
        info("+")
        restored += 1

    infoln("...done (%s links restored, %s skipped)" % (restored, skipped))

# -----------------------------------------------------------------------------
# Fix project roles
#
if not failed:
    info("Fix project roles")

    query = (ltable.role == None) & (ltable.deleted == False)
    updated = db(query).update(role=2)

    infoln("...done (%s records updated)" % updated)

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
