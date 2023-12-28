# Database upgrade script
#
# DRKCM Template Version 2.4.3 => 2.4.4
#
# Execute in web2py folder after code upgrade like:
# python web2py.py -S eden -M -R applications/eden/modules/templates/DRKCM/upgrade/2.4.3-2.4.4.py
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
otable = s3db.org_organisation
ctable = s3db.dvr_case
atable = s3db.dvr_response_action
ttable = s3db.dvr_response_type
stable = s3db.dvr_response_status

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Fix initial consultations
#
if not failed:
    info("Fix initial consultations")

    orgname = "LEA Ellwangen"

    initial_codes = ("INI", "INI+I")
    followup_codes = ("FUP", "FUP+I")
    convert = {"FUP": "INI", "FUP+I": "INI+I"}

    # Get all consultation types {"code": id} {id: "code"}
    query = (ttable.is_consultation == True) & \
            (ttable.deleted == False)
    types = db(query).select(ttable.id, ttable.code)
    type_codes = {t.code: t.id for t in types}
    type_ids = {t.id: t.code for t in types}

    # Get the LEA Organisation
    query = (otable.name == orgname) & (otable.deleted == False)
    organisation = db(query).select(otable.id, limitby=(0, 1)).first()
    if organisation:
        organisation_id = organisation.id

        # Valid cases of the organisation
        query = (ctable.organisation_id == organisation_id) & \
                (ctable.archived == False) & \
                (ctable.deleted == False)
        cases = db(query)._select(ctable.person_id)

        # Closed (but not canceled) statuses
        statuses = (stable.is_closed == True) & \
                   (stable.is_canceled == False) & \
                   (stable.deleted == False)
        rows = db(query).select(stable.id)
        statuses = {row.id for row in rows}

        # Initial consultation types
        query = (ttable.code.belongs(initial_codes)) & \
                (ttable.is_consultation == True) & \
                (ttable.deleted == False)
        rows = db(query).select(ttable.id)
        initial_types = {row.id for row in rows}

        # Follow-up consultation types
        query = (ttable.code.belongs(followup_codes)) & \
                (ttable.is_consultation == True) & \
                (ttable.deleted == False)
        rows = db(query).select(ttable.id)
        followup_types = {row.id for row in rows}

        query = (atable.response_type_id.belongs(followup_types)) & \
                (atable.status_id.belongs(statuses)) & \
                (atable.person_id.belongs(cases)) & \
                (atable.deleted == False)
        rows = db(query).select(atable.person_id, distinct=True)
        updated = 0
        for row in rows:

            person_id = row.person_id

            # Check if this person has an initial consultation registered
            query = (atable.person_id == row.person_id) & \
                    (atable.response_type_id.belongs(initial_types)) & \
                    (atable.status_id.belongs(statuses)) & \
                    (atable.deleted == False)
            if not db(query).select(atable.id, limitby=(0, 1)).first():
                # No initial consultation registered

                # Get the first follow-up consultation
                query = (atable.person_id == row.person_id) & \
                        (atable.response_type_id.belongs(followup_types)) & \
                        (atable.status_id.belongs(statuses)) & \
                        (atable.deleted == False)
                action = db(query).select(atable.id,
                                          atable.response_type_id,
                                          limitby = (0, 1),
                                          orderby = atable.start_date,
                                          ).first()

                # Get the corresponding initial type
                old_code = type_ids.get(action.response_type_id)
                new_code = convert.get(old_code)
                new_id = type_codes.get(new_code)
                if new_id:
                    # Change response type
                    action.update_record(response_type_id = new_id,
                                            modified_by = atable.modified_by,
                                            modified_on = atable.modified_on,
                                            )
                    updated += 1
                    info("+")
                else:
                    info("-")
            else:
                # Initial consultation already registered
                info(".")

        infoln("...done (%s records updated)" % updated)
    else:
        failed = True
        infoln("...failed (organisation %s not found)" % orgname)

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
