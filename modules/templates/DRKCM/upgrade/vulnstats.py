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
stable = s3db.dvr_response_status
vtable = s3db.dvr_vulnerability
ttable = s3db.dvr_vulnerability_type

# Paths
IMPORT_XSLT_FOLDER = os.path.join(request.folder, "static", "formats", "s3csv")
TEMPLATE_FOLDER = os.path.join(request.folder, "modules", "templates", "DRKCM")

# -----------------------------------------------------------------------------
# Fix initial consultations
#
if not failed:

    ORG = "LEA Ellwangen"
    START = datetime.datetime(2023,1,1,0,0,0)
    END = datetime.datetime(2024,1,1,0,0,0)

    # Response statuses that mark actions as closed, but not canceled
    query = (stable.is_closed == True) & \
            ((stable.is_canceled == False) | (stable.is_canceled == None)) & \
            (stable.deleted == False)
    statuses = db(query)._select(stable.id)

    # Get the organisation ID
    query = (otable.name == ORG) & \
            (otable.deleted == False)
    row = db(query).select(otable.id, limitby=(0, 1)).first()
    if row:
        organisation_id = row.id
    else:
        info("...organisation not found...")
        failed = True
        organisation_id = None

    # Get the person IDs of all relevant cases
    query = (ctable.organisation_id == organisation_id) & \
            (ctable.archived == False) & \
            (ctable.deleted == False)
    cases = db(query)._select(ctable.person_id)

    # Lookup the response actions
    query = (atable.person_id.belongs(cases)) & \
            (atable.start_date >= START) & \
            (atable.start_date < END) & \
            (atable.status_id.belongs(statuses)) & \
            (atable.deleted == False)
    person_ids = db(query)._select(atable.person_id, distinct=True)
    total_clients = atable.person_id.count(distinct=True)
    total = db(query).select(total_clients).first()

    # Lookup registered vulnerabilities
    query = (vtable.person_id.belongs(person_ids)) & \
            ((vtable.date == None) | (vtable.date < END)) & \
            ((vtable.end_date == None) | (vtable.end_date >= START)) & \
            (vtable.deleted == False)
    num_affected = vtable.person_id.count(distinct=True)
    rows = db(query).select(vtable.vulnerability_type_id,
                            num_affected,
                            groupby = vtable.vulnerability_type_id,
                            )
    distribution = {row[vtable.vulnerability_type_id]: row[num_affected] for row in rows}
    row = db(query).select(num_affected).first()
    total_affected = row[num_affected] if row else 0

    # Lookup types
    query = (ttable.deleted == False)
    rows = db(query).select(ttable.id, ttable.name)
    types = {row.id: row.name for row in rows}

    # Build stats
    total_cases = total[total_clients]
    infoln("...result:")
    infoln("Anzahl beratene Personen: %s" % total_cases)
    infoln("")
    if total_affected and total_cases:
        percent = round((total_affected / total_cases) * 100, 2)
    else:
        percent = 0
    infoln("Davon mit Vulnerabilitäten: %s (%s%%)" % (total_affected, percent))
    infoln("")

    for type_id, type_name in types.items():
        num_cases = distribution.get(type_id, 0)
        if num_cases and total_cases:
            percent = round((num_cases / total_cases) * 100, 2)
        else:
            percent = 0
        infoln("%s: %s (%s%%)" % (type_name, num_cases, percent))
    infoln("")

# -----------------------------------------------------------------------------
# Finishing up
#
if failed:
    db.rollback()
    infoln("UPGRADE FAILED - Action rolled back.")
else:
    db.commit()
    infoln("UPGRADE SUCCESSFUL.")
