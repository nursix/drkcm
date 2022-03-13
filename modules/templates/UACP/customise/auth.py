"""
    AUTH module customisations for UACP

    License: MIT
"""

from gluon import current

from s3dal import original_tablename

# -------------------------------------------------------------------------
def realm_entity(table, row):
    """
        Assign a Realm Entity to records
    """

    db = current.db
    s3db = current.s3db

    tablename = original_tablename(table)

    realm_entity = 0

    if tablename == "pr_person":

        # Client records are owned by the organisation
        # the case is assigned to
        ctable = s3db.br_case
        query = (ctable.person_id == row.id) & \
                (ctable.deleted == False)
        case = db(query).select(ctable.organisation_id,
                                limitby = (0, 1),
                                ).first()

        if case and case.organisation_id:
            realm_entity = s3db.pr_get_pe_id("org_organisation",
                                             case.organisation_id,
                                             )

    elif tablename in ("pr_address",
                       "pr_contact",
                       "pr_contact_emergency",
                       "pr_image",
                       ):

        # Inherit from person via PE
        table = s3db.table(tablename)
        ptable = s3db.pr_person
        query = (table._id == row.id) & \
                (ptable.pe_id == table.pe_id)
        person = db(query).select(ptable.realm_entity,
                                  limitby = (0, 1),
                                  ).first()
        if person:
            realm_entity = person.realm_entity

    elif tablename in ("br_appointment",
                       "br_case_activity",
                       "br_case_language",
                       "br_assistance_measure",
                       "br_note",
                       "pr_group_membership",
                       "pr_person_details",
                       "pr_person_tag",
                       ):

        # Inherit from person via person_id
        table = s3db.table(tablename)
        ptable = s3db.pr_person
        query = (table._id == row.id) & \
                (ptable.id == table.person_id)
        person = db(query).select(ptable.realm_entity,
                                  limitby = (0, 1),
                                  ).first()
        if person:
            realm_entity = person.realm_entity

    elif tablename == "br_case_activity_update":

        # Inherit from case activity
        table = s3db.table(tablename)
        atable = s3db.br_case_activity
        query = (table._id == row.id) & \
                (atable.id == table.case_activity_id)
        activity = db(query).select(atable.realm_entity,
                                    limitby = (0, 1),
                                    ).first()
        if activity:
            realm_entity = activity.realm_entity

    elif tablename == "br_assistance_measure_theme":

        # Inherit from measure
        table = s3db.table(tablename)
        mtable = s3db.br_assistance_measure
        query = (table._id == row.id) & \
                (mtable.id == table.measure_id)
        activity = db(query).select(mtable.realm_entity,
                                    limitby = (0, 1),
                                    ).first()
        if activity:
            realm_entity = activity.realm_entity

    elif tablename == "pr_group":

        # No realm-entity for case groups
        table = s3db.pr_group
        query = table._id == row.id
        group = db(query).select(table.group_type,
                                 limitby = (0, 1),
                                 ).first()
        if group and group.group_type == 7:
            realm_entity = None

    elif tablename == "project_task":

        # Inherit the realm entity from the assignee
        assignee_pe_id = row.pe_id
        instance_type = s3db.pr_instance_type(assignee_pe_id)
        if instance_type:
            table = s3db.table(instance_type)
            query = table.pe_id == assignee_pe_id
            assignee = db(query).select(table.realm_entity,
                                        limitby = (0, 1),
                                        ).first()
            if assignee and assignee.realm_entity:
                realm_entity = assignee.realm_entity

        # If there is no assignee, or the assignee has no
        # realm entity, fall back to the user organisation
        if realm_entity == 0:
            auth = current.auth
            user_org_id = auth.user.organisation_id if auth.user else None
            if user_org_id:
                realm_entity = s3db.pr_get_pe_id("org_organisation",
                                                 user_org_id,
                                                 )
    return realm_entity

# END =========================================================================

