"""
    AUTH module customisations for MRCMS

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

        organisation_id = None

        if not organisation_id:
            # Client records are owned by the case organisation
            ctable = s3db.dvr_case
            query = (ctable.person_id == row.id) & (ctable.deleted == False)
            case = db(query).select(ctable.organisation_id,
                                    limitby = (0, 1),
                                    ).first()
            if case:
                organisation_id = case.organisation_id

        if not organisation_id:
            # Staff records are owned by the employer organisation
            htable = s3db.hrm_human_resource
            query = (htable.person_id == row.id) & (htable.deleted == False)
            staff = db(query).select(htable.organisation_id,
                                     limitby = (0, 1),
                                     ).first()
            if staff:
                organisation_id = staff.organisation_id

        if organisation_id:
            realm_entity = s3db.pr_get_pe_id("org_organisation", organisation_id)

    elif tablename in ("dvr_case_activity",
                       "dvr_case_details",
                       "dvr_case_flag_case",
                       "dvr_case_language",
                       "dvr_note",
                       "dvr_residence_status",
                       "dvr_response_action",
                       "pr_group_membership",
                       "pr_person_details",
                       "pr_person_tag",
                       "cr_shelter_registration",
                       "cr_shelter_registration_history",
                       ):
        # Inherit from person via person_id
        table = s3db.table(tablename)
        ptable = s3db.pr_person
        query = (table._id == row.id) & (ptable.id == table.person_id)
        person = db(query).select(ptable.realm_entity,
                                  limitby = (0, 1),
                                  ).first()
        if person:
            realm_entity = person.realm_entity

    elif tablename in ("pr_address",
                       "pr_contact",
                       "pr_contact_emergency",
                       "pr_image",
                       ):
        # Inherit from person via pe_id
        table = s3db.table(tablename)
        ptable = s3db.pr_person
        query = (table._id == row.id) & (ptable.pe_id == table.pe_id)
        person = db(query).select(ptable.realm_entity,
                                  limitby = (0, 1),
                                  ).first()
        if person:
            realm_entity = person.realm_entity

    elif tablename in ("dvr_case_activity_need",
                       "dvr_case_activity_update",
                       ):
        # Inherit from case activity
        table = s3db.table(tablename)
        atable = s3db.dvr_case_activity
        query = (table._id == row.id) & (atable.id == table.case_activity_id)
        activity = db(query).select(atable.realm_entity,
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

    elif tablename == "doc_document":
        # Inherit from doc entity, alternatively context organisation
        realm_entity = document_realm_entity(table, row)

    #elif tablename == "cr_shelter":
    #    # Self-owned, OU of managing organisation (default ok)
    #    pass

    elif tablename == "cr_shelter_unit":
        # Inherit from shelter via shelter_id
        table = s3db.table(tablename)
        stable = s3db.cr_shelter
        query = (table._id == row.id) & (stable.id == table.shelter_id)
        shelter = db(query).select(stable.realm_entity,
                                   limitby = (0, 1),
                                   ).first()
        if shelter:
            realm_entity = shelter.realm_entity

    #elif tablename in ("org_group",
    #                   "org_organisation",
    #                   ):
    #    # Self-owned (default ok)
    #    pass

    return realm_entity

# -------------------------------------------------------------------------
def document_realm_entity(table, row):
    """
        Realm rule for doc_document
    """

    db = current.db
    s3db = current.s3db

    realm_entity = 0

    dtable = s3db.doc_document
    etable = s3db.doc_entity

    # Get the document record including instance type of doc_entity
    left = etable.on(etable.doc_id == dtable.doc_id)
    query = (dtable.id == row.id)
    row = db(query).select(dtable.id,
                           dtable.doc_id,
                           dtable.organisation_id,
                           etable.instance_type,
                           left = left,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return realm_entity

    document = row.doc_document
    instance_type = row.doc_entity.instance_type

    # Inherit the realm entity from instance, if available
    if document.doc_id and instance_type:
        itable = s3db.table(instance_type)
        if itable and "realm_entity" in itable.fields:
            query = (itable.doc_id == document.doc_id)
            instance = db(query).select(itable.realm_entity,
                                        limitby = (0, 1),
                                        ).first()
            if instance:
                realm_entity = instance.realm_entity

    # Fallback: use context organisation as realm entity
    if realm_entity == 0 and document.organisation_id:

        realm_entity = s3db.pr_get_pe_id("org_organisation",
                                         document.organisation_id,
                                         )
    return realm_entity

# -------------------------------------------------------------------------
def auth_user_resource(r, tablename):
    # TODO adjust to multitenancy
    #      org-admin created users: use managed org if single
    #      otherwise: provide selector of managed orgs

    settings = current.deployment_settings

    table = current.s3db.auth_user
    field = table.organisation_id



    field.default = settings.get_org_default_organisation()

# END =========================================================================
