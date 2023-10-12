"""
    AUTH module customisations for MRCMS

    License: MIT
"""

from gluon import current

from s3dal import original_tablename

from core import IS_ONE_OF

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

        # NOTE these rules are only effective upon explicit realm
        #      update onaccept of the case/staff record - because
        #      the person record is, necessarily, written first!
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

    #elif tablename in ("dvr_case_flag",
    #                   "dvr_appointment_type",
    #                   "dvr_case_event_type",
    #                   ):
    #    # Owned by the organisation for which they are defined (default okay)
    #    pass

    elif tablename == "dvr_case_flag_case":

        table = s3db.table(tablename)
        ftable = s3db.dvr_case_flag
        query = (table._id == row.id) & (ftable.id == table.flag_id)
        flag = db(query).select(ftable.organisation_id, limitby=(0, 1)).first()
        if flag and flag.organisation_id:
            realm_entity = s3db.pr_get_pe_id("org_organisation", flag.organisation_id)
        else:
            # Inherit from person via person_id
            realm_entity = person_realm_entity(table, row, default=realm_entity)

    elif tablename == "dvr_case_appointment":

        table = s3db.table(tablename)
        ttable = s3db.dvr_case_appointment_type
        query = (table._id == row.id) & (ttable.id == table.type_id)
        atype = db(query).select(ttable.organisation_id, limitby=(0, 1)).first()
        if atype and atype.organisation_id:
            realm_entity = s3db.pr_get_pe_id("org_organisation", atype.organisation_id)
        else:
            # Inherit from person via person_id
            realm_entity = person_realm_entity(table, row, default=realm_entity)

    elif tablename == "dvr_case_event":

        table = s3db.table(tablename)
        ttable = s3db.dvr_case_event_type
        query = (table._id == row.id) & (ttable.id == table.type_id)
        etype = db(query).select(ttable.organisation_id, limitby=(0, 1)).first()
        if etype and etype.organisation_id:
            realm_entity = s3db.pr_get_pe_id("org_organisation", etype.organisation_id)
        else:
            # Inherit from person via person_id
            realm_entity = person_realm_entity(table, row, default=realm_entity)

    elif tablename in ("dvr_case_activity",
                       "dvr_case_details",
                       "dvr_case_language",
                       "dvr_note",
                       "dvr_residence_status",
                       "dvr_response_action",
                       "pr_group_membership",
                       "pr_identity",
                       "pr_person_details",
                       "pr_person_tag",
                       "cr_shelter_registration",
                       "cr_shelter_registration_history",
                       ):
        # Inherit from person via person_id
        table = s3db.table(tablename)
        realm_entity = person_realm_entity(table, row, default=realm_entity)

    elif tablename in ("pr_address",
                       "pr_contact",
                       "pr_contact_emergency",
                       "pr_image",
                       ):
        # Inherit from person via pe_id
        table = s3db.table(tablename)
        realm_entity = person_realm_entity(table, row, key="pe_id", default=realm_entity)

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

    elif tablename in ("doc_document", "doc_image"):
        # Inherit from doc entity, alternatively context organisation
        table = s3db.table(tablename)
        realm_entity = doc_realm_entity(table, row)

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

    #elif tablename in ("org_site_presence",
    #                   "org_site_presence_event",
    #                   ):
    #    # Owned by the site, OU of managing organisation (default ok)
    #    pass

    return realm_entity

# -------------------------------------------------------------------------
def doc_realm_entity(table, row):
    """
        Realm rule for doc_document/doc_image
    """

    db = current.db
    s3db = current.s3db

    realm_entity = 0

    etable = s3db.doc_entity

    # Get the document record including instance type of doc_entity
    left = etable.on(etable.doc_id == table.doc_id)
    query = (table.id == row.id)
    row = db(query).select(table.id,
                           table.doc_id,
                           table.organisation_id,
                           etable.instance_type,
                           left = left,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return realm_entity

    document = row[table]
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
def person_realm_entity(table, row, key="person_id", default=0):
    """
        Returns the realm entity of the context person record

        Args:
            table: the Table
            row: the Row
            key: the key referencing pr_person (person_id|pe_id)
            default: the default to return if no context record is found

        Returns:
            the realm entity (pe_id) of the context person record
    """

    ptable = current.s3db.pr_person
    query = (table._id == row.id)
    if key == "person_id":
        query &= (ptable.id == table.person_id)
    elif key == "pe_id":
        query &= (ptable.pe_id == table.pe_id)
    else:
        return default

    person = current.db(query).select(ptable.realm_entity,
                                      limitby = (0, 1),
                                      ).first()
    return person.realm_entity if person else default

# -------------------------------------------------------------------------
def auth_user_resource(r, tablename):

    db = current.db
    s3db = current.s3db
    auth = current.auth

    table = auth.settings.table_user
    field = table.organisation_id
    field.comment = None

    if not auth.s3_has_roles(("ADMIN", "ORG_GROUP_ADMIN")):
        # Limit OrgAdmins to their managed organisations
        from ..helpers import get_managed_orgs
        organisation_ids = get_managed_orgs()

        otable = s3db.org_organisation
        dbset = db(otable.id.belongs(organisation_ids))
        field.requires = IS_ONE_OF(dbset, "org_organisation.id",
                                   field.represent,
                                   orderby = "name",
                                   )

        # Default if single organisation
        if len(organisation_ids) == 1:
            field.default = organisation_ids[0]
            field.writable = False

# END =========================================================================
