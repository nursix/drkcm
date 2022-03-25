"""
    AUTH module customisations for GIMS

    License: MIT
"""

from gluon import current

from s3dal import original_tablename

# -----------------------------------------------------------------------------
def realm_entity(table, row):
    """
        Assign a Realm Entity to records
    """

    db = current.db
    s3db = current.s3db

    tablename = original_tablename(table)

    realm_entity = 0

    if tablename == "pr_person":

        pass # using default

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

    elif tablename in ("pr_group_membership",
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

    elif tablename == "cr_shelter_population":

        # Inherit from shelter
        table = s3db.table(tablename)
        stable = s3db.cr_shelter
        query = (table._id == row.id) & \
                (stable.id == table.shelter_id)
        shelter = db(query).select(stable.realm_entity,
                                   limitby = (0, 1),
                                   ).first()
        if shelter:
            realm_entity = shelter.realm_entity

    return realm_entity

# =============================================================================
def update_commune_group_shelter_reader(user_id):
    """
        Automatically assign/remove the SHELTER_READER role for
        commune groups depending on which districts the user has
        the role for

        Args:
            user_id: the user ID
    """

    db = current.db
    s3db = current.s3db
    auth = current.auth

    # Get the group ID of the SHELTER_READER role
    rtable = auth.settings.table_group
    role = db(rtable.uuid == "SHELTER_READER").select(rtable.id,
                                                      limitby = (0, 1),
                                                      ).first()
    if not role:
        return
    role_id = role.id

    # Get all current SHELTER_READER assignments
    atable = auth.settings.table_membership
    query = (atable.user_id == user_id) & \
            (atable.group_id == role_id) & \
            (atable.deleted == False)
    assigned = db(query).select(atable.pe_id, atable.system).as_dict(key="pe_id")

    if not assigned:
        return

    elif 0 in assigned:
        # Global role => remove all system-assigned (as they are redundant)
        remove = [k for k, v in assigned.items() if v["system"]]
        assign = None

    else:
        # Look up all DISTRICTS and COMMUNES groups
        from ..config import DISTRICTS, COMMUNES
        gtable = s3db.org_group
        query = ((gtable.name == DISTRICTS) | (gtable.name.like("%s%%" % COMMUNES))) & \
                (gtable.name != COMMUNES) & \
                (gtable.deleted == False)
        groups = db(query).select(gtable.id,
                                  gtable.pe_id,
                                  gtable.name,
                                  ).as_dict(key="name")

        districts = groups[DISTRICTS]
        if districts["pe_id"] in assigned:
            # User has the role for the DISTRICTS org group
            # => auto-assign for all COMMUNES groups
            remove = None
            assign = []
            for name, group in groups.items():
                pe_id = group["pe_id"]
                if name.startswith(COMMUNES) and pe_id not in assigned:
                    assign.append(pe_id)
        else:
            # Get the pe_ids and district IDs of all districts
            mtable = s3db.org_group_membership
            otable = s3db.org_organisation
            ttable = s3db.org_organisation_tag
            join = [mtable.on((mtable.organisation_id == otable.id) & \
                              (mtable.group_id == districts["id"]) & \
                              (mtable.deleted == False)),
                    ttable.on((ttable.organisation_id == otable.id) & \
                              (ttable.tag == "DistrictID") & \
                              (ttable.deleted == False)),
                    ]
            query = (otable.deleted == False)
            rows = db(query).select(otable.pe_id,
                                    ttable.value,
                                    join = join,
                                    )

            # Determine which district groups the user should have and which not
            add, rmv = [], []
            for row in rows:
                district = row.org_organisation
                district_id = row.org_organisation_tag.value
                if not district_id:
                    continue
                district_group_name = "%s (%s)" % (COMMUNES, district_id)
                if district.pe_id in assigned:
                    add.append(district_group_name)
                else:
                    rmv.append(district_group_name)

            # Also remove those district groups for which there is no district
            for name, group in groups.items():
                if name.startswith(COMMUNES) and name not in add:
                    rmv.append(name)

            # Determine which assignments need to be added/removed
            assign, remove = [], []
            for name, group in groups.items():
                pe_id = group["pe_id"]
                if name in add and pe_id not in assigned:
                    assign.append(pe_id)
                elif name in rmv and pe_id in assigned and assigned[pe_id]["system"]:
                    remove.append(pe_id)

    # Remove/add assignments as needed
    if remove:
        for pe_id in remove:
            auth.s3_remove_role(user_id, role_id, for_pe=pe_id)
    if assign:
        for pe_id in assign:
            auth.s3_assign_role(user_id, role_id, for_pe=pe_id, system=True)

# -----------------------------------------------------------------------------
def assign_role(user_id, role_id, for_pe=None):
    """
        Extend standard role assignment with auto-assignment of SHELTER_READER
    """

    current.auth.s3_assign_role(user_id, role_id, for_pe=for_pe)
    update_commune_group_shelter_reader(user_id)

def remove_role(user_id, role_id, for_pe=None):
    """
        Extend standard role assignment with auto-assignment of SHELTER_READER
    """

    current.auth.s3_remove_role(user_id, role_id, for_pe=for_pe)
    update_commune_group_shelter_reader(user_id)

# END =========================================================================
