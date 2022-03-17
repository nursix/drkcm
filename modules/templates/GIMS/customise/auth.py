"""
    AUTH module customisations for GIMS

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

    return realm_entity

# END =========================================================================

