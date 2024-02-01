"""
    ACT module customisations for MRCMS

    License: MIT
"""

from gluon import current
from gluon.storage import Storage

from core import IS_ONE_OF, OptionsFilter

# =============================================================================
def act_beneficiary_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.act_beneficiary

    # Custom label for person_id
    field = table.person_id
    field.label = T("Participant")

    # Link to case file if permitted, include ID in representation
    fmt = "[%(pe_label)s] %(last_name)s, %(first_name)s"
    linkto = current.auth.permission.accessible_url(c = "dvr",
                                                    f = "person",
                                                    t = "pr_person",
                                                    args = ["[id]"],
                                                    extension = "",
                                                    )
    field.represent = s3db.pr_PersonRepresent(fields = ("pe_label",
                                                        "last_name",
                                                        "first_name",
                                                        ),
                                              labels = fmt,
                                              show_link = linkto is not False,
                                              linkto = linkto or None,
                                              )

    # Autocomplete using dvr/person controller, adapt comment
    from core import S3PersonAutocompleteWidget
    field.widget = S3PersonAutocompleteWidget(controller = "dvr",
                                              function = "person_search",
                                              )
    field.comment = T("Enter some characters of the ID or name to start the search, then select from the drop-down")

    # Using custom terminology "participant" instead of "beneficiary"
    crud_strings = current.response.s3.crud_strings
    crud_strings[tablename] = Storage(
        label_create = T("Add Participant"),
        title_display = T("Participant Details"),
        title_list = T("Participants"),
        title_update = T("Edit Participant"),
        label_list_button = T("List Participants"),
        label_delete_button = T("Delete Participant"),
        msg_record_created = T("Participant added"),
        msg_record_modified = T("Participant updated"),
        msg_record_deleted = T("Participant deleted"),
        msg_list_empty = T("No Participants currently registered"),
        )

# =============================================================================
def act_activity_resource(r, tablename):

    s3db = current.s3db

    table = s3db.act_activity
    insertable = True

    field = table.organisation_id
    field.writable = False

    # Configure organisation_id and insertable depending on which
    # organisations the user has permission to create activities for
    from ..helpers import permitted_orgs
    organisation_ids = permitted_orgs("create", "act_activity")
    if not organisation_ids:
        # No organisations => not insertable
        insertable = False
    elif len(organisation_ids) == 1:
        # Exactly one organisation => default
        field.default = organisation_ids[0]
    else:
        # Multiple organisations => selectable
        otable = s3db.org_organisation
        dbset = current.db(otable.id.belongs(organisation_ids))
        field.requires = IS_ONE_OF(dbset, "org_organisation.id",
                                   field.represent,
                                   )
        field.writable = True

    s3db.configure("act_activity", insertable=insertable)

# -----------------------------------------------------------------------------
def act_activity_controller(**attr):

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        if not r.record:
            resource = r.resource
            table = resource.table

            # Custom list fields
            list_fields = ["name",
                           "type_id",
                           "date",
                           "end_date",
                           "time",
                           "place",
                           ]

            from ..helpers import permitted_orgs
            organisation_ids = permitted_orgs("read", "act_activity")
            if len(organisation_ids) != 1:
                # Include organisation_id in list_fields
                list_fields.insert(0, "organisation_id")

                # Add organisation filter
                represent = table.organisation_id.represent
                filter_opts = represent.bulk(organisation_ids)
                filter_opts.pop(None, None)
                org_filter = OptionsFilter("organisation_id",
                                           options = filter_opts,
                                           )
                filter_widgets = resource.get_config("filter_widgets")
                filter_widgets.insert(1, org_filter)

            resource.configure(list_fields = list_fields,
                               orderby = "act_activity.date desc",
                               )
        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import act_rheader
    attr["rheader"] = act_rheader

    return attr

# END =========================================================================
