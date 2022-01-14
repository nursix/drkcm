"""
    HRM module customisations for RLPPTM

    License: MIT
"""

from gluon import current

# -------------------------------------------------------------------------
def human_resource_onvalidation(form):

    person_id = form.vars.get("person_id")
    if person_id:
        table = current.s3db.hrm_human_resource
        query = (table.person_id == person_id) & \
                (table.deleted == False)
        duplicate = current.db(query).select(table.id,
                                             limitby = (0, 1),
                                             ).first()
        if duplicate:
            form.errors.person_id = current.T("Person already has a staff record")

# -------------------------------------------------------------------------
def hrm_human_resource_resource(r, tablename):

    current.s3db.add_custom_callback("hrm_human_resource",
                                     "onvalidation",
                                     human_resource_onvalidation,
                                     )

# -------------------------------------------------------------------------
def hrm_human_resource_controller(**attr):

    T = current.T
    s3db = current.s3db

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Restrict data formats
        from ..helpers import restrict_data_formats
        restrict_data_formats(r)

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource

        is_org_group_admin = current.auth.s3_has_role("ORG_GROUP_ADMIN")

        # Configure components to inherit realm_entity from person
        s3db.configure("pr_person",
                       realm_components = ("person_details",
                                           "contact",
                                           "address",
                                           ),
                       )
        phone_label = current.deployment_settings.get_ui_label_mobile_phone()
        if r.representation == "xls":
            s3db.add_components("pr_pentity",
                                pr_address = ({"name": "home_address",
                                               "joinby": "pe_id",
                                               "filterby": {"type": 1},
                                               "multiple": False,
                                               }),
                                )

            # Site obsolete-flag representation
            stable = s3db.org_site
            field = stable.obsolete
            field.label = T("Closed")
            field.represent = lambda v, row=None: T("yes") if v else "-"

            list_fields = ["organisation_id",
                           "site_id",
                           "site_id$obsolete",
                           "site_id$location_id$addr_street",
                           "site_id$location_id$L4",
                           "site_id$location_id$L3",
                           "site_id$location_id$addr_postcode",
                           "person_id",
                           "job_title_id",
                           (T("Email"), "person_id$email.value"),
                           (phone_label, "person_id$phone.value"),
                           (T("Home Address"), "person_id$home_address.location_id"),
                           "status",
                           ]
        else:
            list_fields = ["organisation_id",
                           "person_id",
                           "job_title_id",
                           "site_id",
                           (T("Email"), "person_id$email.value"),
                           (phone_label, "person_id$phone.value"),
                           "status",
                           ]

        from core import OptionsFilter, TextFilter, get_filter_options
        filter_widgets = [
            TextFilter(["person_id$first_name",
                        "person_id$last_name",
                        "organisation_id$name",
                        "person_id$email.value",
                        "person_id$phone.value",
                        ],
                       label = T("Search"),
                       ),
            OptionsFilter("job_title_id",
                          options = lambda: get_filter_options("hrm_job_title"),
                          hidden = True,
                          ),
            ]
        if is_org_group_admin:
            filter_widgets[1:1] = [
                OptionsFilter(
                    "organisation_id$group__link.group_id",
                    label = T("Organization Group"),
                    options = lambda: get_filter_options("org_group"),
                    ),
                OptionsFilter(
                    "organisation_id$organisation_type__link.organisation_type_id",
                    label = T("Organization Type"),
                    options = lambda: get_filter_options("org_organisation_type"),
                    hidden = True,
                    ),
                ]

        resource.configure(filter_widgets = filter_widgets,
                           list_fields = list_fields,
                           )

        return result
    s3.prep = prep

    return attr

# END =========================================================================
