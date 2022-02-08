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

    s3db = current.s3db

    from ..config import TESTSTATIONS

    # Determine user role
    has_role = current.auth.s3_has_role

    is_teststation_admin = has_role("ORG_ADMIN")
    managed_orgs = None

    if is_teststation_admin:
        from ..helpers import get_managed_orgs
        managed_orgs = get_managed_orgs(TESTSTATIONS)
        if not managed_orgs:
            is_teststation_admin = False

    is_org_group_admin = False if is_teststation_admin else has_role("ORG_GROUP_ADMIN")

    resource = organisation_id = None
    if is_teststation_admin or is_org_group_admin:
        # Determine organisation_id
        record = r.record
        master = r.tablename
        if master == "hrm_human_resource":
            resource = r.resource
            organisation_id = record.organisation_id if record else None

        elif master == "org_organisation":
            resource = r.component
            organisation_id = record.id

        elif master == "pr_person":
            resource = r.component
            table = resource.table
            if r.component_id:
                query = (table.id == r.component_id)
            else:
                query = (table.person_id == record.id)
            row = current.db(query).select(table.organisation_id,
                                           limitby = (0, 1),
                                           ).first()
            organisation_id = row.organisation_id if row else None

    table = resource.table if resource else s3db.hrm_human_resource

    if organisation_id:
        # Check if organisation is a (managed) test station
        if is_teststation_admin and organisation_id in managed_orgs:
            show_org_contact = org_contact_writable = True
        else:
            from ..helpers import is_org_group
            show_org_contact = is_org_group(organisation_id, TESTSTATIONS)
            org_contact_writable = False
    else:
        show_org_contact = org_contact_writable = False

    if show_org_contact:
        # Expose org_contact field
        org_contact = "org_contact"

        field = table.org_contact

        field.readable = True
        field.writable = org_contact_writable
        field.label = current.T("Test Station Manager")

        if is_teststation_admin:
            from core import WithAdvice
            org_contact = WithAdvice("org_contact",
                                     text = ("hrm",
                                             "human_resource",
                                             "TestStationManagerIntro",
                                             ),
                                     below = True,
                                     cmsxml = True,
                                     )
    else:
        org_contact = None

    from core import S3SQLCustomForm
    if r.component_name == "managers":
        # TODO add workflow-tags (with subheader)
        # TODO all fields read-only except tags

        field = table.organisation_id
        field.readable = field.writable = False

        if r.component_id:
            from ..helpers import PersonRepresentManager
            field = table.person_id
            field.readable = True
            field.writable = False
            field.represent = PersonRepresentManager(show_email = True,
                                                     show_phone = True,
                                                     show_link = False,
                                                     styleable = True,
                                                     )
        person_id = "person_id"
        current.s3db.configure("hrm_human_resource",
                               insertable = False,
                               deletable = False,
                               )
    else:
        person_id = None
        table.organisation_id.writable = False

    # Use custom-form for HRs
    crud_form = S3SQLCustomForm("organisation_id",
                                person_id,
                                "site_id",
                                "job_title_id",
                                org_contact,
                                "start_date",
                                "end_date",
                                "status",
                                )
    current.s3db.configure("hrm_human_resource",
                           crud_form = crud_form,
                           )

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
