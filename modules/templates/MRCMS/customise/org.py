"""
    ORG module customisations for MRCMS

    License: MIT
"""

from gluon import current

from ..config import PROVIDERS

# -------------------------------------------------------------------------
def org_group_controller(**attr):

    s3 = current.response.s3

    standard_prep = s3.prep
    def prep(r):

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource
        table = resource.table

        record = r.record
        if record and record.name == PROVIDERS:
            # Group name cannot be changed
            field = table.name
            field.writable = False

            # Group cannot be deleted
            resource.configure(deletable = False)

        # Cannot create new groups unless Admin or site-wide OrgGroupAdmin
        from ..helpers import get_role_realms
        if not current.auth.s3_has_role("ADMIN") and \
           get_role_realms("ORG_GROUP_ADMIN") is not None:
            resource.configure(insertable = False)

        if r.interactive:
            if not r.component:
                from core import S3SQLCustomForm
                crud_form = S3SQLCustomForm("name", "comments")
                resource.configure(crud_form = crud_form)
            elif r.component_name == "organisation":
                r.component.configure(insertable = False,
                                      editable = False,
                                      deletable = False,
                                      )

        list_fields = ["name", "comments"]
        resource.configure(list_fields = list_fields,
                           )

        # TODO filter form
        # TODO CRUD string translations

        return result
    s3.prep = prep

    # TODO postp to remove DELETE-button for PROVIDERS

    # Custom rheader
    from ..rheaders import mrcms_org_rheader
    attr["rheader"] = mrcms_org_rheader

    return attr

# -------------------------------------------------------------------------
def org_organisation_resource(r, tablename):

    # TODO implement
    # use branches
    # customise form+filters
    # only OrgGroupAdmin can create new root orgs, but org admin can create branches
    # org group mandatory when any org group exists
    # default org group by tag?
    pass

def org_organisation_controller(**attr):

    # TODO if not OrgGroupAdmin or OrgAdmin for multiple orgs, and staff of only one org => open that org

    # TODO not insertable on main tab unless OrgGroupAdmin

    # TODO form with reduced fields
    #      => also customise component forms (via resource)

    # TODO custom list fields

    # TODO filters

    # Custom rheader
    from ..rheaders import mrcms_org_rheader
    attr["rheader"] = mrcms_org_rheader

    return attr

# -------------------------------------------------------------------------
# TODO drop?
def org_facility_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    # Hide "code" field (not needed)
    table = s3db.org_facility
    field = table.code
    field.readable = field.writable = False

    # Location selector just needs country + address
    from core import LocationSelector
    field = table.location_id
    field.widget = LocationSelector(levels = ["L0"],
                                    show_address=True,
                                    show_map = False,
                                    )

    field = table.obsolete
    field.label = T("Inactive")
    field.represent = lambda opt: T("Inactive") if opt else current.messages["NONE"]

    # Custom list fields
    list_fields = ["name",
                   "site_facility_type.facility_type_id",
                   "organisation_id",
                   "location_id",
                   "contact",
                   "phone1",
                   "phone2",
                   "email",
                   #"website",
                   "obsolete",
                   "comments",
                   ]

    # Custom filter widgets
    from core import TextFilter, OptionsFilter, get_filter_options
    filter_widgets = [TextFilter(["name",
                                  "organisation_id$name",
                                  "organisation_id$acronym",
                                  "comments",
                                  ],
                                 label = T("Search"),
                                 ),
                      OptionsFilter("site_facility_type.facility_type_id",
                                    options = lambda: get_filter_options(
                                                            "org_facility_type",
                                                            translate = True,
                                                            ),
                                    ),
                      OptionsFilter("organisation_id",
                                    ),
                      OptionsFilter("obsolete",
                                    options = {False: T("No"),
                                               True: T("Yes"),
                                               },
                                    default = [False],
                                    cols = 2,
                                    )
                      ]

    s3db.configure("org_facility",
                   #deletable = False,
                   filter_widgets = filter_widgets,
                   list_fields = list_fields,
                   )

# -------------------------------------------------------------------------
# TODO drop?
def org_facility_controller(**attr):

    # Allow selection of all countries
    current.deployment_settings.gis.countries = []

    # Custom rheader+tabs
    if current.request.controller == "org":
        from ..rheaders import mrcms_org_rheader
        attr = dict(attr)
        attr["rheader"] = mrcms_org_rheader

    return attr

# -------------------------------------------------------------------------
def org_site_presence_event_resource(r, tablename):

    s3db = current.s3db

    # Represent registering user by their name
    table = s3db.org_site_presence_event
    field = table.created_by
    field.represent = s3db.auth_UserRepresent(show_name=True,
                                              show_email=False,
                                              )

# END =========================================================================
