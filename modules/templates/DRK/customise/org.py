"""
    ORG module customisations for DRK

    License: MIT
"""

from gluon import current

# -------------------------------------------------------------------------
def org_facility_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    # Hide "code" field (not needed)
    table = s3db.org_facility
    field = table.code
    field.readable = field.writable = False

    # Location selector just needs country + address
    from core import S3LocationSelector
    field = table.location_id
    field.widget = S3LocationSelector(levels = ["L0"],
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
def org_facility_controller(**attr):

    # Allow selection of all countries
    current.deployment_settings.gis.countries = []

    # Custom rheader+tabs
    if current.request.controller == "org":
        from ..rheaders import drk_org_rheader
        attr = dict(attr)
        attr["rheader"] = drk_org_rheader

    return attr

# END =========================================================================
