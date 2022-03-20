"""
    CR module customisations for GIMS

    License: MIT
"""

from collections import OrderedDict
from gluon import current, IS_EMPTY_OR, DIV

from ..helpers import restrict_data_formats

# -------------------------------------------------------------------------
def cr_shelter_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.cr_shelter

    shelter_status_opts = OrderedDict(((2, T("Open##status")),
                                       (1, T("Closed")),
                                       ))

    from core import LocationFilter, \
                     S3LocationSelector, \
                     OptionsFilter, \
                     S3PriorityRepresent, \
                     S3SQLCustomForm, \
                     S3SQLInlineComponent, \
                     S3SQLInlineLink, \
                     TextFilter, \
                     s3_fieldmethod, \
                     get_filter_options

    from ..helpers import ShelterDetails, ServiceListRepresent

    # Field methods for a more compact representation
    # of place/contact information
    table.place = s3_fieldmethod("place",
                                 ShelterDetails.place,
                                 represent = ShelterDetails.place_represent,
                                 )

    table.contact = s3_fieldmethod("contact",
                                   ShelterDetails.contact,
                                   represent = ShelterDetails.contact_represent,
                                   )

    # No add-link for organisation
    field = table.organisation_id
    field.comment = None

    # Custom label + tooltip for population
    field = table.population
    population_label = T("Current Population##shelter")
    field.label = population_label
    field.comment = DIV(_class="tooltip",
                        _title="%s|%s" % (population_label,
                                          T("Current shelter population as a number of people"),
                                          ))

    # Enable contact name and website fields
    field = table.contact_name
    field.readable = field.writable = True
    field = table.website
    field.readable = field.writable = True

    # Shelter type is required
    field = table.shelter_type_id
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other

    # Configure location selector
    field = table.location_id
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other
    field.widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                      required_levels = ("L1", "L2", "L3"),
                                      show_address = True,
                                      show_postcode = True,
                                      show_map = True,
                                      )

    # Color-coded status representation
    field = table.status
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other
    field.represent = S3PriorityRepresent(shelter_status_opts,
                                          {1: "red",
                                           2: "green",
                                           }).represent

    ltable = s3db.cr_shelter_service_shelter
    field = ltable.service_id
    field.label = T("Services")
    field.represent = ServiceListRepresent(lookup = "cr_shelter_service",
                                           )

    # CRUD Form
    crud_fields = [# ----- Shelter -----
                   "organisation_id",
                   "name",
                   "shelter_type_id",
                   S3SQLInlineLink(
                        "shelter_service",
                        field = "service_id",
                        label = T("Services"),
                        cols = 3,
                        render_list = True,
                        ),
                   "status",
                   # ----- Address -----
                   "location_id",
                   # ----- Contact -----
                   "website",
                   "contact_name",
                   "phone",
                   "email",
                   # ----- Capacity and Population -----
                   "capacity",
                   S3SQLInlineComponent("population",
                                        label = T("Current Population##shelter"),
                                        fields = ["type_id",
                                                  "population_adults",
                                                  "population_children",
                                                  ],
                                        ),
                   "population",
                   "population_adults",
                   "population_children",
                   # ----- Other Details -----
                   "comments"
                   ]

    subheadings = {"organisation_id": T("Facility"),
                   "location_id": T("Address"),
                   "website": T("Contact Information"),
                   "capacity": T("Capacity / Occupancy"),
                   "comments": T("Administrative"),
                   }

    # Filter widgets
    filter_widgets = [TextFilter(["name",
                                  ],
                                 label = T("Search"),
                                 ),
                      OptionsFilter("status",
                                    options = shelter_status_opts,
                                    default = 2,
                                    cols = 2,
                                    sort = False,
                                    ),
                      OptionsFilter("shelter_service__link.service_id",
                                    options = lambda: get_filter_options("cr_shelter_service"),
                                    ),
                      LocationFilter("location_id",
                                     levels = ["L2", "L3"],
                                     hidden = True,
                                     ),
                      OptionsFilter("shelter_type_id",
                                    options = lambda: get_filter_options("cr_shelter_type"),
                                    hidden = True,
                                    ),
                      OptionsFilter("organisation_id",
                                    hidden = True,
                                    ),
                      ]

    # List fields
    list_fields = ["organisation_id",
                   "name",
                   "shelter_type_id",
                   "status",
                   (T("Available Capacity"), "available_capacity"),
                   (T("Place"), "place"),
                   (T("Contact"), "contact"),
                   #"website",
                   #"shelter_service__link.service_id",
                   ]

    s3db.configure("cr_shelter",
                   crud_form = S3SQLCustomForm(*crud_fields),
                   subheadings = subheadings,
                   extra_fields = ["contact_name",
                                   "phone",
                                   "email",
                                   "location_id$L3",
                                   "location_id$L2",
                                   "location_id$L1",
                                   "population",
                                   "capacity",
                                   ],
                  filter_widgets = filter_widgets,
                  list_fields = list_fields,
                  )

# -------------------------------------------------------------------------
def cr_shelter_controller(**attr):

    settings = current.deployment_settings
    s3 = current.response.s3

    auth = current.auth

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        # Restrict data formats
        restrict_data_formats(r)

        # Enable catalog layers in map views
        settings.gis.widget_catalogue_layers = True

        # Hide last update except for own records
        record = r.record
        if not r.record or \
            not auth.s3_has_permission("update", r.table, record_id=record.id):
            s3.hide_last_update = True

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import cr_rheader
    attr["rheader"] = cr_rheader

    return attr

# END =========================================================================
