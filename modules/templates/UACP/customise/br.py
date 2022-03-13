"""
    BR module customisations for UACP

    License: MIT
"""

from collections import OrderedDict

from gluon import current, URL, IS_EMPTY_OR, IS_LENGTH, I, SPAN, IS_NOT_EMPTY
from gluon.storage import Storage

from core import FS, IS_ONE_OF

from ..helpers import restrict_data_formats

# -------------------------------------------------------------------------
def br_home():
    """ Do not redirect to person-controller """

    T = current.T

    return {"module_name": T("Current Needs")}

# -------------------------------------------------------------------------
def chargeable_warning(v, row=None):
    """ Visually enhanced representation of chargeable-flag """

    T = current.T

    if v:
        return SPAN(T("yes"),
                    I(_class = "fa fa-exclamation-triangle"),
                    _class = "charge-warn",
                    )
    else:
        return SPAN(T("no"),
                    _class = "free-hint",
                    )

# -------------------------------------------------------------------------
def offer_onaccept(form):
    """
        Custom onaccept-routine for offers of assistance
            - trigger direct offer notifications on approval
    """

    # Get record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    db = current.db
    s3db = current.s3db

    # Get the record
    table = s3db.br_assistance_offer
    query = (table.id == record_id)
    record = db(query).select(table.id,
                              table.status,
                              limitby = (0, 1),
                              ).first()
    if not record:
        return

    if record.status == "APR":
        # Look up all pending direct offers
        dotable = s3db.br_direct_offer
        query = (dotable.offer_id == record_id) & \
                (dotable.notify == True) & \
                (dotable.notified_on == None)
        pending = db(query).select(dotable.id)

        # TODO do this async?
        from ..helpers import notify_direct_offer
        for direct_offer in pending:
            notify_direct_offer(direct_offer.id)

# -------------------------------------------------------------------------
def offer_date_dt_orderby(field, direction, orderby, left_joins):
    """
        When sorting offers by date, use created_on to maintain
        consistent order of multiple offers on the same date
    """

    sorting = {"table": field.tablename,
               "direction": direction,
               }
    orderby.append("%(table)s.date%(direction)s,%(table)s.created_on%(direction)s" % sorting)

# -------------------------------------------------------------------------
def br_assistance_offer_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.br_assistance_offer

    # Configure fields
    from ..helpers import ProviderRepresent
    from core import S3PriorityRepresent

    field = table.pe_id
    field.label = T("Provider")
    field.represent = ProviderRepresent()

    if r.function == "assistance_offer":
        from core import WithAdvice
        field = table.description
        field.widget = WithAdvice(field.widget,
                                  text = ("br",
                                          "assistance_offer",
                                          "OfferDetailsIntro",
                                          ),
                                  )

    field = table.contact_phone
    field.label = T("Phone #")

    field = table.chargeable
    field.represent = chargeable_warning

    # Color-coded representation of availability/status
    field = table.availability
    availability_opts = s3db.br_assistance_offer_availability
    field.represent = S3PriorityRepresent(dict(availability_opts),
                                          {"AVL": "green",
                                           "OCP": "amber",
                                           "RTD": "black",
                                           }).represent
    field = table.status
    status_opts = s3db.br_assistance_offer_status
    field.represent = S3PriorityRepresent(dict(status_opts),
                                          {"NEW": "lightblue",
                                           "APR": "green",
                                           "BLC": "red",
                                           }).represent

    subheadings = {"need_id": T("Offer"),
                   "location_id": T("Place where help is provided"),
                   "contact_name": T("Contact Information"),
                   "availability": T("Availability and Status"),
                   }

    s3db.configure("br_assistance_offer",
                    # Default sort order: newest first
                    orderby = "br_assistance_offer.date desc, br_assistance_offer.created_on desc",
                    subheadings = subheadings,
                    )

    # Maintain consistent order for multiple assistance offers
    # on the same day (by enforcing created_on as secondary order criterion)
    field = table.date
    field.represent.dt_orderby = offer_date_dt_orderby

    # If the offer is created on direct offer tab, then pre-set the need_id and lock it
    need_id = r.get_vars.get("need_id")
    if need_id and need_id.isdigit():
        field = table.need_id
        field.default = int(need_id)
        if field.default:
            field.writable = False

    # Custom callback to notify pending direct offers upon approval
    s3db.add_custom_callback("br_assistance_offer",
                             "onaccept",
                             offer_onaccept,
                             )

# -------------------------------------------------------------------------
def configure_offer_details(table):
    """
        Configure offer details for more compact list_fields
            - better usability on mobile devices
    """

    s3db = current.s3db

    from core import s3_fieldmethod
    from ..helpers import OfferDetails

    table.place = s3_fieldmethod("place",
                                 OfferDetails.place,
                                 represent = OfferDetails.place_represent,
                                 )
    table.contact = s3_fieldmethod("contact",
                                   OfferDetails.contact,
                                   represent = OfferDetails.contact_represent,
                                   )
    s3db.configure("br_assistance_offer",
                   extra_fields = ["location_id$L3",
                                   "location_id$L2",
                                   "location_id$L1",
                                   "contact_name",
                                   "contact_phone",
                                   "contact_email",
                                   ],
                   )

# -------------------------------------------------------------------------
def br_assistance_offer_controller(**attr):

    T = current.T
    db = current.db
    auth = current.auth
    s3db = current.s3db
    settings = current.deployment_settings

    s3 = current.response.s3

    is_event_manager = auth.s3_has_role("EVENT_MANAGER")
    is_relief_provider = auth.s3_has_role("RELIEF_PROVIDER")
    org_role = is_event_manager or is_relief_provider

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        get_vars = r.get_vars

        resource = r.resource
        table = resource.table

        from ..helpers import get_current_events, get_managed_orgs

        if is_relief_provider:
            providers = get_managed_orgs("RELIEF_PROVIDER")
        elif auth.user:
            providers = [auth.user.pe_id]
        else:
            providers = []

        # Check perspective
        viewing = r.viewing
        if viewing and viewing[0] == "br_case_activity":
            case_activity_id = viewing[1]
            direct_offers, mine = True, False
            # Must have update-permission for the case activity viewing
            if not auth.s3_has_permission("update", "br_case_activity",
                                          record_id = case_activity_id,
                                          c = "br",
                                          f = "case_activity",
                                          ):
                r.unauthorised()
            # Filter to the context of the case activity viewing
            query = FS("direct_offer.case_activity_id") == case_activity_id
            resource.add_filter(query)
        else:
            direct_offers = False
            mine = r.function == "assistance_offer"

        if mine:
            # Adjust list title, allow last update info
            title_list = T("Our Relief Offers") if org_role else \
                         T("My Relief Offers")
            s3.hide_last_update = False

            # Filter for offers of current user
            if len(providers) == 1:
                query = (FS("pe_id") == providers[0])
            else:
                query = (FS("pe_id").belongs([]))
            resource.add_filter(query)

            # Make editable
            resource.configure(insertable = True,
                               editable = True,
                               deletable = True,
                               )
        else:
            # Adjust list title, hide last update info
            title_list = T("Direct Offers") if direct_offers else \
                         T("Current Relief Offers")
            s3.hide_last_update = not is_event_manager

            # Restrict data formats
            restrict_data_formats(r)

            # Enable catalog layers in map views
            settings.gis.widget_catalogue_layers = True

            # URL pre-filter options
            match = not direct_offers and get_vars.get("match") == "1"
            show_pending = show_blocked = show_all = False
            if is_event_manager and not direct_offers:
                if get_vars.get("pending") == "1":
                    show_pending = True
                    title_list = T("Pending Approvals")
                elif get_vars.get("blocked") == "1":
                    show_blocked = True
                    title_list = T("Blocked Entries")
                elif get_vars.get("all") == "1":
                    show_all = True
                    title_list = T("All Offers")
            elif match:
                title_list = T("Matching Offers")

            # Make read-only
            writable = is_event_manager and not direct_offers
            resource.configure(insertable = False,
                               editable = writable,
                               deletable = writable,
                               )

        s3.crud_strings["br_assistance_offer"]["title_list"] = title_list

        from core import LocationFilter, \
                         S3LocationSelector, \
                         OptionsFilter, \
                         TextFilter, \
                         get_filter_options

        if not r.component:

            # Default Event
            field = table.event_id
            events = get_current_events(r.record)
            if events:
                dbset = db(s3db.event_event.id.belongs(events))
                field.requires = IS_ONE_OF(dbset, "event_event.id",
                                           field.represent,
                                           )
                field.default = events[0]
                field.writable = len(events) != 1

            # Default Provider
            field = table.pe_id
            field.readable = not mine or org_role
            if len(providers) == 1:
                field.default = providers[0]
                field.writable = False
            elif providers:
                etable = s3db.pr_pentity
                dbset = db(etable.pe_id.belongs(providers))
                field.requires = IS_ONE_OF(dbset, "pr_pentity.pe_id",
                                           field.represent,
                                           )
                field.writable = mine
            elif is_event_manager:
                field.writable = False

            # Address mandatory, Lx-only
            field = table.location_id
            requires = field.requires
            if isinstance(requires, IS_EMPTY_OR):
                field.requires = requires.other
            field.widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                              required_levels = ("L1", "L2", "L3"),
                                              filter_lx = settings.get_custom("regional"),
                                              show_address = False,
                                              show_postcode = False,
                                              show_map = False,
                                              )

            # TODO End date mandatory
            # => default to 4 weeks from now

            if not is_event_manager:
                # Need type is mandatory
                field = table.need_id
                requires = field.requires
                if isinstance(requires, IS_EMPTY_OR):
                    field.requires = requires.other

                # At least phone number is required
                # - TODO default from user if CITIZEN
                field = table.contact_phone
                requires = field.requires
                if isinstance(requires, IS_EMPTY_OR):
                    field.requires = requires.other

            # Status only writable for EVENT_MANAGER
            field = table.status
            field.writable = is_event_manager

            if not r.record:

                # Filters
                if direct_offers:
                    filter_widgets = None
                else:
                    from ..helpers import OfferAvailabilityFilter, \
                                          get_offer_filters

                    # Apply availability filter
                    OfferAvailabilityFilter.apply_filter(resource, get_vars)

                    # Filter for matching offers?
                    if not mine and match:
                        filters = get_offer_filters()
                        if filters:
                            resource.add_filter(filters)

                    filter_widgets = [
                        TextFilter(["name",
                                    "refno",
                                    "description",
                                    "website",
                                    ],
                                   label = T("Search"),
                                   ),
                        OptionsFilter("need_id",
                                      options = lambda: \
                                                get_filter_options("br_need",
                                                                   translate = True,
                                                                   ),
                                        ),
                        OptionsFilter("chargeable",
                                      cols = 2,
                                      hidden = mine,
                                      ),
                        ]

                    if not mine:
                        # Add location filter for all-offers perspective
                        filter_widgets.append(
                            LocationFilter("location_id",
                                           levels = ("L2", "L3"),
                                           ))

                    if mine or is_event_manager:
                        # Add filter for availability / status
                        availability_opts = s3db.br_assistance_offer_availability
                        status_opts = s3db.br_assistance_offer_status
                        filter_widgets.extend([
                            OptionsFilter("availability",
                                          options = OrderedDict(availability_opts),
                                          hidden = True,
                                          sort = False,
                                          cols = 3,
                                          ),
                            OptionsFilter("status",
                                          options = OrderedDict(status_opts),
                                          hidden = True,
                                          sort = False,
                                          cols = 3,
                                          ),
                            ])
                    if not mine:
                        # Add availability date range filter for all-offers perspective
                        filter_widgets.append(
                            OfferAvailabilityFilter("date",
                                                    label = T("Available"),
                                                    hidden = True,
                                                    ))

                # Visibility Filter
                if mine:
                    # Show all accessible
                    vquery = None
                else:
                    # Filter out unavailable, unapproved and past offers
                    today = current.request.utcnow.date()
                    vquery = (FS("availability") == "AVL") & \
                             (FS("status") == "APR") & \
                             ((FS("end_date") == None) | (FS("end_date") >= today))
                    # Event manager can override this with URL options
                    if is_event_manager and not direct_offers:
                        if show_pending:
                            vquery = (FS("status") == "NEW")
                        elif show_blocked:
                            vquery = (FS("status") == "BLC")
                        elif show_all:
                            vquery = None
                if vquery:
                    resource.add_filter(vquery)

                # List fields
                if not mine:
                    # All or direct offers
                    configure_offer_details(table)
                    list_fields = ["need_id",
                                   "name",
                                   (T("Place"), "place"),
                                   "pe_id",
                                   (T("Contact"), "contact"),
                                   "refno",
                                   "description",
                                   "chargeable",
                                   "website",
                                   "availability",
                                   "date",
                                   "end_date",
                                   #"status"
                                   ]
                    if is_event_manager:
                        list_fields.append("status")
                else:
                    # My/our offers
                    list_fields = ["need_id",
                                   "name",
                                   #"pe_id",
                                   "location_id$L3",
                                   #"location_id$L2",
                                   "location_id$L1",
                                   "refno",
                                   "chargeable",
                                   "availability",
                                   "date",
                                   "end_date",
                                   "status"
                                   ]
                    if org_role:
                        list_fields.insert(2, "pe_id")

                resource.configure(filter_widgets = filter_widgets,
                                   list_fields = list_fields,
                                   )

                # Report options
                if r.method == "report":
                    facts = ((T("Number of Relief Offers"), "count(id)"),
                             (T("Number of Providers"), "count(pe_id)"),
                             )
                    axes = ["need_id",
                            "location_id$L4",
                            "location_id$L3",
                            "location_id$L2",
                            "location_id$L1",
                            "availability",
                            "chargeable",
                            (T("Provider Type"), "pe_id$instance_type"),
                            ]
                    default_rows = "need_id"
                    default_cols = "location_id$L3"

                    report_options = {
                        "rows": axes,
                        "cols": axes,
                        "fact": facts,
                        "defaults": {"rows": default_rows,
                                     "cols": default_cols,
                                     "fact": "count(id)",
                                     "totals": True,
                                     },
                        }
                    resource.configure(report_options=report_options)

        elif r.component_name == "direct_offer":

            # Perspective to manage direct-offer links for an offer

            # List fields
            list_fields = ["case_activity_id",
                           "case_activity_id$date",
                           "case_activity_id$location_id$L3",
                           "case_activity_id$location_id$L1",
                           (T("Need Status"), "case_activity_id$status_id"),
                           (T("Approval"), "offer_id$status"),
                           ]

            r.component.configure(list_fields = list_fields,
                                  # Can only read or delete here
                                  insertable = False,
                                  editable = False,
                                  )

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import rlpcm_br_rheader
    attr["rheader"] = rlpcm_br_rheader

    return attr


# -------------------------------------------------------------------------
def activity_date_dt_orderby(field, direction, orderby, left_joins):
    """
        When sorting activities by date, use created_on to maintain
        consistent order of multiple activities on the same date
    """

    sorting = {"table": field.tablename,
               "direction": direction,
               }
    orderby.append("%(table)s.date%(direction)s,%(table)s.created_on%(direction)s" % sorting)

# -------------------------------------------------------------------------
def br_case_activity_resource(r, tablename):

    T = current.T
    settings = current.deployment_settings

    # Case file or self-reporting?
    record = r.record
    case_file = r.tablename == "pr_person" and record
    ours = r.function == "case_activity" and \
                         current.auth.s3_has_roles(("RELIEF_PROVIDER", "CASE_MANAGER"))

    s3 = current.response.s3
    crud_strings = s3.crud_strings

    s3db = current.s3db
    table = s3db.br_case_activity

    # Can't change start date, always today
    field = table.date
    field.writable = False
    # Maintain consistent order for multiple activities
    # on the same day (by enforcing created_on as secondary order criterion)
    field.represent.dt_orderby = activity_date_dt_orderby

    # Need type is mandatory
    field = table.need_id
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other

    # Subject is mandatory + limit length
    field = table.subject
    field.label = T("Short Description")
    field.requires = [IS_NOT_EMPTY(), IS_LENGTH(128)]

    # Location is visible
    from ..helpers import get_current_location
    from core import S3LocationSelector

    field = table.location_id
    field.readable = field.writable = True
    field.label = T("Place")
    if case_file:
        # Defaults to beneficiary tracking location
        field.default = get_current_location(record.id)
    else:
        # Default to current user's tracking location
        field.default = get_current_location()
    field.widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                      required_levels = ("L1", "L2", "L3"),
                                      filter_lx = settings.get_custom("regional"),
                                      show_address = False,
                                      show_postcode = False,
                                      show_map = False,
                                      )

    if case_file or ours:
        # Custom form to change field order
        from core import S3SQLCustomForm
        crud_fields = ["person_id",
                       "priority",
                       "date",
                       "need_id",
                       "subject",
                       "need_details",
                       "location_id",
                       "activity_details",
                       "outcome",
                       "status_id",
                       "comments",
                       ]
        s3db.configure("br_case_activity",
                       crud_form = S3SQLCustomForm(*crud_fields),
                       )
        # Subheadings for CRUD form
        subheadings = {"priority": T("Need Details"),
                       "location_id": T("Need Location"),
                       "activity_details": T("Support provided"),
                       "status_id": T("Status"),
                       }
    else:
        # Default form with mods per settings
        # Subheadings for CRUD form
        subheadings = {"date": T("Need Details"),
                       "location_id": T("Need Location"),
                       "status_id": T("Status"),
                       }
    s3db.configure("br_case_activity",
                   subheadings = subheadings,
                   # Default sort order: newest first
                   orderby = "br_case_activity.date desc, br_case_activity.created_on desc",
                   )

    # CRUD Strings
    crud_strings["br_case_activity"] = Storage(
        label_create = T("Report Need"),
        title_display = T("Need Details"),
        title_list = T("Needs"),
        title_report = T("Needs Statistic"),
        title_update = T("Edit Need"),
        label_list_button = T("List Needs"),
        label_delete_button = T("Delete Need"),
        msg_record_created = T("Need added"),
        msg_record_modified = T("Need updated"),
        msg_record_deleted = T("Need deleted"),
        msg_list_empty = T("No Needs currently registered"),
        )

# -------------------------------------------------------------------------
def br_case_activity_controller(**attr):

    T = current.T
    db = current.db
    auth = current.auth
    s3db = current.s3db
    settings = current.deployment_settings

    s3 = current.response.s3

    is_event_manager = auth.s3_has_role("EVENT_MANAGER")
    is_case_manager = auth.s3_has_roles(("RELIEF_PROVIDER", "CASE_MANAGER"))

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource
        table = resource.table

        # Check perspective
        mine = r.function == "case_activity"
        crud_strings = s3.crud_strings["br_case_activity"]
        if mine:
            # Adjust list title, allow last update info
            if is_case_manager:
                crud_strings["title_list"] = T("Our Needs")
            else:
                crud_strings["title_list"] = T("My Needs")
            s3.hide_last_update = False

            # Beneficiary requirements
            field = table.person_id
            field.writable = False
            if is_case_manager:
                # Must add in case-file
                field.readable = True
                insertable = False
            else:
                # Set default beneficiary + hide it
                logged_in_person = auth.s3_logged_in_person()
                field.default = logged_in_person
                field.readable = False
                if not r.record:
                    # Filter to own activities
                    query = FS("person_id") == logged_in_person
                    resource.add_filter(query)
                insertable = True

            # Allow update/delete
            editable = deletable = True
        else:
            # Adjust list title, hide last update info
            crud_strings["title_list"] = T("Current Needs")
            s3.hide_last_update = not is_event_manager

            # Restrict data formats
            restrict_data_formats(r)

            # Enable catalog layers in map views
            settings.gis.widget_catalogue_layers = True

            # Limit to active activities
            today = current.request.utcnow.date()
            query = (FS("status_id$is_closed") == False) & \
                    ((FS("end_date") == None) | (FS("end_date") >= today))
            resource.add_filter(query)

            # Deny create, only event manager can update/delete
            insertable = False
            editable = deletable = is_event_manager

        resource.configure(insertable = insertable,
                           editable = editable,
                           deletable = deletable,
                           )

        if not r.component:

            if not mine or not is_case_manager:
                # Hide irrelevant fields
                for fn in ("person_id", "activity_details", "outcome", "priority"):
                    field = table[fn]
                    field.readable = field.writable = False

            # List fields
            list_fields = ["date",
                           "need_id",
                           "subject",
                           "location_id$L4",
                           "location_id$L3",
                           "location_id$L2",
                           "location_id$L1",
                           #"status_id",
                           ]
            if mine or is_event_manager:
                list_fields.append("status_id")
                if is_case_manager:
                    list_fields[1:1] = ("priority", "person_id")

                    # Represent person_id as link to case file
                    field = table.person_id
                    field.represent = s3db.pr_PersonRepresent(show_link=True)

            # Filters
            from core import DateFilter, \
                             TextFilter, \
                             LocationFilter, \
                             OptionsFilter, \
                             get_filter_options
            filter_widgets = [
                TextFilter(["subject",
                            "need_details",
                            ],
                           label = T("Search"),
                           ),
                OptionsFilter("need_id",
                              options = lambda: \
                                        get_filter_options("br_need",
                                                           translate = True,
                                                           ),
                              ),
                LocationFilter("location_id",
                               label = T("Place"),
                               levels = ("L2", "L3"),
                               ),
                DateFilter("date",
                           hidden = True,
                           ),
                ]
            if mine or is_event_manager:
                filter_widgets.append(
                    OptionsFilter("status_id",
                                  options = lambda: \
                                            get_filter_options("br_case_activity_status",
                                                               translate = True,
                                                               ),
                                  hidden = True,
                                  ))

            resource.configure(filter_widgets = filter_widgets,
                               list_fields = list_fields,
                               )

            # Report options
            if r.method == "report":
                facts = ((T("Number of Need Reports"), "count(id)"),
                         )
                axes = ["need_id",
                        "location_id$L4",
                        "location_id$L3",
                        "location_id$L2",
                        "location_id$L1",
                        "status_id",
                        ]
                default_rows = "need_id"
                default_cols = "location_id$L3"

                report_options = {
                    "rows": axes,
                    "cols": axes,
                    "fact": facts,
                    "defaults": {"rows": default_rows,
                                 "cols": default_cols,
                                 "fact": "count(id)",
                                 "totals": True,
                                 },
                    }
                resource.configure(report_options=report_options)

        elif r.component_name == "direct_offer":

            if r.function != "activities":
                # This perspective is not supported
                r.error(403, current.ERROR.NOT_PERMITTED)

            # Perspective to list and add direct offers

            component = r.component

            # Show only approved offers
            query = (FS("offer_id$status") == "APR")
            component.add_filter(query)

            # List fields
            br_assistance_offer_resource(r, "br_assistance_offer")
            list_fields = ["offer_id",
                           "offer_id$pe_id",
                           "offer_id$location_id$L3",
                           "offer_id$location_id$L1",
                           "offer_id$date",
                           "offer_id$end_date",
                           "offer_id$availability",
                           ]
            component.configure(list_fields = list_fields)

            record = r.record

            # Configure offer_id selector
            ctable = r.component.table
            field = ctable.offer_id

            # Use must be permitted to manage the offer
            aotable = s3db.br_assistance_offer
            query = auth.s3_accessible_query("update", aotable, c="br", f="assistance_offer")
            # Need type must match
            need_id = record.need_id if record else None
            if need_id:
                query &= aotable.need_id == need_id
            # Offer must not be blocked, nor past
            today = r.utcnow.date()
            query &= (aotable.status != "BLC") & \
                        ((aotable.end_date == None) | (aotable.end_date >= today))
            dbset = db(query)
            field.requires = IS_ONE_OF(dbset, "br_assistance_offer.id",
                                       field.represent,
                                       )

            # Add popup link as primary route to create new offer as direct offer
            from s3layouts import S3PopupLink
            popup_vars = {# Request the options via this tab to apply above filters
                          "parent": "activities/%s/direct_offer" % record.id,
                          "child": "offer_id",
                          }
            need_id = record.need_id
            if need_id:
                popup_vars["need_id"] = need_id
            field.comment = S3PopupLink(label = T("Create new Offer"),
                                        c = "br",
                                        f = "assistance_offer",
                                        m = "create",
                                        vars = popup_vars,
                                        )

            # TODO add a CMS intro for selector

            # Cannot create direct offers for my own needs
            insertable = not auth.s3_has_permission("update", "br_case_activity",
                                                    c = "br",
                                                    f = "case_activity",
                                                    record_id = record.id,
                                                    )
            r.component.configure(insertable = insertable,
                                  # Direct offers can not be updated
                                  # => must delete + create new
                                  editable = False,
                                  )

            resource.configure(ignore_master_access = ("direct_offer",),
                               )

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import rlpcm_br_rheader
    attr["rheader"] = rlpcm_br_rheader

    return attr

# -------------------------------------------------------------------------
def direct_offer_create_onaccept(form):
    """
        Custom onaccept-routine for direct offers
            - trigger notifications if offer is already approved
    """

    # Get the record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    T = current.T
    db = current.db
    s3db = current.s3db

    # Get the offer
    table = s3db.br_direct_offer
    aotable = s3db.br_assistance_offer
    join = aotable.on(aotable.id == table.offer_id)

    query = (table.id == record_id)
    row = db(query).select(table.id,
                           table.notify,
                           table.notified_on,
                           aotable.id,
                           aotable.status,
                           aotable.end_date,
                           join = join,
                           limitby = (0, 1),
                           ).first()
    if not row:
        return

    record = row.br_direct_offer
    offer = row.br_assistance_offer

    today = current.request.utcnow.date()

    if offer.status == "NEW":
        current.response.warning = T("Your offer is waiting for approval by the administrator")

    elif offer.status == "APR" and \
         (offer.end_date == None or offer.end_date >= today) and \
         record.notify and \
         record.notified_on == None:

        from ..helpers import notify_direct_offer
        error = notify_direct_offer(record.id)
        if error:
            current.response.error = T("Notification could not be sent: %(error)s") % {"error": error}
        else:
            current.response.information = T("Notification sent")

# -------------------------------------------------------------------------
def br_direct_offer_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.br_direct_offer

    field = table.offer_id
    from core import WithAdvice
    from gluon.sqlhtml import OptionsWidget
    field.widget = WithAdvice(OptionsWidget.widget,
                              text = ("br",
                                      "direct_offer",
                                      "DirectOfferSelectorIntro",
                                      ),
                              )

    # Custom label+represent for case activity
    # - always link to activities-perspective ("Current Needs")
    field = table.case_activity_id
    field.label = T("Need")
    field.represent = s3db.br_CaseActivityRepresent(show_as = "subject",
                                                    show_link = True,
                                                    linkto = URL(c = "br",
                                                                 f = "activities",
                                                                 args = "[id]",
                                                                 extension = "",
                                                                 ),
                                                    )

    # Callback to trigger notification
    s3db.add_custom_callback("br_direct_offer",
                             "onaccept",
                             direct_offer_create_onaccept,
                             method = "create",
                             )


# END =========================================================================
