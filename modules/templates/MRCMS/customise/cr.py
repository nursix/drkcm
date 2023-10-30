"""
    CR module customisations for MRCMS

    License: MIT
"""

from gluon import current, URL, DIV, H4, P, TAG

from core import S3CRUD, FS, IS_ONE_OF, \
                 LocationSelector, PresenceRegistration, S3SQLCustomForm, \
                 get_form_record_id, s3_fieldmethod

# -------------------------------------------------------------------------
def client_site_status(person_id, site_id, site_type, case_status):
    """
        Check whether a person to register at a site is a resident,
        whether they are permitted to enter/leave premises, and whether
        there are advice/instructions to the reception staff

        Args:
            person_id: the person ID
            site_id: the site ID
            site_type: the site type (tablename)
            case_status: the current case status (Row)
        Returns:
            dict, see person_site_status
    """

    T = current.T

    db = current.db
    s3db = current.s3db

    result = {"valid": False,
              "allowed_in": False,
              "allowed_out": False,
              }

    if case_status.is_closed:
        result["error"] = T("Closed case")
        return result

    if site_type == "cr_shelter":
        # Check for a shelter registration
        stable = s3db.cr_shelter
        rtable = s3db.cr_shelter_registration
        query = (stable.site_id == site_id) & \
                (stable.id == rtable.shelter_id) & \
                (rtable.person_id == person_id) & \
                (rtable.deleted != True)
        registration = db(query).select(rtable.id,
                                        rtable.registration_status,
                                        limitby=(0, 1),
                                        ).first()
        if not registration or registration.registration_status == 3:
            # No registration with this site, or checked-out
            return result

    result["valid"] = True

    # Get the current presence status at the site
    from core import SitePresence
    presence = SitePresence.status(person_id, site_id)[0]

    allowed_in = True
    allowed_out = True

    # Check if we have any case flag to deny passage and/or to show instructions
    ftable = s3db.dvr_case_flag
    ltable = s3db.dvr_case_flag_case
    query = (ltable.person_id == person_id) & \
            (ltable.deleted != True) & \
            (ftable.id == ltable.flag_id) & \
            (ftable.deleted != True)
    flags = db(query).select(ftable.name,
                             ftable.deny_check_in,
                             ftable.deny_check_out,
                             ftable.advise_at_check_in,
                             ftable.advise_at_check_out,
                             ftable.advise_at_id_check,
                             ftable.instructions,
                             )
    info = []
    append = info.append
    for flag in flags:
        # Deny IN/OUT?
        if flag.deny_check_in:
            allowed_in = False
        if flag.deny_check_out:
            allowed_out = False

        # Show flag instructions?
        if flag.advise_at_id_check:
            advise = True
        elif presence == "IN":
            advise = flag.advise_at_check_out
        elif presence == "OUT":
            advise = flag.advise_at_check_in
        else:
            advise = flag.advise_at_check_in or flag.advise_at_check_out
        if advise:
            instructions = flag.instructions
            if instructions is not None:
                instructions = instructions.strip()
            if not instructions:
                instructions = current.T("No instructions for this flag")
            append(DIV(H4(T(flag.name)),
                       P(instructions),
                       _class="checkpoint-instructions",
                       ))
    if info:
        result["info"] = DIV(_class="checkpoint-advise", *info)

    result["allowed_in"] = allowed_in
    result["allowed_out"] = allowed_out

    return result

# -------------------------------------------------------------------------
def staff_site_status(person_id, site_id, organisation_ids):
    """
        Check whether a person to register at a site is a staff member

        Args:
            person_id: the person ID
            organisation_ids: IDs of all valid staff organisations for the site
        Returns:
            dict, see person_site_status
    """

    db = current.db
    s3db = current.s3db

    htable = s3db.hrm_human_resource
    query = (htable.person_id == person_id) & \
            (htable.organisation_id.belongs(organisation_ids)) & \
            (htable.status == 1) & \
            (htable.deleted == False)
    staff = db(query).select(htable.id, limitby=(0, 1)).first()

    valid = True if staff else False

    result = {"valid": valid,
              "allowed_in": valid,
              "allowed_out": valid,
              }
    return result

# -------------------------------------------------------------------------
def person_site_status(site_id, person):
    """
        Determine the current status of a person with regard to
        entering/leaving a site

        Args:
            site_id: the site ID
            person: the person record
        Returns:
            dict {"valid": Person can be registered in/out at the site,
                  "error": Error message for the above,
                  "allowed_in": Person is allowed to enter the site,
                  "allowed_out": Person is allowed to leave the site,
                  "info": Instructions for reception staff,
                  }
    """

    T = current.T

    db = current.db
    s3db = current.s3db

    result = {"valid": False,
              "allowed_in": False,
              "allowed_out": False,
              }
    person_id = person.id

    # Get the site type and managing organisation(s)
    otable = s3db.org_organisation
    stable = s3db.org_site
    join = otable.on(otable.id == stable.organisation_id)
    row = db(stable.site_id == site_id).select(stable.instance_type,
                                               otable.id,
                                               otable.root_organisation,
                                               otable.pe_id,
                                               join = join,
                                               limitby = (0, 1),
                                               ).first()
    if not row:
        result["error"] = T("Invalid site")
        return result

    site = row.org_site
    organisation = row.org_organisation

    organisation_ids = [organisation.id]

    root_org = organisation.root_organisation
    if root_org and root_org != organisation.id:
        # Include all parent organisations
        pe_ids = s3db.pr_get_ancestors(organisation.pe_id)
        rows = db((otable.pe_id.belongs(pe_ids))).select(otable.id)
        organisation_ids += [row.id for row in rows]

    # Check for case
    ctable = s3db.dvr_case
    cstable = s3db.dvr_case_status
    query = (ctable.person_id == person_id) & \
            (ctable.organisation_id.belongs(organisation_ids)) & \
            (cstable.id == ctable.status_id)
    case = db(query).select(ctable.id,
                            cstable.is_closed,
                            limitby = (0, 1),
                            ).first()

    if case and case.dvr_case.id:
        # Is a client
        result.update(client_site_status(person_id,
                                         site_id,
                                         site.instance_type,
                                         case.dvr_case_status,
                                         ))
        if not result["valid"] and not result.get("error"):
            result["error"] = T("Not currently a resident")
    else:
        # May be a staff member
        result.update(staff_site_status(person_id,
                                        site_id,
                                        organisation_ids,
                                        ))
        if not result["valid"] and not result.get("error"):
            # Neither resident nor active staff member, so invalid ID
            result["error"] = T("Invalid ID")

    if result["valid"] and not result.get("error"):

        auth = current.auth
        instructions = None

        uperson_id = auth.s3_logged_in_person()
        if uperson_id == person_id:
            if not auth.s3_has_roles(("ORG_ADMIN", "SECURITY")):
                result["allowed_in"] = None
                instructions = T("Self-registration not permitted. Please register with authorized staff at the site.")
        elif not PresenceRegistration.present(site_id):
            result["allowed_in"] = result["allowed_out"] = None
            instructions = T("You must be reported as present at the site yourself in order to register the presence of others.")

        if instructions:
            result["info"] = DIV(DIV(P(instructions),
                                     _class="checkpoint-instructions",
                                     ),
                                 _class="checkpoint-advise",
                                 )
    return result

# -------------------------------------------------------------------------
def on_site_presence_event(site_id, person_id):
    """
        Update last_seen_on in case file when a site presence event
        is registered (if the person has a case file)

        Args:
            site_id: the site_id of the shelter
            person_id: the person_id to check-in
    """

    db = current.db
    s3db = current.s3db

    ctable = s3db.dvr_case
    query = (ctable.person_id == person_id) & \
            (ctable.deleted == False)
    if db(query).select(ctable.id, limitby=(0, 1)).first():
        current.s3db.dvr_update_last_seen(person_id)

# -------------------------------------------------------------------------
def cr_shelter_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.cr_shelter

    # Configure fields
    field = table.location_id
    field.widget = LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                    required_levels = ("L1", "L2", "L3"),
                                    show_address = True,
                                    show_postcode = True,
                                    address_required = True,
                                    postcode_required = True,
                                    show_map = False,
                                    )
    field.represent = s3db.gis_LocationRepresent(show_link = False)

    field = table.obsolete
    field.label = T("Defunct")

    # Custom form
    crud_fields = ["name",
                   "organisation_id",
                   "shelter_type_id",
                   "status",
                   "location_id",
                   "capacity",
                   "blocked_capacity",
                   "population",
                   "available_capacity",
                   "comments",
                   "obsolete"
                   ]

    subheadings = {"name": T("Shelter"),
                   "location_id": T("Location"),
                   "capacity": T("Capacity"),
                   "comments": T("Other"),
                   }

    # Table configuration
    s3db.configure("cr_shelter",
                   crud_form = S3SQLCustomForm(*crud_fields),
                   subheadings = subheadings,
                   realm_components = ("shelter_unit",
                                       ),
                   create_next = URL(c ="cr",
                                     f ="shelter",
                                     args = ["[id]", "shelter_unit"],
                                     ),
                   )

    # Shelter overview method
    from ..shelter import ShelterOverview
    s3db.set_method("cr_shelter",
                    method = "overview",
                    action = ShelterOverview,
                    )

    from ..presence import PresenceList
    s3db.set_method("cr_shelter",
                    method = "presence_list",
                    action = PresenceList,
                    )

# -------------------------------------------------------------------------
def cr_shelter_controller(**attr):

    T = current.T
    s3 = current.response.s3

    settings = current.deployment_settings

    # Custom prep
    standard_prep = s3.prep
    def custom_prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        if r.method == "presence":
            # Configure presence event callbacks
            current.s3db.configure("cr_shelter",
                                   site_presence_in = on_site_presence_event,
                                   site_presence_out = on_site_presence_event,
                                   site_presence_seen = on_site_presence_event,
                                   site_presence_status = person_site_status,
                                   )

        else:
            if r.record and r.method == "profile":
                # Add PoI layer to the Map
                s3db = current.s3db
                ftable = s3db.gis_layer_feature
                query = (ftable.controller == "gis") & \
                        (ftable.function == "poi")
                layer = current.db(query).select(ftable.layer_id,
                                                 limitby = (0, 1)
                                                 ).first()
                try:
                    layer_id = layer.layer_id
                except AttributeError:
                    # No suitable prepop found
                    pass
                else:
                    pois = {"active": True,
                            "layer_id": layer_id,
                            "name": current.T("Buildings"),
                            "id": "profile-header-%s-%s" % ("gis_poi", r.id),
                            }
                    profile_layers = s3db.get_config("cr_shelter", "profile_layers")
                    profile_layers += (pois,)
                    s3db.configure("cr_shelter",
                                   profile_layers = profile_layers,
                                   )
            #else:
                #has_role = current.auth.s3_has_role
                #if has_role("SECURITY") and not has_role("ADMIN"):
                #    # Security can access nothing in cr/shelter except
                #    # Dashboard and Check-in/out UI
                #    current.auth.permission.fail()

            if r.interactive:
                # TODO should also be deletable while there are no shelter registrations
                #      => probably the individual record only, not from list
                r.resource.configure(filter_widgets = None,
                                     deletable = False,
                                     )

        if not r.component:
            # Open shelter basic details in read mode
            settings.ui.open_read_first = True

        #elif r.component_name == "shelter_unit":
            ## Expose "transitory" flag for housing units
            #utable = current.s3db.cr_shelter_unit
            #field = utable.transitory
            #field.readable = field.writable = True

            ## Custom list fields
            #list_fields = [(T("Name"), "name"),
                           #"transitory",
                           #"capacity",
                           #"population",
                           #"blocked_capacity",
                           #"available_capacity",
                           #]
            #r.component.configure(list_fields=list_fields)

        return result
    s3.prep = custom_prep

    # Custom postp
    standard_postp = s3.postp
    def custom_postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        # Hide side menu and rheader for presence registration
        if r.method == "presence":
            current.menu.options = None
            if isinstance(output, dict):
                output["rheader"] = ""
            return output

        # Custom view for shelter inspection
        if r.method == "inspection":
            from core import CustomController
            CustomController._view("MRCMS", "shelter_inspection.html")
            return output

        record = r.record

        # Add presence registration button, if permitted
        if record and not r.component and \
            PresenceRegistration.permitted("cr_shelter", record) and \
            isinstance(output, dict) and "buttons" in output:

            buttons = output["buttons"]

            # Add a "Presence Registration"-button
            presence_url = URL(c="cr", f="shelter", args=[record.id, "presence"])
            presence_btn = S3CRUD.crud_button(T("Presence Registration"), _href=presence_url)

            delete_btn = buttons.get("delete_btn")
            buttons["delete_btn"] = TAG[""](presence_btn, delete_btn) \
                                    if delete_btn else presence_btn

        return output
    s3.postp = custom_postp

    from ..rheaders import cr_rheader
    attr = dict(attr)
    attr["rheader"] = cr_rheader

    return attr

# -------------------------------------------------------------------------
def cr_shelter_unit_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.cr_shelter_unit

    field = table.location_id
    field.readable = field.writable = False

    field = table.transitory
    field.label = T("Staging Area")
    field.readable = field.writable = True

    table.occupancy = s3_fieldmethod("occupancy",
                                     shelter_unit_occupancy,
                                     represent = occupancy_represent,
                                     )

    list_fields = [(T("Name"), "name"),
                   "transitory",
                   "status",
                   "capacity",
                   "population",
                   "blocked_capacity",
                   "available_capacity",
                   (T("Occupancy %"), "occupancy"),
                   ]
    s3db.configure("cr_shelter_unit",
                   list_fields = list_fields,
                   extra_fields = ("capacity", "blocked_capacity", "popuplation"),
                   )

# -------------------------------------------------------------------------
def shelter_unit_occupancy(row):
    """
        Returns the occupancy of a housing unit in %, field method

        Args:
            the shelter unit Row (capacity, blocked_capacity, popuplation)

        Returns:
            integer
    """

    if hasattr(row, "cr_shelter_unit"):
        row = row.cr_shelter_unit
    try:
        total_capacity = row.capacity
        blocked_capacity = row.blocked_capacity
        population = row.population
    except AttributeError:
        return None

    if not total_capacity:
        return None
    if blocked_capacity is None:
        blocked_capacity = 0
    if population is None:
        population = 0

    # Blocked capacity cannot exceed total capacity
    blocked_capacity = min(total_capacity, blocked_capacity)

    # Available capacity
    available = total_capacity - blocked_capacity
    if available < population:
        # Population over available capacity:
        # - ignore any blocked capacity that is actually occupied
        available = min(total_capacity, population)

    if not available:
        # A unit with no available capacity is full if, and only if,
        # it is occupied - otherwise it is (in fact) empty
        return 100 if population else 0
    else:
        # Compute occupancy rate, use floor-rounding so the rate is
        # still indicative of even small free capacity (=show 100%
        # only once it is actually reached, not by rounding)
        return (population * 100 // available)

# -------------------------------------------------------------------------
def occupancy_represent(value):
    """
        Represents occupancy% as progress bar

        Args:
            value - the occupancy in % (integer >= 0)
        Returns:
            DIV.occupancy-bar
    """

    if value is None:
        return DIV("?", _class="occupancy-bar")
    elif not value:
        value = 0
        css_class = "occupancy-0"
    else:
        reprval = (value - 1) // 10 * 10 + 10
        if reprval > 100:
            css_class = "occupancy-exc"
        else:
            css_class = "occupancy-%s" % reprval

    return DIV("%s%%" % value,
               DIV(_class="occupancy %s" % css_class),
               _class="occupancy-bar",
               )

# -------------------------------------------------------------------------
def cr_shelter_unit_controller(**attr):

    db = current.db
    s3db = current.s3db

    s3 = current.response.s3

    standard_prep = s3.prep
    def custom_prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        if not r.record and r.representation == "json":
            # Shelter unit selector => return only available units
            status_query = (FS("status") == 1)

            # Include current unit, if known
            person_id = str(r.get_vars.get("person"))
            if person_id.isdigit():
                rtable = s3db.cr_shelter_registration
                query = (rtable.person_id == person_id) & \
                        (rtable.deleted == False)
                reg = db(query).select(rtable.shelter_unit_id,
                                        limitby = (0, 1),
                                        orderby = ~rtable.id,
                                        ).first()
                current_unit = reg.shelter_unit_id
            else:
                current_unit = None

            if current_unit:
                status_query |= (FS("id") == current_unit)

            r.resource.add_filter(status_query)

        return result
    s3.prep = custom_prep

    return attr

# -------------------------------------------------------------------------
def cr_shelter_registration_onaccept(form):
    """
        Onaccept of shelter registration
            - when checked-out, expire all system-generated ID cards
    """

    db = current.db
    s3db = current.s3db

    record_id = get_form_record_id(form)
    if not record_id:
        return

    table = s3db.cr_shelter_registration
    record = db(table.id == record_id).select(table.id,
                                              table.person_id,
                                              table.registration_status,
                                              limitby = (0, 1),
                                              ).first()
    if not record:
        return

    if record.registration_status == 3:
        from ..idcards import IDCard
        IDCard(record.person_id).auto_expire()

# -------------------------------------------------------------------------
def cr_shelter_registration_resource(r, tablename):

    table = current.s3db.cr_shelter_registration
    field = table.shelter_unit_id

    if r.controller == "cr":
        # Filter to available housing units
        from gluon import IS_EMPTY_OR
        field.requires = IS_EMPTY_OR(IS_ONE_OF(current.db, "cr_shelter_unit.id",
                                               field.represent,
                                               filterby = "status",
                                               filter_opts = (1,),
                                               orderby = "shelter_id",
                                               ))

    # Custom-callback to trigger status-dependent actions
    current.s3db.add_custom_callback("cr_shelter_registration",
                                     "onaccept",
                                     cr_shelter_registration_onaccept,
                                     )

# -------------------------------------------------------------------------
def cr_shelter_registration_controller(**attr):
    """
        Shelter Registration controller is just used
        by the Quartiermanager role.
    """

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def custom_prep(r):
        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        if r.method == "assign":

            # TODO replace with default_case_shelter, using viewing
            from ..helpers import get_default_shelter

            # Prep runs before split into create/update (Create should never happen in Village)
            table = r.table
            shelter_id = get_default_shelter()
            if shelter_id:
                # Only 1 Shelter
                f = table.shelter_id
                f.default = shelter_id
                f.writable = False # f.readable kept as True for cr_shelter_registration_onvalidation
                f.comment = None

            # Only edit for this Person
            f = table.person_id
            f.default = r.get_vars["person_id"]
            f.writable = False
            f.comment = None
            # Registration status hidden
            f = table.registration_status
            f.readable = False
            f.writable = False
            # Check-in dates hidden
            f = table.check_in_date
            f.readable = False
            f.writable = False
            f = table.check_out_date
            f.readable = False
            f.writable = False

            # Go back to the list of residents after assigning
            current.s3db.configure("cr_shelter_registration",
                                   create_next = URL(c="dvr", f="person"),
                                   update_next = URL(c="dvr", f="person"),
                                   )

        return result
    s3.prep = custom_prep

    return attr

# END =========================================================================
