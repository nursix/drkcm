"""
    PR module customisations for MRCMS

    License: MIT
"""

import datetime

from gluon import current, URL, A, SPAN, TAG
from gluon.storage import Storage

from core import FS, IS_ONE_OF, s3_str

# Limit after which a checked-out resident is reported overdue (days)
ABSENCE_LIMIT = 5

# =============================================================================
def mrcms_absence(row):
    # TODO update for org_site_presence_event
    # TODO referring to the site where currently registered as checked-in
    """
        Field method to display duration of absence in
        dvr/person list view and rheader

        Args:
            row: the Row
    """

    if hasattr(row, "cr_shelter_registration"):
        registration = row.cr_shelter_registration
    else:
        registration = None

    result = current.messages["NONE"]

    if registration is None or \
       not hasattr(registration, "registration_status") or \
       not hasattr(registration, "check_out_date"):
        # must reload
        db = current.db
        s3db = current.s3db

        person = row.pr_person if hasattr(row, "pr_person") else row
        person_id = person.id
        if not person_id:
            return result
        table = s3db.cr_shelter_registration
        query = (table.person_id == person_id) & \
                (table.deleted != True)
        registration = db(query).select(table.registration_status,
                                        table.check_out_date,
                                        limitby = (0, 1),
                                        ).first()

    if registration and \
       registration.registration_status == 3:

        T = current.T

        check_out_date = registration.check_out_date
        if check_out_date:

            delta = (current.request.utcnow - check_out_date).total_seconds()
            if delta < 0:
                delta = 0
            days = int(delta / 86400)

            if days < 1:
                result = "<1 %s" % T("Day")
            elif days == 1:
                result = "1 %s" % T("Day")
            else:
                result = "%s %s" % (days, T("Days"))

            if days >= ABSENCE_LIMIT:
                result = SPAN(result, _class="overdue")

        else:
            result = SPAN(T("Date unknown"), _class="overdue")

    return result

# -------------------------------------------------------------------------
def event_overdue(code, interval):
    """
        Get cases (person_ids) for which a certain event is overdue

        Args:
            code: the event code
            interval: the interval in days
    """
    # TODO refactor with event class instead of event code

    db = current.db
    s3db = current.s3db

    ttable = s3db.dvr_case_event_type
    ctable = s3db.dvr_case
    stable = s3db.dvr_case_status
    etable = s3db.dvr_case_event

    # Get event type ID
    if code[-1] == "*":
        # Prefix
        query = (ttable.code.like("%s%%" % code[:-1]))
        limitby = None
    else:
        query = (ttable.code == code)
        limitby = (0, 1)
    query &= (ttable.deleted == False)

    rows = db(query).select(ttable.id, limitby=limitby)
    if not rows:
        # No such event type
        return set()
    elif limitby:
        type_query = (etable.type_id == rows.first().id)
    else:
        type_query = (etable.type_id.belongs(set(row.id for row in rows)))

    # Determine deadline
    now = current.request.utcnow
    then = now - datetime.timedelta(days=interval)

    # Check only open cases
    join = stable.on((stable.id == ctable.status_id) & \
                     (stable.is_closed == False))

    # Join only events after the deadline
    left = etable.on((etable.person_id == ctable.person_id) & \
                     type_query & \
                     (etable.date != None) & \
                     (etable.date >= then) & \
                     (etable.deleted == False))

    # ...and then select the rows which don't have any
    query = (ctable.archived == False) & \
            (ctable.date < then.date()) & \
            (ctable.deleted == False)
    rows = db(query).select(ctable.person_id,
                            left = left,
                            join = join,
                            groupby = ctable.person_id,
                            having = (etable.date.max() == None),
                            )
    return set(row.person_id for row in rows)

# -------------------------------------------------------------------------
def pr_person_resource(r, tablename):

    s3db = current.s3db
    auth = current.auth

    has_permission = auth.s3_has_permission

    # Users who can not register new residents also have
    # only limited write-access to basic details of residents
    if r.controller == "dvr" and not has_permission("create", "pr_person"):

        # Can not write any fields in main person record
        # (fields in components may still be writable, though)
        ptable = s3db.pr_person
        for field in ptable:
            field.writable = False

        # Can not add or modify contact or identity information
        for tn in ("pr_contact", "pr_identity", "pr_image"):
            s3db.configure(tn,
                           insertable = False,
                           editable = False,
                           deletable = False,
                           )

        # Can not update shelter registration (except housing unit)
        rtable = s3db.cr_shelter_registration
        for field in rtable:
            if field.name != "shelter_unit_id":
                field.writable = False

    # Do not include acronym in Case-Org Representation
    table = s3db.dvr_case
    field = table.organisation_id
    field.represent = s3db.org_OrganisationRepresent(parent=False, acronym=False)

    # Configure components to inherit realm_entity from the person
    # record upon forced realm update
    s3db.configure("pr_person",
                   realm_components = ("case_activity",
                                       "case_details",
                                       "case_language",
                                       "case_note",
                                       "residence_status",
                                       "response_action",
                                       "group_membership",
                                       "person_details",
                                       "person_tag",
                                       "shelter_registration",
                                       "shelter_registration_history",
                                       "address",
                                       "contact",
                                       "contact_emergency",
                                       "image",
                                       ),
                   )

# -------------------------------------------------------------------------
def configure_person_tags():
    """
        Configure filtered pr_person_tag components for
        registration numbers:
            - BAMF Registration Number (tag=BAMF)
    """

    current.s3db.add_components("pr_person",
                                pr_person_tag = ({"name": "bamf",
                                                  "joinby": "person_id",
                                                  "filterby": {
                                                    "tag": "BAMF",
                                                    },
                                                  "multiple": False,
                                                  },
                                                 )
                                )

# -------------------------------------------------------------------------
def get_case_organisation(person_id):
    """
        Determines the current case organisation for a person record

        Args:
            person_id: the person record ID
        Returns:
            the organisation record ID, or None if no case could be found
    """

    table = current.s3db.dvr_case
    query = (table.person_id == person_id) & \
            (table.deleted == False)
    row = current.db(query).select(table.organisation_id,
                                   orderby = ~table.id,
                                   limitby = (0, 1),
                                   ).first()

    return row.organisation_id if row else None

# -------------------------------------------------------------------------
def get_available_shelters(organisation_id, person_id=None):
    """
        The available shelters of the case organisation, to configure
        inline shelter registration in case form

        Args:
            organisation_id: the ID of the case organisation
            person_id: the person_id of the client

        Returns:
            list of shelter IDs

        Note:
            - includes the current shelter where the client is registered,
              even if it is closed
    """

    db = current.db
    s3db = current.s3db

    # Get the current shelter registration for person_id
    if person_id:
        rtable = s3db.cr_shelter_registration
        query = (rtable.person_id == person_id) & \
                (rtable.deleted == False)
        reg = db(query).select(rtable.shelter_id,
                               limitby = (0, 1),
                               orderby = ~rtable.id,
                               ).first()
        current_shelter = reg.shelter_id
    else:
        current_shelter = None

    stable = s3db.cr_shelter
    status_query = (stable.status == 2)
    if current_shelter:
        status_query |= (stable.id == current_shelter)

    query = (stable.organisation_id == organisation_id) & \
            status_query & \
            (stable.deleted == False)
    rows = db(query).select(stable.id)
    shelters = [row.id for row in rows]

    return shelters

# -------------------------------------------------------------------------
def configure_inline_shelter_registration(component, shelters, person_id=None):
    """
        Configure inline shelter registration in case form

        Args:
            component: the shelter registration component (aliased table)
            shelters: the available shelters of the case organisation (if
                      case organisation is known)
            person_id: the person_id of the client (if known)

        Returns:
            boolean whether to show the shelter registration inline
    """

    db = current.db
    s3db = current.s3db

    if shelters:
        rtable = component.table

        default_shelter = shelters[0] if len(shelters) == 1 else None

        # Configure shelter ID
        field = rtable.shelter_id
        field.default = default_shelter
        field.writable = False if default_shelter else True
        field.comment = None
        field.widget = None

        if len(shelters) > 1:
            # Configure dynamic options filter for shelter unit
            script = '''
$.filterOptionsS3({
 'trigger':'sub_shelter_registration_shelter_id',
 'target':'sub_shelter_registration_shelter_unit_id',
 'lookupResource':'shelter_unit',
 'lookupPrefix':'cr',
 'lookupKey':'shelter_id',
 'lookupURL':S3.Ap.concat('/cr/shelter_unit.json?person=%s&shelter_unit.shelter_id=')
})''' % person_id
            #cr/shelter_unit.json?shelter_unit.shelter_id=1&represent=1
            current.response.s3.jquery_ready.append(script)
        else:
            # Statically filter shelter units to default shelter
            from gluon import IS_EMPTY_OR

            # Get current registration unit
            if person_id:
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

            field = rtable.shelter_unit_id
            utable = s3db.cr_shelter_unit
            status_query = (utable.status == 1)
            if current_unit:
                status_query |= (utable.id == current_unit)
            dbset = db((utable.shelter_id == default_shelter) & status_query)
            field.requires = IS_EMPTY_OR(IS_ONE_OF(dbset,
                                                   "cr_shelter_unit.id",
                                                   field.represent,
                                                   sort = True,
                                                   ))
        show_inline = True
    else:
        # - hide shelter registration
        show_inline = False

    return show_inline

# -------------------------------------------------------------------------
def configure_case_form(resource,
                        organisation_id=None,
                        shelters=None,
                        person_id=None,
                        privileged=False,
                        cancel=False,
                        ):
    """
        Configure the case form

        Args:
            resource: the pr_person resource
            organisation_id: the case organisation (if known)
            shelters: the available shelters of the case organisation
            person_id: the person_id of the client
            privileged: whether the user has a privileged role
            cancel: whether the case is to be archived, and thence
                    the shelter registration to be canceled

        Notes:
            - Case flags only available if case organisation is known
            - Shelter registration only shown inline if case organisation is known
    """

    from core import S3SQLCustomForm, S3SQLInlineComponent, S3SQLInlineLink

    T = current.T

    # Configure shelter registration component
    component = resource.components.get("shelter_registration")
    show_inline = configure_inline_shelter_registration(component, shelters, person_id)

    if cancel or not show_inline:
        # Ignore registration data in form if the registration
        # is to be cancelled - otherwise a new registration is
        # created by the subform-processing right after
        # dvr_case_onaccept deletes the current one:
        reg_shelter = None
        reg_unit_id = None
        reg_status = None
        reg_check_in_date = None
        reg_check_out_date = None
    else:
        reg_shelter = "shelter_registration.shelter_id" \
                      if shelters and len(shelters) > 1 else None
        reg_unit_id = "shelter_registration.shelter_unit_id"
        reg_status = "shelter_registration.registration_status"
        reg_check_in_date = "shelter_registration.check_in_date"
        reg_check_out_date = "shelter_registration.check_out_date"

    # Filter flags for case organisation
    if organisation_id:
        flags = S3SQLInlineLink("case_flag",
                                label = T("Flags"),
                                field = "flag_id",
                                help_field = "comments",
                                filterby = {"organisation_id": organisation_id},
                                cols = 4,
                                )
    else:
        flags = None

    if privileged:

        # Extended form for privileged user roles
        crud_form = S3SQLCustomForm(
                # Case Details ----------------------------
                (T("Case Status"), "dvr_case.status_id"),
                flags,

                # Person Details --------------------------
                (T("ID"), "pe_label"),
                "last_name",
                "first_name",
                "person_details.nationality",
                "date_of_birth",
                "gender",
                "person_details.marital_status",

                # Process Data ----------------------------
                "dvr_case.date",
                "dvr_case.organisation_id",

                # TODO Replace by suitable model:
                #"dvr_case.origin_site_id",
                #"dvr_case.destination_site_id",

                S3SQLInlineComponent(
                        "bamf",
                        fields = [("", "value")],
                        filterby = {"field": "tag",
                                    "options": "BAMF",
                                    },
                        label = T("BAMF Reference Number"),
                        multiple = False,
                        name = "bamf",
                        ),
                "dvr_case.valid_until",

                S3SQLInlineComponent(
                        "residence_status",
                        fields = ["status_type_id",
                                  "permit_type_id",
                                  "reference",
                                  "valid_from",
                                  "valid_until",
                                  ],
                        label = T("Residence Status"),
                        multiple = False,
                        ),

                # Shelter Data ----------------------------
                reg_shelter,
                reg_unit_id,
                reg_status,
                reg_check_in_date,
                reg_check_out_date,

                # Other Details ---------------------------
                "person_details.occupation",
                S3SQLInlineComponent(
                        "contact",
                        fields = [("", "value")],
                        filterby = {"field": "contact_method",
                                    "options": "SMS",
                                    },
                        label = T("Mobile Phone"),
                        multiple = False,
                        name = "phone",
                        ),
                "person_details.literacy",
                S3SQLInlineComponent(
                        "case_language",
                        fields = ["language",
                                  "quality",
                                  "comments",
                                  ],
                        label = T("Language / Communication Mode"),
                        ),
                "person_details.religion",
                "dvr_case.comments",

                # Archived-flag ---------------------------
                # TODO make this available only to case admins and org admins
                (T("Invalid"), "dvr_case.archived"),
                )

        subheadings = {"dvr_case_status_id": T("Case Status"),
                       "pe_label": T("Person Details"),
                       "dvr_case_date": T("Registration"),
                       "shelter_registration_shelter_id": T("Lodging"),
                       "person_details_occupation": T("Other Details"),
                       "dvr_case_archived": T("File Status")
                       }
    else:
        # Reduced form for non-privileged user roles
        crud_form = S3SQLCustomForm(
                flags,
                (T("ID"), "pe_label"),
                "last_name",
                "first_name",
                "person_details.nationality",
                "date_of_birth",
                "gender",
                reg_shelter,
                reg_unit_id,
                S3SQLInlineComponent(
                        "contact",
                        fields = [("", "value")],
                        filterby = {"field": "contact_method",
                                    "options": "SMS",
                                    },
                        label = T("Mobile Phone"),
                        multiple = False,
                        name = "phone",
                        ),
                "dvr_case.comments",
                )

        subheadings = None

    resource.configure(crud_form = crud_form,
                       subheadings = subheadings,
                       )

# -------------------------------------------------------------------------
def configure_case_filters(resource, privileged=False, show_family_transferable=False):
    # TODO review + refactor

    T = current.T
    s3db = current.s3db

    filter_widgets = resource.get_config("filter_widgets")
    if filter_widgets:
        from core import DateFilter, \
                         OptionsFilter, \
                         TextFilter

        extend_text_filter = True
        for fw in filter_widgets:
            # No filter default for case status
            if fw.field == "dvr_case.status_id":
                fw.opts.default = None
            if fw.field == "case_flag_case.flag_id":
                fw.opts.size = None
            # Text filter includes EasyOpt Number and Case Comments
            if extend_text_filter and isinstance(fw, TextFilter):
                fw.field.extend(("dvr_case.comments",
                                 ))
                fw.opts.comment = T("You can search by name, ID or comments")
                extend_text_filter = False

        # Add filter for date of birth
        dob_filter = DateFilter("date_of_birth")
        #dob_filter.operator = ["eq"]
        filter_widgets.insert(1, dob_filter)

        # Additional filters for privileged roles
        if privileged:
            # Add filter for family transferability
            if show_family_transferable:
                ft_filter = OptionsFilter("dvr_case.household_transferable",
                                          label = T("Family Transferable"),
                                          options = {True: T("Yes"),
                                                     False: T("No"),
                                                     },
                                          cols = 2,
                                          hidden = True,
                                          )
                filter_widgets.append(ft_filter)

            # Add filter for registration date
            reg_filter = DateFilter("dvr_case.date",
                                    hidden = True,
                                    )
            filter_widgets.append(reg_filter)

            # Add filter for registration status
            reg_filter = OptionsFilter("shelter_registration.registration_status",
                                       label = T("Presence"),
                                       options = s3db.cr_shelter_registration_status_opts,
                                       hidden = True,
                                       cols = 3,
                                       )
            filter_widgets.append(reg_filter)

            # Add filter for BAMF Registration Number
            bamf_filter = TextFilter(["bamf.value"],
                                     label = T("BAMF Ref.No."),
                                     hidden = True,
                                     )
            filter_widgets.append(bamf_filter)

        # Add filter for IDs
        id_filter = TextFilter(["pe_label"],
                               label = T("IDs"),
                               match_any = True,
                               hidden = True,
                               comment = T("Search for multiple IDs (separated by blanks)"),
                               )
        filter_widgets.append(id_filter)

# -------------------------------------------------------------------------
def configure_case_list_fields(resource,
                               privileged = False,
                               check_overdue = False,
                               show_family_transferable = False,
                               fmt = None,
                               ):
    # TODO docstring

    T = current.T

    #settings = current.deployment_settings

    # TODO Restore when absence fixed
    #if fmt in ("html", "iframe", "aadata"):
    #    # Delivers HTML, so restrict to GUI:
    #    absence_field = (T("Checked-out"), "absence")
    #    resource.configure(extra_fields = ["shelter_registration.registration_status",
    #                                       "shelter_registration.check_out_date",
    #                                       ],
    #                       )
    #else:
    #    absence_field = None

    # Standard list fields
    list_fields = [(T("ID"), "pe_label"),
                   "last_name",
                   "first_name",
                   "date_of_birth",
                   "gender",
                   "person_details.nationality",
                   #"dvr_case.date",
                   #"dvr_case.status_id",
                   # TODO include shelter_id if multiple orgs or multiple shelters
                   (T("Shelter"), "shelter_registration.shelter_unit_id"),
                   ]
    orderby = "pr_person.last_name"

    if privileged:
        # Additional list fields for privileged roles
        list_fields[-1:-1] = ("dvr_case.date",
                              "dvr_case.status_id",
                              )
        # Show latest on top
        orderby = "dvr_case.date desc"

        # Add fields for managing transferability
        #if settings.get_dvr_manage_transferability() and not check_overdue:
        #    transf_fields = ["dvr_case.transferable",
        #                     (T("Size of Family"), "dvr_case.household_size"),
        #                     ]
        #    if show_family_transferable:
        #        transf_fields.append((T("Family Transferable"), "dvr_case.household_transferable"))
        #    list_fields[-1:-1] = transf_fields

        # Days of absence (virtual field)
        # TODO Restore when absence fixed
        #if absence_field:
        #    list_fields.append(absence_field)

    resource.configure(list_fields = list_fields,
                       orderby = orderby,
                       )

# -------------------------------------------------------------------------
def configure_id_cards(r, resource, administration=False):
    """
        Configure ID card generator
    """

    if administration:
        from ..idcards import GenerateIDCard
        current.s3db.set_method("pr_person",
                                component = "identity",
                                method = "generate",
                                action = GenerateIDCard,
                                )

# -------------------------------------------------------------------------
def configure_dvr_person_controller(r, privileged=False, administration=False):
    """
        Case File (Full)

        Args:
            r: the CRUDRequest
            privileged: user has privileged role (to see extended case details)
            administration: user has an administrive role (ORG_ADMIN|CASE_ADMIN)
    """

    db = current.db
    s3db = current.s3db
    settings = current.deployment_settings

    resource = r.resource

    from gluon import Field, IS_IN_SET, IS_NOT_EMPTY

    # Absence-days method, used in both list_fields and rheader
    table = r.table
    table.absence = Field.Method("absence", mrcms_absence)

    # ID Card Export
    configure_id_cards(r, resource, administration=administration)

    # Determine case organisation
    case_resource = resource.components.get("dvr_case")
    default_case_organisation = s3db.org_restrict_for_organisations(case_resource)
    record = r.record
    if record:
        person_id = record.id
        case_organisation = get_case_organisation(person_id)
    else:
        person_id = None
        case_organisation = default_case_organisation

    if not r.component:

        configure_person_tags()

        # Determine available shelters and default
        if case_organisation:
            shelters = get_available_shelters(case_organisation, person_id)
        else:
            shelters = None

        if r.interactive and r.method != "import":

            # Registration status effective dates not manually updateable
            if r.id:
                rtable = s3db.cr_shelter_registration
                field = rtable.check_in_date
                field.writable = False
                field = rtable.check_out_date
                field.writable = False

            # Make marital status mandatory, remove "other"
            dtable = s3db.pr_person_details
            field = dtable.marital_status
            options = dict(s3db.pr_marital_status_opts)
            del options[9] # Remove "other"
            field.requires = IS_IN_SET(options, zero=None)

            # Make gender mandatory, remove "unknown"
            field = table.gender
            field.default = None
            from core import IS_PERSON_GENDER
            options = dict(s3db.pr_gender_opts)
            del options[1] # Remove "unknown"
            field.requires = IS_PERSON_GENDER(options, sort = True)

            # No comment for pe_label
            field = table.pe_label
            field.writable = not settings.get_custom("autogenerate_case_ids")
            field.comment = None

            # Last name is required
            field = table.last_name
            field.requires = IS_NOT_EMPTY()

            # TODO make nationality mandatory

            # Check whether the shelter registration shall be cancelled
            cancel = False
            if r.http == "POST":
                post_vars = r.post_vars
                archived = post_vars.get("sub_dvr_case_archived")
                status_id = post_vars.get("sub_dvr_case_status_id")
                if archived:
                    cancel = True
                if not cancel and status_id:
                    stable = s3db.dvr_case_status
                    status = db(stable.id == status_id).select(stable.is_closed,
                                                               limitby = (0, 1),
                                                               ).first()
                    if status and status.is_closed:
                        cancel = True

            # Configure case form
            configure_case_form(resource,
                                privileged = privileged,
                                cancel = cancel,
                                shelters = shelters,
                                organisation_id = case_organisation,
                                person_id = person_id,
                                )

            # Configure case filters
            configure_case_filters(resource,
                                   privileged = privileged,
                                   #show_family_transferable = show_family_transferable,
                                   )

        # Configure case list fields (must be outside of r.interactive)
        configure_case_list_fields(resource,
                                   privileged = privileged,
                                   #check_overdue = check_overdue,
                                   #show_family_transferable = show_family_transferable,
                                   fmt = r.representation,
                                   )

    elif r.component_name == "case_appointment":

        component = r.component

        # Filter to appointment types of the case organisation
        if case_organisation:

            # Filter records
            component.add_filter(FS("type_id$organisation_id") == case_organisation)

            # Filter type selector
            ctable = component.table
            ttable = s3db.dvr_case_appointment_type
            field = ctable.type_id
            dbset = db(ttable.organisation_id == case_organisation)
            field.requires = IS_ONE_OF(dbset, "dvr_case_appointment_type.id",
                                       field.represent,
                                       )

        # Make appointments tab read-only even if the user is permitted
        # to create or update appointments (via event registration), except
        # for ORG_ADMIN/CASE_ADMIN:
        if not administration:
            component.configure(insertable = False,
                                editable = False,
                                deletable = False,
                                )

# -------------------------------------------------------------------------
def configure_security_person_controller(r):
    """ Case file (Security) """

    # TODO review + refactor

    T = current.T
    db = current.db
    s3db = current.s3db

    # Restricted view for Security staff
    if r.component:
        from gluon import redirect
        redirect(r.url(method=""))

    resource = r.resource

    # Autocomplete using alternative search method
    search_fields = ("first_name", "last_name", "pe_label")
    s3db.set_method("pr_person",
                    method = "search_ac",
                    action = s3db.pr_PersonSearchAutocomplete(search_fields),
                    )

    current.deployment_settings.ui.export_formats = None

    # Filter to valid and open cases
    query = (FS("dvr_case.id") != None) & \
            ((FS("dvr_case.archived") == False) | \
             (FS("dvr_case.archived") == None)) & \
            (FS("dvr_case.status_id$is_closed") == False)
    resource.add_filter(query)

    # Adjust CRUD strings
    current.response.s3.crud_strings["pr_person"].update(
        {"title_list": T("Current Residents"),
         "label_list_button": T("List Residents"),
         })

    # No side menu
    current.menu.options = None

    # Only Show Security Notes
    ntable = s3db.dvr_note_type
    note_type = db(ntable.name == "Security").select(ntable.id,
                                                     limitby=(0, 1),
                                                     ).first()
    try:
        note_type_id = note_type.id
    except AttributeError:
        current.log.error("Prepop not done - deny access to dvr_note component")
        note_type_id = None
        atable = s3db.dvr_note
        atable.date.readable = atable.date.writable = False
        atable.note.readable = atable.note.writable = False

    # Custom CRUD form
    from core import S3SQLCustomForm, S3SQLInlineComponent
    crud_form = S3SQLCustomForm(
                    (T("ID"), "pe_label"),
                    "last_name",
                    "first_name",
                    "date_of_birth",
                    #"gender",
                    "person_details.nationality",
                    "shelter_registration.shelter_unit_id",
                    S3SQLInlineComponent(
                            "case_note",
                            fields = [(T("Date"), "date"),
                                        "note",
                                        ],
                            filterby = {"field": "note_type_id",
                                        "options": note_type_id,
                                        },
                            label = T("Security Notes"),
                            ),
                    )

    # Custom list fields
    list_fields = [(T("ID"), "pe_label"),
                   "last_name",
                   "first_name",
                   "date_of_birth",
                   #"gender",
                   "person_details.nationality",
                   "shelter_registration.shelter_unit_id",
                   ]

    # Profile page (currently unused)
    if r.method == "profile":
        from gluon.html import DIV, H2, TABLE, TR, TD
        from core import s3_fullname
        person_id = r.id
        record = r.record
        table = r.table
        dtable = s3db.pr_person_details
        details = db(dtable.person_id == person_id).select(dtable.nationality,
                                                           limitby=(0, 1)
                                                           ).first()
        try:
            nationality = details.nationality
        except AttributeError:
            nationality = None
        rtable = s3db.cr_shelter_registration
        reg = db(rtable.person_id == person_id).select(rtable.shelter_unit_id,
                                                       limitby=(0, 1)
                                                       ).first()
        try:
            shelter_unit_id = reg.shelter_unit_id
        except AttributeError:
            shelter_unit_id = None
        profile_header = DIV(H2(s3_fullname(record)),
                                TABLE(TR(TD(T("ID")),
                                         TD(record.pe_label)
                                         ),
                                      TR(TD(table.last_name.label),
                                         TD(record.last_name)
                                         ),
                                      TR(TD(table.first_name.label),
                                         TD(record.first_name)
                                         ),
                                      TR(TD(table.date_of_birth.label),
                                         TD(record.date_of_birth)
                                         ),
                                      TR(TD(dtable.nationality.label),
                                         TD(nationality)
                                         ),
                                      TR(TD(rtable.shelter_unit_id.label),
                                         TD(shelter_unit_id)
                                         ),
                                      ),
                                _class="profile-header",
                                )

        notes_widget = dict(label = "Security Notes",
                            label_create = "Add Note",
                            type = "datatable",
                            tablename = "dvr_note",
                            filter = ((FS("note_type_id$name") == "Security") & \
                                        (FS("person_id") == person_id)),
                            #icon = "report",
                            create_controller = "dvr",
                            create_function = "note",
                            )
        profile_widgets = [notes_widget]
    else:
        profile_header = None
        profile_widgets = None

    resource.configure(crud_form = crud_form,
                       list_fields = list_fields,
                       profile_header = profile_header,
                       profile_widgets = profile_widgets,
                       )

# -------------------------------------------------------------------------
def configure_default_person_controller(r):
    """ Personal Profile """

    T = current.T

    s3db = current.s3db

    # Expose ID-label (read-only)
    table = s3db.pr_person
    field = table.pe_label
    field.label = T("ID")
    field.readable = True
    field.comment = None

    if not r.component:

        # Reduce form to relevant fields
        from core import S3SQLCustomForm
        crud_form = S3SQLCustomForm("pe_label",
                                    "last_name",
                                    "first_name",
                                    "date_of_birth",
                                    "gender",
                                    "person_details.nationality",
                                    )
        s3db.configure("pr_person", crud_form=crud_form)

    elif r.component_name == "human_resource":

        # Reduce to relevant list_fields
        list_fields = ["organisation_id",
                       "site_id",
                       "job_title_id",
                       "start_date",
                       "end_date",
                       "status",
                       ]
        r.component.configure(list_fields = list_fields,
                              # Staff records not modifiable in this view
                              # => must use tab of organisation for that
                              insertable = False,
                              editable = False,
                              deletable = False,
                              )

    elif r.component_name in ("identity", "image"):

        # User cannot self modify their identity details
        r.component.configure(insertable = False,
                              editable = False,
                              deletable = False,
                              )

# -------------------------------------------------------------------------
def configure_hrm_person_controller(r):
    """ Staff File """

    T = current.T

    s3db = current.s3db
    auth = current.auth

    # Expose ID label (read-only)
    table = s3db.pr_person
    field = table.pe_label
    field.label = T("ID")
    field.readable = True
    field.comment = None

    # ID Card Export
    administration = auth.s3_has_role("ORG_ADMIN")
    configure_id_cards(r, r.resource, administration=administration)

    if not r.component:

        # Reduce form to relevant fields
        if r.record and r.record.id == auth.s3_logged_in_person() or \
           auth.s3_has_roles(("ORG_ADMIN", "SECURITY")):
            pe_label = "pe_label"
        else:
            pe_label = None

        from core import S3SQLCustomForm
        crud_form = S3SQLCustomForm(pe_label,
                                    "last_name",
                                    "first_name",
                                    "date_of_birth",
                                    "gender",
                                    "person_details.nationality",
                                    )

        s3db.configure("pr_person", crud_form=crud_form)

    elif r.component_name == "human_resource":

        # Reduce to relevant list_fields
        list_fields = ["organisation_id",
                       "site_id",
                       "job_title_id",
                       "start_date",
                       "end_date",
                       "status",
                       ]
        r.component.configure(list_fields=list_fields)

# -------------------------------------------------------------------------
def pr_person_controller(**attr):

    T = current.T
    auth = current.auth
    s3 = current.response.s3

    is_org_admin = auth.s3_has_role("ORG_ADMIN")

    ADMINISTRATION = ("CASE_ADMIN",)
    administration = is_org_admin or auth.s3_has_roles(ADMINISTRATION)

    PRIVILEGED = ("CASE_MANAGER", "CASE_ASSISTANT")
    privileged = administration or auth.s3_has_roles(PRIVILEGED)

    QUARTERMASTER = auth.s3_has_role("QUARTERMASTER") and not privileged

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        if QUARTERMASTER:
            # Enforce closed=0
            r.vars["closed"] = r.get_vars["closed"] = "0"

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        get_vars = r.get_vars

        # Adjust list title for invalid cases (normally "Archived")
        archived = get_vars.get("archived")
        if archived in ("1", "true", "yes"):
            crud_strings = s3.crud_strings["pr_person"]
            crud_strings["title_list"] = T("Invalid Cases")

        controller = r.controller
        if controller == "dvr":
            configure_dvr_person_controller(r,
                                            privileged = privileged,
                                            administration = administration,
                                            )
        elif controller == "security":
            configure_security_person_controller(r)

        elif controller == "hrm":
            configure_hrm_person_controller(r)

        elif controller == "default":
            configure_default_person_controller(r)

        if r.component_name == "identity":

            if r.component_id:
                component = r.component
                component.load()
                crecord = component.records().first()
                if crecord and crecord.system:
                    # Restrict modifications
                    ctable = component.table
                    readonly = ["type", "value", "valid_from"]
                    for fn in readonly:
                        ctable[fn].writable = False
                    hidden = ["description", "ia_name", "image"]
                    for fn in hidden:
                        field = ctable[fn]
                        field.readable = field.writable = False
                    if crecord.invalid:
                        ctable.valid_until.writable = False

        return result
    s3.prep = prep

    # Custom postp
    standard_postp = s3.postp
    def postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if QUARTERMASTER:
            # Add Action Button to assign Housing Unit to the Resident
            s3.actions = [dict(label=s3_str(T("Assign Shelter")),
                               _class="action-btn",
                               url=URL(c="cr",
                                       f="shelter_registration",
                                       args=["assign"],
                                       vars={"person_id": "[id]"},
                                       )),
                          ]

        if not r.component and r.record and isinstance(output, dict):

            # Custom CRUD buttons
            if "buttons" not in output:
                buttons = output["buttons"] = {}
            else:
                buttons = output["buttons"]

            # Button to generate ID
            if administration and r.controller == "dvr" or \
               is_org_admin and r.controller == "hrm":
                card_button = A(T("Generate ID"),
                                _href = r.url(component = "identity",
                                              method = "generate",
                                              ),
                                _class = "action-btn",
                                )
            else:
                card_button = ""

            # Render in place of the delete-button
            buttons["delete_btn"] = TAG[""](card_button,
                                            )
        return output
    s3.postp = postp

    # Custom rheader tabs
    from ..rheaders import dvr_rheader, hrm_rheader, default_rheader
    if current.request.controller == "dvr":
        attr["rheader"] = dvr_rheader
    elif current.request.controller == "hrm":
        attr["rheader"] = hrm_rheader
    elif current.request.controller == "default":
        attr["rheader"] = default_rheader

    return attr

# -------------------------------------------------------------------------
def pr_group_membership_controller(**attr):

    # TODO review + refactor

    T = current.T
    s3db = current.s3db
    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        ROLE = T("Role")

        resource = r.resource
        if r.controller == "dvr":

            # Set default shelter
            from ..helpers import get_default_shelter
            shelter_id = get_default_shelter()
            if shelter_id:
                rtable = s3db.cr_shelter_registration
                field = rtable.shelter_id
                field.default = shelter_id

            if r.interactive:
                table = resource.table

                from gluon import IS_EMPTY_OR
                from core import PersonSelector

                s3db.pr_person.pe_label.label = T("ID")

                field = table.person_id
                field.represent = s3db.pr_PersonRepresent(show_link=True)
                field.widget = PersonSelector(controller = "dvr",
                                              pe_label = True,
                                              nationality = True,
                                              )

                field = table.role_id
                field.readable = field.writable = True
                field.label = ROLE
                field.comment = None
                field.requires = IS_EMPTY_OR(
                                    IS_ONE_OF(current.db, "pr_group_member_role.id",
                                              field.represent,
                                              filterby = "group_type",
                                              filter_opts = (7,),
                                              ))

                field = table.group_head
                field.label = T("Head of Family")

                # Custom CRUD strings for this perspective
                s3.crud_strings["pr_group_membership"] = Storage(
                    label_create = T("Add Family Member"),
                    title_display = T("Family Member Details"),
                    title_list = T("Family Members"),
                    title_update = T("Edit Family Member"),
                    label_list_button = T("List Family Members"),
                    label_delete_button = T("Remove Family Member"),
                    msg_record_created = T("Family Member added"),
                    msg_record_modified = T("Family Member updated"),
                    msg_record_deleted = T("Family Member removed"),
                    msg_list_empty = T("No Family Members currently registered")
                    )

            list_fields = [(T("ID"), "person_id$pe_label"),
                           "person_id",
                           "person_id$date_of_birth",
                           "person_id$gender",
                           "group_head",
                           (ROLE, "role_id"),
                           (T("Case Status"), "person_id$dvr_case.status_id"),
                           "person_id$dvr_case.transferable",
                           ]
            # Retain group_id in list_fields if added in standard prep
            lfields = resource.get_config("list_fields")
            if "group_id" in lfields:
                list_fields.insert(0, "group_id")
            resource.configure(filter_widgets = None,
                               list_fields = list_fields,
                               )
        return result
    s3.prep = prep

    from ..rheaders import dvr_rheader
    attr["rheader"] = dvr_rheader

    return attr

# END =========================================================================
