"""
    PR module customisations for MRCMS

    License: MIT
"""

import datetime

from gluon import current, URL, A, IS_EMPTY_OR, SPAN, TAG
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

            delta = max(0, (current.request.utcnow - check_out_date).total_seconds())
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

        # Cannot modify any fields in main person record
        ptable = s3db.pr_person
        for field in ptable:
            field.writable = False

        # Cannot modify certain details
        dtable = s3db.pr_person_details
        for fn in ("nationality", "marital_status"):
            dtable[fn].writable = False

        # Can not add or modify contact or identity information,
        # images, tags or residence status
        for tn in ("pr_contact",
                   "pr_identity",
                   "pr_image",
                   "pr_person_tag",
                   "dvr_residence_status",
                   ):
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
                   realm_components = ("address",
                                       "case_activity",
                                       "case_details",
                                       "case_language",
                                       "case_note",
                                       "contact",
                                       "contact_emergency",
                                       "group_membership",
                                       "identity",
                                       "image",
                                       "person_details",
                                       "person_tag",
                                       "residence_status",
                                       "response_action",
                                       "service_contact",
                                       "shelter_registration",
                                       "shelter_registration_history",
                                       "vulnerability",
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
        field.writable = not default_shelter
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

            # Get current registration unit
            if person_id:
                rtable = s3db.cr_shelter_registration
                query = (rtable.person_id == person_id) & \
                        (rtable.deleted == False)
                reg = db(query).select(rtable.shelter_unit_id,
                                       limitby = (0, 1),
                                       orderby = ~rtable.id,
                                       ).first()
                current_unit = reg.shelter_unit_id if reg else None
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
                        administration=False,
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
            administration: whether the user has an administrative role
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
        # Ignore registration data in form if the registration is to be
        # cancelled (i.e. case closed or marked as invalid) - otherwise
        # the implicit cancelation (dvr_case_onaccept) is subsequently
        # overridden by the subform processing:
        reg_shelter = None
        reg_unit_id = None
        reg_status = None
        reg_check_in_date = None
        reg_check_out_date = None
    else:
        # Show shelter selector only if there are multiple alternatives,
        # otherwise this will default (see configure_inline_shelter_registration)
        reg_shelter = "shelter_registration.shelter_id" \
                      if shelters and len(shelters) > 1 else None
        reg_unit_id = "shelter_registration.shelter_unit_id"
        reg_status = "shelter_registration.registration_status"
        reg_check_in_date = "shelter_registration.check_in_date"
        reg_check_out_date = "shelter_registration.check_out_date"

    # Configure case component
    component = resource.components.get("dvr_case")
    if not administration:
        field = component.table.archived
        field.readable = field.writable = False
    field = component.table.reference
    field.label = T("Principal Ref.No.")
    field.comment = T("Reference number to use in communication with the principal client, e.g. %(examples)s") % \
                    {"examples": "ZAB-Nr."}

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

                "dvr_case.reference",
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
                        "phone",
                        fields = [("", "value")],
                        label = T("Mobile Phone"),
                        multiple = False,
                        name = "phone",
                        ),
                S3SQLInlineComponent(
                        "email",
                        fields = [("", "value")],
                        label = T("Email"),
                        multiple = False,
                        name = "email",
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
def configure_case_filters(resource, organisation_id=None, privileged=False):
    """
        Configure case list filters

        Args:
            resource: the (pr_person) resource
            organisation_id: the default case organisation ID
            privileged: whether the user has a privileged role with
                        extended access to fields
    """

    T = current.T

    db = current.db
    s3db = current.s3db

    from core import AgeFilter, DateFilter, OptionsFilter, TextFilter, get_filter_options

    # Status filter options
    get_status_opts = s3db.dvr_case_status_filter_opts
    default_status = None
    closed = current.request.get_vars.get("closed")
    if closed == "only":
        status_opts = lambda: get_status_opts(closed=True)
    elif closed in {"1", "include"}:
        status_opts = get_status_opts
    else:
        status_opts = lambda: get_status_opts(closed=False)
        # Assuming that the default status is an open-status
        default_status = s3db.dvr_case_default_status()

    # Text filter fields
    text_filter_fields = ["pe_label",
                          "first_name",
                          "last_name",
                          "dvr_case.comments",
                          ]
    if privileged:
        text_filter_fields.extend(["dvr_case.reference",
                                   "shelter_registration.shelter_unit_id$name",
                                   ])

    # Basic filters
    filter_widgets = [
            TextFilter(text_filter_fields,
                       label = T("Search"),
                       comment = T("You can search by name, ID or comments"),
                       ),
            OptionsFilter("dvr_case.status_id",
                          cols = 3,
                          default = default_status,
                          label = T("Case Status"),
                          options = status_opts,
                          sort = False,
                          ),
            AgeFilter("date_of_birth",
                      label = T("Age"),
                      hidden = True,
                      ),
            OptionsFilter("person_details.nationality",
                          hidden = True,
                          ),
            OptionsFilter("case_flag_case.flag_id",
                          label = T("Flags"),
                          options = get_filter_options("dvr_case_flag",
                                                       translate = True,
                                                       ),
                          cols = 3,
                          hidden = True,
                          ),
            ]

    # Extended filters
    ptable = s3db.pr_person
    ctable = s3db.dvr_case
    otable = s3db.org_organisation
    stable = s3db.cr_shelter
    rtable = s3db.cr_shelter_registration

    query = current.auth.s3_accessible_query("read", "pr_person")
    person_ids = db(query)._select(ptable.id)

    # Organisation filter
    organisation_ids = db(ctable.person_id.belongs(person_ids))._select(ctable.organisation_id)
    organisations = db(otable.id.belongs(organisation_ids)).select(otable.id,
                                                                   otable.name,
                                                                   )
    if organisations and len(organisations) > 1:
        filter_widgets.append(
                OptionsFilter("dvr_case.organisation_id",
                              options = {o.id: o.name for o in organisations},
                              hidden = True,
                              ))

    # Shelter filter
    shelter_ids = db(rtable.person_id.belongs(person_ids))._select(rtable.shelter_id)
    shelters = db(stable.id.belongs(shelter_ids)).select(stable.id,
                                                         stable.name,
                                                         )
    if shelters and len(shelters) > 1:
        filter_widgets.append(
                OptionsFilter("cr_shelter_registration.shelter_id",
                              options = {s.id: s.name for s in shelters},
                              hidden = True,
                              ))

    # Additional filters for privileged roles
    if privileged:
        from ..helpers import AbsenceFilter
        filter_widgets.extend([
                AbsenceFilter("dvr_case.last_seen_on",
                              label = T("Last seen"),
                              hidden = True,
                              ),
                DateFilter("dvr_case.date",
                           hidden = True,
                           ),
                TextFilter(["bamf.value"],
                           label = T("BAMF Ref.No."),
                           hidden = True,
                           ),
                TextFilter(["pe_label"],
                           label = T("IDs"),
                           match_any = True,
                           hidden = True,
                           comment = T("Search for multiple IDs (separated by blanks)"),
                           ),
                ])

    resource.configure(filter_widgets=filter_widgets)

# -------------------------------------------------------------------------
def configure_case_list_fields(resource,
                               privileged = False,
                               fmt = None,
                               ):
    """
        Configure case list fields

        Args:
            resource: the pr_person resource
            privileged: whether the user has a privileged role to see
                        extended case details
            fmt: the output format
    """

    T = current.T

    # Accessible shelters
    shelters = current.s3db.resource("cr_shelter").select(["id"], as_rows=True)
    if len(shelters) == 1:
        # Only one shelter => include only housing unit
        shelter = None
        unit = (T("Housing Unit"), "shelter_registration.shelter_unit_id")
    elif shelters:
        # Multiple shelters => include both shelter and housing unit
        shelter = (T("Shelter"), "shelter_registration.shelter_id")
        unit = (T("Housing Unit"), "shelter_registration.shelter_unit_id")
    else:
        # No shelters => include neither
        shelter = unit = None

    if privileged:
        # Additional list fields for privileged roles
        case_date = "dvr_case.date"
        case_status = "dvr_case.status_id"

        # Show latest on top
        orderby = "dvr_case.date desc"

        # Days of absence (virtual field)
        # TODO Restore when absence fixed
        #if absence_field:
        #    list_fields.append(absence_field)
    else:
        case_date = case_status = None

        # Order alphabetically
        orderby = "pr_person.last_name, pr_person.first_name"

    # Custom list fields
    if fmt in ("xlsx", "xls"):
        list_fields = [(T("ID"), "pe_label"),
                       (T("Principal Ref.No."), "dvr_case.reference"),
                       (T("BAMF Ref.No."), "bamf.value"),
                       "last_name",
                       "first_name",
                       "date_of_birth",
                       "gender",
                       "person_details.nationality",
                       (T("Size of Family"), "dvr_case.household_size"),
                       case_status,
                       case_date,
                       shelter,
                       unit,
                       "dvr_case.last_seen_on",
                       ]
    else:
        list_fields = [(T("ID"), "pe_label"),
                       "last_name",
                       "first_name",
                       "date_of_birth",
                       "gender",
                       "person_details.nationality",
                       case_status,
                       case_date,
                       shelter,
                       unit,
                       ]

    resource.configure(list_fields = list_fields,
                       orderby = orderby,
                       )

# -------------------------------------------------------------------------
def configure_case_reports(resource):

    T = current.T

    # Report options
    facts = ((T("Number of Cases"), "count(id)"),
             )
    axes = ["person_details.nationality",
            "gender",
            "person_details.religion",
            "person_details.literacy",
            #"shelter_registration.shelter_id",
            #"shelter_registration.shelter_unit_id",
            ]
    report_options = {
        "rows": axes,
        "cols": axes,
        "fact": facts,
        "defaults": {"rows": axes[0],
                     "cols": None,
                     "fact": facts[0],
                     "totals": True,
                     },
        }

    resource.configure(report_options=report_options)

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
            from ..helpers import get_available_shelters
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

            # No comment for pe_label
            field = table.pe_label
            field.writable = not settings.get_custom("autogenerate_case_ids")
            field.comment = None

            # Last name is required
            field = table.last_name
            field.requires = IS_NOT_EMPTY()

            # Date of Birth is required
            field = table.date_of_birth
            requires = field.requires
            if isinstance(requires, IS_EMPTY_OR):
                field.requires = requires.other

            # Make gender mandatory, remove "unknown"
            field = table.gender
            field.default = None
            from core import IS_PERSON_GENDER
            options = dict(s3db.pr_gender_opts)
            del options[1] # Remove "unknown"
            field.requires = IS_PERSON_GENDER(options, sort = True)

            # Make marital status mandatory, remove "other"
            dtable = s3db.pr_person_details
            field = dtable.marital_status
            options = dict(s3db.pr_marital_status_opts)
            del options[9] # Remove "other"
            field.requires = IS_IN_SET(options, zero=None)

            # Make nationality mandatory
            field = dtable.nationality
            requires = field.requires
            if isinstance(requires, IS_EMPTY_OR):
                field.requires = requires.other
            settings.pr.nationality_explicit_unclear = True

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
                                administration = administration,
                                cancel = cancel,
                                shelters = shelters,
                                organisation_id = case_organisation,
                                person_id = person_id,
                                )

            # Configure case filters
            if not record:
                configure_case_filters(resource,
                                       organisation_id = case_organisation,
                                       privileged = privileged,
                                       )

            # Configure case reports
            configure_case_reports(resource)

        # Configure case list fields (must be outside of r.interactive)
        configure_case_list_fields(resource,
                                   privileged = privileged,
                                   fmt = r.representation,
                                   )

        # Apply absence filter
        if not record:
            from ..helpers import AbsenceFilter
            AbsenceFilter.apply_filter(resource, r.get_vars)

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
        # for CASE_ADMIN/CASE_MANAGER:
        if not privileged:
            component.configure(insertable = False,
                                editable = False,
                                deletable = False,
                                )

    elif r.component.tablename in ("dvr_vulnerability",
                                   "dvr_case_activity",
                                   "dvr_response_action",
                                   ):
        is_org_admin = current.auth.s3_has_role("ORG_ADMIN")

        # Do not show job title for staff member link
        represent = s3db.hrm_HumanResourceRepresent(show_link = False,
                                                    show_title = False,
                                                    )
        for tn in ("dvr_vulnerability",
                   "dvr_case_activity",
                   "dvr_case_activity_update",
                   "dvr_response_action",
                   ):
            field = s3db[tn].human_resource_id
            field.readable = True
            # If there is a default staff member responsible set, only
            # the OrgAdmin (or Admin) can select someone else:
            field.writable = is_org_admin if field.default else True
            field.represent = represent

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

    elif r.component_name == "image":

        # Only OrgAdmin can modify staff photographs
        if not current.auth.s3_has_role("ORG_ADMIN"):
            r.component.configure(insertable = False,
                                  editable = False,
                                  deletable = False,
                                  )

# -------------------------------------------------------------------------
def pr_person_controller(**attr):

    T = current.T
    auth = current.auth
    s3 = current.response.s3

    current.deployment_settings.base.bigtable = True

    is_org_admin = auth.s3_has_role("ORG_ADMIN")
    is_case_admin = auth.s3_has_role("CASE_ADMIN")

    administration = is_org_admin or is_case_admin

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
        if r.controller in ("dvr", "counsel"):
            from .dvr import dvr_person_prep
            result = dvr_person_prep(r)
        else:
            result = standard_prep(r) if callable(standard_prep) else True

        get_vars = r.get_vars

        # Adjust list title for invalid cases (normally "Archived")
        archived = get_vars.get("archived")
        if archived in ("1", "true", "yes"):
            crud_strings = s3.crud_strings["pr_person"]
            crud_strings["title_list"] = T("Invalid Cases")

        controller = r.controller
        if controller in ("dvr", "counsel"):
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

        if r.record and isinstance(output, dict):

            # Generate-ID button (required appropriate role)
            if r.controller == "dvr" and is_case_admin or \
               r.controller == "hrm" and is_org_admin:
                id_btn = A(T("Generate ID"),
                           _href = r.url(component = "identity",
                                         method = "generate",
                                         ),
                          _class = "action-btn activity button",
                          )
                if not r.component:
                    if "buttons" not in output:
                        buttons = output["buttons"] = {}
                    else:
                        buttons = output["buttons"]
                    buttons["delete_btn"] = TAG[""](id_btn)
                elif r.component_name == "identity":
                    showadd_btn = output.get("showadd_btn")
                    if showadd_btn:
                        output["showadd_btn"] = TAG[""](id_btn, showadd_btn)
                    else:
                        output["showadd_btn"] = id_btn

            # Organizer-button for appointments
            if r.component_name == "case_appointment":
                oa_btn = A(T("Calendar"),
                           _href = r.url(component = "case_appointment",
                                         method = "organize",
                                         ),
                           _class = "action-btn activity button",
                           )
                showadd_btn = output.get("showadd_btn")
                if showadd_btn:
                    output["showadd_btn"] = TAG[""](oa_btn, showadd_btn)
                else:
                    output["showadd_btn"] = oa_btn

        return output
    s3.postp = postp

    # Custom rheader tabs
    from ..rheaders import dvr_rheader, hrm_rheader, default_rheader
    if current.request.controller in ("dvr", "counsel"):
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

        c = r.controller

        # Custom prep for DVR views
        if c in ("counsel", "dvr"):
            from .dvr import dvr_group_membership_prep
            result = dvr_group_membership_prep(r)
        elif callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        resource = r.resource
        if c in ("counsel", "dvr"):

            viewing = r.viewing
            if viewing and viewing[0] == "pr_person":
                person_id = viewing[1]
            else:
                person_id = None

            from ..helpers import get_default_case_organisation, get_default_case_shelter
            if person_id:
                organisation_id = get_case_organisation(person_id)
                shelter_id, unit_id = get_default_case_shelter(person_id)
            else:
                organisation_id = get_default_case_organisation()
                shelter_id, unit_id = get_default_case_shelter(person_id)

            # Set default case organisation
            ctable = s3db.dvr_case
            ctable.organisation_id.default = organisation_id

            # Set default shelter and housing unit
            rtable = s3db.cr_shelter_registration
            rtable.shelter_id.default = shelter_id
            rtable.shelter_unit_id.default = unit_id

            ROLE = T("Role")

            if r.interactive:
                table = resource.table

                from core import PersonSelector

                s3db.pr_person.pe_label.label = T("ID")

                field = table.person_id
                case_url = URL(c = r.controller,
                               f = "person",
                               args = ["[id]"],
                               extension = "",
                               )
                field.represent = s3db.pr_PersonRepresent(show_link = True,
                                                          linkto = case_url,
                                                          )
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
