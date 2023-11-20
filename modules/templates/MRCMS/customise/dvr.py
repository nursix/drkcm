"""
    DVR module customisations for MRCMS

    License: MIT
"""

import datetime

from dateutil import tz

from gluon import current, URL, A, INPUT, SQLFORM, TAG, IS_EMPTY_OR
from gluon.storage import Storage

from s3dal import Field
from core import CRUDMethod, CRUDRequest, CustomController, FS, IS_ONE_OF, \
                 S3PermissionError, S3DateTime, S3SQLCustomForm, S3SQLInlineLink, \
                 DateFilter, OptionsFilter, TextFilter, \
                 get_form_record_id, s3_fieldmethod, s3_redirect_default, \
                 set_default_filter, set_last_record_id, s3_fullname, s3_str

from .pr import configure_person_tags

# -------------------------------------------------------------------------
def dvr_home():
    """ Redirect dvr/index to dvr/person """

    s3_redirect_default(URL(f="person"))

# -------------------------------------------------------------------------
def dvr_case_onaccept(form):
    """
        Onaccept of case:
            - if the case has been archived, or closed, make sure any
              active shelter registration is set to checked-out
            - if the client does not have a shelter registration, but
              there is a default shelter for new registrations, then
              create a new registration (unless there is a registration
              subform inline)
    """

    db = current.db
    s3db = current.s3db
    auth = current.auth

    record_id = get_form_record_id(form)
    if not record_id:
        return

    table = s3db.dvr_case
    record = db(table.id == record_id).select(table.id,
                                              table.person_id,
                                              table.status_id,
                                              table.archived,
                                              limitby = (0, 1),
                                              ).first()
    if not record:
        return

    # Inline shelter registration?
    inline = "sub_shelter_registration_registration_status" in current.request.post_vars

    cancel = False
    if record.archived:
        cancel = True
    else:
        stable = s3db.dvr_case_status
        status = db(stable.id == record.status_id).select(stable.is_closed,
                                                          limitby = (0, 1),
                                                          ).first()
        if status and status.is_closed:
            cancel = True

    rtable = s3db.cr_shelter_registration
    query = (rtable.person_id == record.person_id)
    if cancel:
        # If there is an active shelter registration, check-out the client
        rtable = s3db.cr_shelter_registration
        query &= (rtable.person_id == record.person_id) & \
                 (rtable.registration_status != 3) & \
                 (rtable.deleted == False)
        reg = db(query).select(rtable.id,
                               limitby = (0, 1),
                               ).first()
        if reg:
            r = CRUDRequest("cr", "shelter_registration", args=[], get_vars={})
            r.customise_resource("cr_shelter_registration")
            reg.update_record(registration_status=3)
            s3db.onaccept(rtable, reg, method="update")

    elif not inline:
        # If there is no shelter registration for this client, but we have a
        # default shelter for new registrations, then create a new registration
        reg = db(query).select(rtable.id,
                               limitby = (0, 1),
                               ).first()
        if not reg and rtable.shelter_id.default:
            reg = {"person_id": record.person_id}
            reg_id = reg["id"] = rtable.insert(person_id=record.person_id)
            s3db.update_super(table, reg)
            auth.s3_set_record_owner(table, reg_id)
            auth.s3_make_session_owner(table, reg_id)
            s3db.onaccept(rtable, reg, method="create")

# -------------------------------------------------------------------------
def dvr_case_resource(r, tablename):

    s3db = current.s3db
    ctable = s3db.dvr_case

    # Allow direct transfer of cases between organisations
    # TODO replace this by a formal takeover-procedure
    s3db.configure("dvr_case", update_realm=True)

    # Custom onaccept to propagate status changes
    s3db.add_custom_callback(tablename, "onaccept", dvr_case_onaccept)

    # All fields read-only except comments, unless user has permission
    # to create new cases
    if not current.auth.s3_has_permission("create", "dvr_case"):
        for field in ctable:
            if field.name != "comments":
                field.writable = False

# -------------------------------------------------------------------------
def dvr_note_resource(r, tablename):
    # TODO review + refactor

    T = current.T
    auth = current.auth

    if not auth.s3_has_role("ADMIN"):

        db = current.db
        s3db = current.s3db

        # Restrict access by note type
        GENERAL = "General"
        MEDICAL = "Medical"
        SECURITY = "Security"

        permitted_note_types = [GENERAL]

        user = auth.user
        if user:
            has_roles = auth.s3_has_roles

            # Roles permitted to access "Security" type notes
            SECURITY_ROLES = ("ADMIN_HEAD",
                              "SECURITY_HEAD",
                              "POLICE",
                              "MEDICAL",
                              )
            if has_roles(SECURITY_ROLES):
                permitted_note_types.append(SECURITY)

            # Roles permitted to access "Health" type notes
            MEDICAL_ROLES = ("ADMIN_HEAD",
                             "MEDICAL",
                             )
            if has_roles(MEDICAL_ROLES):
                permitted_note_types.append(MEDICAL)

        # Filter notes to permitted note types
        query = FS("note_type_id$name").belongs(permitted_note_types)
        if r.tablename == "dvr_note":
            r.resource.add_filter(query)
        else:
            r.resource.add_component_filter("case_note", query)

        # Filter note type selector to permitted note types
        ttable = s3db.dvr_note_type
        query = ttable.name.belongs(permitted_note_types)
        rows = db(query).select(ttable.id)
        note_type_ids = [row.id for row in rows]

        table = s3db.dvr_note
        field = table.note_type_id
        field.label = T("Category")

        if len(note_type_ids) == 1:
            field.default = note_type_ids[0]
            field.writable = False

        field.requires = IS_ONE_OF(db(query), "dvr_note_type.id",
                                   field.represent,
                                   )

# -------------------------------------------------------------------------
def dvr_case_activity_resource(r, tablename):

    T = current.T

    table = current.s3db.dvr_case_activity

    # Set default human_resource_id, alter label
    field = table.human_resource_id
    field.default = current.auth.s3_logged_in_human_resource()
    field.label = T("Registered by")
    field.widget = None # use standard drop-down

# -------------------------------------------------------------------------
def dvr_case_activity_controller(**attr):

    T = current.T
    s3db = current.s3db
    s3 = current.response.s3

    current.deployment_settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def custom_prep(r):

        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        resource = r.resource

        # Filter to valid cases
        if not r.record:
            query = (FS("person_id$dvr_case.archived") == False) | \
                    (FS("person_id$dvr_case.archived") == None)
            resource.add_filter(query)
            # TODO filter to open cases

        if not r.component:

            if r.interactive:
                # Represent person_id as link (including ID)
                table = resource.table
                field = table.person_id
                fmt = "%(pe_label)s %(last_name)s, %(first_name)s"
                field.represent = s3db.pr_PersonRepresent(fields = ("pe_label",
                                                                    "last_name",
                                                                    "first_name",
                                                                    ),
                                                          labels = fmt,
                                                          show_link = True,
                                                          )

            # Custom list fields
            list_fields = [(T("ID"), "person_id$pe_label"),
                           "person_id$first_name",
                           "person_id$last_name",
                           "need_id",
                           "emergency",
                           "status_id",
                           ]
            resource.configure(list_fields=list_fields)

        return result
    s3.prep = custom_prep

    return attr

# -------------------------------------------------------------------------
def dvr_case_appointment_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.dvr_case_appointment

    # Custom label for comments-field
    field = table.comments
    field.label = T("Details")
    field.comment = None

    # Organizer popups
    if r.tablename == "pr_person":
        title = "type_id"
        description = ["type_id",
                       "status",
                       "comments",
                       ]
    elif r.tablename == "dvr_case_appointment":
        title = "person_id"
        description = [(T("ID"), "person_id$pe_label"),
                       "type_id",
                       "status",
                       "comments",
                       ]
    else:
        title = description = None

    # Configure Organizer
    if title:
        s3db.configure("dvr_case_appointment",
                       organize = {"start": "date",
                                   "title": title,
                                   "description": description,
                                   # Color by status
                                   "color": "status",
                                   "colors": {
                                       1: "#ffaa00", # required (amber)
                                       2: "#10427b", # planned (blue)
                                       3: "#009f00", # in progress (light green)
                                       4: "#006100", # completed (green)
                                       5: "#d10000", # missed (red)
                                       6: "#666",    # canceled (gray)
                                       7: "#666",    # not required (gray)
                                       }
                                   },
                       )

# -------------------------------------------------------------------------
def dvr_case_appointment_controller(**attr):

    T = current.T

    db = current.db
    s3db = current.s3db
    auth = current.auth

    s3 = current.response.s3

    current.deployment_settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def custom_prep(r):

        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        resource = r.resource

        # Filter to active cases
        if not r.record:
            query = (FS("person_id$dvr_case.archived") == False) | \
                    (FS("person_id$dvr_case.archived") == None)
            resource.add_filter(query)

        # Filter for org-specific appointment types
        # - not necessary since appointments are within the realm of
        #   the type-defining organisation anyway, so permissions will
        #   filter anyway

        if not r.component:

            configure_person_tags()

            if r.interactive and not r.id:

                # Which organisation can the user see appointments for?
                permissions = current.auth.permission
                permitted_realms = permissions.permitted_realms("dvr_case_appointment", "read")
                if permitted_realms is not None:
                    otable = s3db.org_organisation
                    query = (otable.pe_id.belongs(permitted_realms)) & \
                            (otable.deleted == False)
                    organisations = db(query).select(otable.id)
                    organisation_ids = [o.id for o in organisations]
                else:
                    organisation_ids = None # global access

                # Which shelters can the user see appointments for?
                if organisation_ids is None or organisation_ids:
                    stable = s3db.cr_shelter
                    query = (stable.status != 1) & (stable.deleted == False)
                    if organisation_ids:
                        query = stable.organisation_id.belongs(organisation_ids) & query
                    shelters = db(query).select(stable.id, stable.name)
                    shelter_filter_opts = {s.id: s.name for s in shelters}
                else:
                    shelters = shelter_filter_opts = None # no shelters available

                # Which appointment types can the user see?
                ttable = s3db.dvr_case_appointment_type
                query = auth.s3_accessible_query("read", "dvr_case_appointment_type")
                if organisation_ids:
                    query = ttable.organisation_id.belongs(organisation_ids) & query
                types = db(query).select(ttable.id, ttable.name)
                type_filter_opts = {t.id: t.name for t in types}

                # Filter widgets
                filter_widgets = [
                    TextFilter(["person_id$pe_label",
                                "person_id$first_name",
                                "person_id$last_name",
                                ],
                                label = T("Search"),
                                ),
                    OptionsFilter("type_id",
                                  options = type_filter_opts,
                                  cols = 3,
                                  ),
                    OptionsFilter("status",
                                  options = s3db.dvr_appointment_status_opts,
                                  default = 2,
                                  ),

                    TextFilter(["person_id$pe_label"],
                               label = T("IDs"),
                               match_any = True,
                               hidden = True,
                               comment = T("Search for multiple IDs (separated by blanks)"),
                               ),
                    OptionsFilter("person_id$dvr_case.status_id$is_closed",
                                  cols = 2,
                                  default = False,
                                  hidden = True,
                                  label = T("Case Closed"),
                                  options = {True: T("Yes"), False: T("No")},
                                  ),
                    ]
                if r.method != "organize":
                    now = r.utcnow
                    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    tomorrow = today + datetime.timedelta(days=1)
                    filter_widgets.insert(-2, DateFilter("date",
                                                        default = {"ge": today,
                                                                   "le": tomorrow,
                                                                   },
                                              ))

                # Add organisation filter if user can see appointments
                # from more than one org
                if organisation_ids is None or len(organisation_ids) > 1:
                    filter_widgets.insert(-2,
                        OptionsFilter("person_id$dvr_case.organisation_id",
                                      hidden = True,
                                      ))

                # Add shelter filter if user can see appointments from
                # more than one shelter
                if shelter_filter_opts:
                    filter_widgets.insert(-2,
                        OptionsFilter("person_id$shelter_registration.shelter_id",
                                      options = shelter_filter_opts,
                                      hidden = True,
                                      ))

                resource.configure(filter_widgets = filter_widgets)

            # Custom list fields
            list_fields = [(T("ID"), "person_id$pe_label"),
                           "person_id$first_name",
                           "person_id$last_name",
                           "type_id",
                           "date",
                           "status",
                           "comments",
                           ]

            #if r.representation in ("xlsx", "xls"):
            #    # Include Person UUID for bulk status update
            #    list_fields.append(("UUID", "person_id$uuid"))

            resource.configure(list_fields = list_fields,
                               insertable = False,
                               deletable = False,
                               update_next = r.url(method=""),
                               )

        return result
    s3.prep = custom_prep

    return attr

# -------------------------------------------------------------------------
def case_event_report_default_filters(event_code=None):
    """
        Set default filters for case event report

        Args:
            event_code: code for the default event type
    """

    if event_code:
        ttable = current.s3db.dvr_case_event_type

        if event_code[-1] == "*":
            query = (ttable.code.like("%s%%" % event_code[:-1])) & \
                    (ttable.is_inactive == False)
            if event_code[:-1] == "FOOD":
                # Include SURPLUS-MEALS events
                query |= (ttable.code == "SURPLUS-MEALS")
        else:
            query = (ttable.code == event_code)
        query &= (ttable.deleted == False)

        rows = current.db(query).select(ttable.id)
        event_ids = [row.id for row in rows]
        if event_ids:
            set_default_filter("~.type_id",
                               event_ids,
                               tablename = "dvr_case_event",
                               )

    # Minimum date: one week
    WEEK_AGO = datetime.datetime.now() - \
                datetime.timedelta(days=7)
    min_date = WEEK_AGO.replace(hour=7, minute=0, second=0)

    set_default_filter("~.date",
                       {"ge": min_date,
                        },
                       tablename = "dvr_case_event",
                       )

# -------------------------------------------------------------------------
def dvr_case_event_resource(r, tablename):

    s3db = current.s3db

    from ..food import FoodDistribution
    s3db.set_method("dvr_case_event",
                    method = "register_food",
                    action = FoodDistribution,
                    )

    #s3db.add_custom_callback("dvr_case_event",
    #                         "onaccept",
    #                         case_event_create_onaccept,
    #                         method = "create",
    #                         )

# -------------------------------------------------------------------------
def dvr_case_event_controller(**attr):

    T = current.T
    s3 = current.response.s3

    standard_prep = s3.prep
    def custom_prep(r):
        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        resource = r.resource
        table = resource.table

        if r.method == "report":
            # Set report default filters
            event_code = r.get_vars.get("code")
            case_event_report_default_filters(event_code)

            dates = MRCMSCaseEventDateAxes()

            # Field method for day-date of events
            table.date_day = s3_fieldmethod(
                                "date_day",
                                dates.case_event_date_day,
                                represent = dates.case_event_date_day_represent,
                                )
            table.date_tod = s3_fieldmethod(
                                "date_tod",
                                dates.case_event_time_of_day,
                                )

            # Pivot axis options
            report_axes = ["type_id",
                           (T("Date"), "date_day"),
                           (T("Time of Day"), "date_tod"),
                           "created_by",
                           ]

            # Configure report options
            code = r.get_vars.get("code")
            if code and code[-1] != "*":
                # Single event type => group by ToD (legacy)
                default_cols = "date_tod"
            else:
                # Group by type (standard behavior)
                default_cols = "type_id"
            report_options = {
                "rows": report_axes,
                "cols": report_axes,
                "fact": [(T("Total Quantity"), "sum(quantity)"),
                         #(T("Number of Events"), "count(id)"),
                         ],
                "defaults": {"rows": "date_day",
                             "cols": default_cols,
                             "fact": "sum(quantity)",
                             "totals": True,
                             },
                }
            resource.configure(report_options = report_options,
                               extra_fields = ["date",
                                               "person_id",
                                               "type_id",
                                               ],
                               )
        return result
    s3.prep = custom_prep

    # Custom postp
    standard_postp = s3.postp
    def custom_postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if r.method in ("register", "register_food"):
            CustomController._view("MRCMS", "register_case_event.html")
        return output
    s3.postp = custom_postp

    return attr

# -------------------------------------------------------------------------
def managed_orgs_field():
    """
        Returns a Field with an organisation selector, to be used
        for imports of organisation-specific types
    """

    db = current.db
    s3db = current.s3db
    auth = current.auth

    from ..helpers import get_managed_orgs

    if auth.s3_has_role("ADMIN"):
        dbset = db
    else:
        managed_orgs = []
        for role in ("ORG_GROUP_ADMIN", "ORG_ADMIN"):
            if auth.s3_has_role(role):
                managed_orgs = get_managed_orgs(role=role)
        otable = s3db.org_organisation
        dbset = db(otable.id.belongs(managed_orgs))

    field = Field("organisation_id", "reference org_organisation",
                  requires = IS_ONE_OF(dbset, "org_organisation.id", "%(name)s"),
                  represent = s3db.org_OrganisationRepresent(),
                  )
    return field

# -------------------------------------------------------------------------
def dvr_case_appointment_type_controller(**attr):

    T = current.T
    auth = current.auth

    s3 = current.response.s3

    # Selectable organisation
    attr["csv_extra_fields"] = [{"label": "Organisation",
                                 "field": managed_orgs_field(),
                                 }]

    # Custom postp
    standard_postp = s3.postp
    def postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        # Import-button
        if not r.record and not r.method and auth.s3_has_permission("create", "dvr_case_appointment_type"):
            if isinstance(output, dict):
                import_btn = A(T("Import"),
                               _href = r.url(method="import"),
                               _class = "action-btn activity button",
                               )
                showadd_btn = output.get("showadd_btn")
                if showadd_btn:
                    output["showadd_btn"] = TAG[""](import_btn, showadd_btn)
                else:
                    output["showadd_btn"] = import_btn

        return output
    s3.postp = postp

    return attr

# -------------------------------------------------------------------------
def dvr_case_event_type_resource(r, tablename):

    T = current.T

    s3db = current.s3db

    # TODO filter case event exclusion to types of same org
    #      if we have a r.record, otherwise OptionsFilterS3?

    crud_form = S3SQLCustomForm("organisation_id",
                                "event_class",
                                "code",
                                "name",
                                "is_inactive",
                                "is_default",
                                "residents_only",
                                "register_multiple",
                                "role_required",
                                "appointment_type_id",
                                "min_interval",
                                "max_per_day",
                                S3SQLInlineLink("excluded_by",
                                                field = "excluded_by_id",
                                                label = T("Not Combinable With"),
                                                comment = T("Events that exclude registration of this event type on the same day"),
                                                ),
                                "presence_required",
                                )

    s3db.configure("dvr_case_event_type",
                   crud_form = crud_form,
                   )

# -------------------------------------------------------------------------
def dvr_case_event_type_controller(**attr):

    T = current.T

    db = current.db
    auth = current.auth

    s3 = current.response.s3

    # Selectable organisation
    attr["csv_extra_fields"] = [{"label": "Organisation",
                                 "field": managed_orgs_field(),
                                 }]

    standard_prep = s3.prep
    def prep(r):
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource
        table = resource.table

        # Restrict role_required to managed roles
        from core import S3RoleManager
        managed_roles = S3RoleManager.get_managed_roles(auth.user.id)
        roles = {k for k, v in managed_roles.items() if v["a"]}

        rtable = auth.settings.table_group
        dbset = db(rtable.id.belongs(roles))

        field = table.role_required
        field.requires = IS_EMPTY_OR(IS_ONE_OF(dbset, "%s.id" % rtable,
                                               field.represent,
                                               ))
        return result
    s3.prep = prep

    # Custom postp
    standard_postp = s3.postp
    def postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        # Import-button
        if not r.record and not r.method and auth.s3_has_permission("create", "dvr_case_event_type"):
            if isinstance(output, dict):
                import_btn = A(T("Import"),
                               _href = r.url(method="import"),
                               _class = "action-btn activity button",
                               )
                showadd_btn = output.get("showadd_btn")
                if showadd_btn:
                    output["showadd_btn"] = TAG[""](import_btn, showadd_btn)
                else:
                    output["showadd_btn"] = import_btn

        return output
    s3.postp = postp

    return attr

# -------------------------------------------------------------------------
def dvr_case_flag_controller(**attr):

    T = current.T
    auth = current.auth

    s3 = current.response.s3

    # Selectable organisation
    attr["csv_extra_fields"] = [{"label": "Organisation",
                                 "field": managed_orgs_field(),
                                 }]

    # Custom postp
    standard_postp = s3.postp
    def postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        # Import-button
        if not r.record and not r.method and auth.s3_has_permission("create", "dvr_case_flag"):
            if isinstance(output, dict):
                import_btn = A(T("Import"),
                               _href = r.url(method="import"),
                               _class = "action-btn activity button",
                               )
                showadd_btn = output.get("showadd_btn")
                if showadd_btn:
                    output["showadd_btn"] = TAG[""](import_btn, showadd_btn)
                else:
                    output["showadd_btn"] = import_btn

        return output
    s3.postp = postp

    return attr

# -------------------------------------------------------------------------
def dvr_service_contact_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    table = s3db.dvr_service_contact

    field = table.type_id
    field.label = T("Type")

    field = table.organisation_id
    field.readable = field.writable = False

    field = table.organisation
    field.label = T("Organization")
    field.readable = field.writable = True

# -------------------------------------------------------------------------
def dvr_site_activity_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    s3db.set_method("dvr_site_activity",
                    method = "create",
                    action = MRCMSCreateSiteActivityReport,
                    )
    s3db.configure("dvr_site_activity",
                   listadd = False,
                   addbtn = True,
                   editable = False,
                   )

    crud_strings = current.response.s3.crud_strings
    crud_strings["dvr_site_activity"] = Storage(
        label_create = T("Create Residents Report"),
        title_display = T("Residents Report"),
        title_list = T("Residents Reports"),
        title_update = T("Edit Residents Report"),
        label_list_button = T("List Residents Reports"),
        label_delete_button = T("Delete Residents Report"),
        msg_record_created = T("Residents Report created"),
        msg_record_modified = T("Residents Report updated"),
        msg_record_deleted = T("Residents Report deleted"),
        msg_list_empty = T("No Residents Reports found"),
        )

# =============================================================================
def dvr_person_prep(r):
    """
        Prep-function for dvr/counsel person controller, replaces
        standard dvr/person prep so that it can be called from both
        dvr and counsel controllers
    """
    # TODO integrate in pr_person_controller?

    T = current.T

    db = current.db
    s3db = current.s3db

    s3 = current.response.s3
    settings = current.deployment_settings

    # Set the default case status
    s3db.dvr_case_default_status()

    # Filter to persons who have a case registered
    resource = r.resource
    resource.add_filter(FS("dvr_case.id") != None)

    get_vars = r.get_vars

    CASES = T("Cases")
    CURRENT = T("Current Cases")
    CLOSED = T("Closed Cases")

    # Filters to split case list
    if not r.record:

        # Filter to active/archived cases
        archived = get_vars.get("archived")
        if archived == "1":
            archived = True
            CASES = T("Archived Cases")
            query = FS("dvr_case.archived") == True
        else:
            archived = False
            query = (FS("dvr_case.archived") == False) | \
                    (FS("dvr_case.archived") == None)

        # Filter to open/closed cases
        closed = get_vars.get("closed")
        if closed == "only":
            # Show only closed cases
            CASES = CLOSED
            query &= FS("dvr_case.status_id$is_closed") == True
        elif closed not in {"1", "include"}:
            # Show only open cases (default)
            CASES = CURRENT
            query &= (FS("dvr_case.status_id$is_closed") == False) | \
                     (FS("dvr_case.status_id$is_closed") == None)

        resource.add_filter(query)
    else:
        archived = False

    # Should not be able to delete records in this view
    resource.configure(deletable = False)

    if r.component and r.id:
        ctable = r.component.table
        if "case_id" in ctable.fields and \
           str(ctable.case_id.type)[:18] == "reference dvr_case":

            # Find the Case ID
            dvr_case = s3db.dvr_case
            query = (dvr_case.person_id == r.id) & \
                    (dvr_case.deleted != True)
            cases = db(query).select(dvr_case.id, limitby=(0, 2))

            case_id = ctable.case_id
            if cases:
                # Set default
                case_id.default = cases.first().id
            if len(cases) == 1:
                # Only one case => hide case selector
                case_id.readable = case_id.writable = False
            else:
                # Configure case selector
                case_id.requires = IS_ONE_OF(db(query), "dvr_case.id",
                                             case_id.represent,
                                             )

    if r.interactive:
        s3.crud_strings["pr_person"] = Storage(
            label_create = T("Create Case"),
            title_display = T("Case Details"),
            title_list = CASES,
            title_update = T("Edit Case Details"),
            label_list_button = T("List Cases"),
            label_delete_button = T("Delete Case"),
            msg_record_created = T("Case added"),
            msg_record_modified = T("Case details updated"),
            msg_record_deleted = T("Case deleted"),
            msg_list_empty = T("No Cases currently registered")
            )

        component = r.component
        if not component:
            # Expose the "archived"-flag? (update forms only)
            if r.record and r.method != "read":
                ctable = s3db.dvr_case
                field = ctable.archived
                field.readable = field.writable = True

        elif component.tablename == "dvr_case_activity":

            person_id = r.record.id
            organisation_id = s3db.dvr_case_organisation(person_id)

            # Set default status
            s3db.dvr_case_activity_default_status()

            if settings.get_dvr_vulnerabilities():
                # Limit selectable vulnerabilities to case
                s3db.dvr_configure_case_vulnerabilities(person_id)

            if settings.get_dvr_manage_response_actions():

                # Set defaults for inline responses
                s3db.dvr_set_response_action_defaults()

                # Limit selectable response themes to case organisation
                if settings.get_dvr_response_themes():
                    s3db.dvr_configure_case_responses(organisation_id)

            # Configure CRUD form
            component.configure(crud_form=s3db.dvr_case_activity_form(r))

        elif component.tablename == "dvr_response_action":

            person_id = r.record.id
            organisation_id = s3db.dvr_case_organisation(person_id)

            # Set defaults
            s3db.dvr_set_response_action_defaults()

            if settings.get_dvr_vulnerabilities():
                # Limit selectable vulnerabilities to case
                s3db.dvr_configure_case_vulnerabilities(person_id)

            # Limit selectable response themes to case organisation
            if settings.get_dvr_response_themes():
                s3db.dvr_configure_case_responses(organisation_id)

        elif component.tablename == "dvr_vulnerability":

            person_id = r.record.id
            organisation_id = s3db.dvr_case_organisation(person_id)

            # Limit vulnerabilities by case organisation sectors
            s3db.dvr_configure_vulnerability_types(organisation_id)

            # Set default human_resource_id
            field = component.table.human_resource_id
            field.default = current.auth.s3_logged_in_human_resource()

    return True

# =============================================================================
def dvr_group_membership_prep(r):
    # TODO docstring
    # TODO integrate in pr_group_membership_controller?

    db = current.db
    s3db = current.s3db

    table = r.table
    resource = r.resource

    # Hide unwanted fields from PersonSelector
    settings = current.deployment_settings
    settings.pr.request_email = False
    settings.pr.request_home_phone = False
    settings.hrm.email_required = False

    get_vars = r.get_vars
    if "viewing" in get_vars:

        try:
            vtablename, record_id = get_vars["viewing"].split(".")
        except ValueError:
            return False

        if vtablename == "pr_person":

            # Get all group_ids with this person_id
            gtable = s3db.pr_group
            join = gtable.on(gtable.id == table.group_id)
            query = (table.person_id == record_id) & \
                    (gtable.group_type == 7) & \
                    (table.deleted != True)
            rows = db(query).select(table.group_id, join=join)
            group_ids = set(row.group_id for row in rows)

            # Hide the link for this person (to prevent changes/deletion)
            if group_ids:
                # Single group ID?
                group_id = tuple(group_ids)[0] if len(group_ids) == 1 else None
            elif r.http == "POST":
                name = s3_fullname(record_id)
                group_id = gtable.insert(name=name, group_type=7)
                s3db.update_super(gtable, {"id": group_id})
                table.insert(group_id = group_id,
                             person_id = record_id,
                             group_head = True,
                             )
                group_ids = {group_id}
            resource.add_filter(FS("person_id") != record_id)

        else:
            group_ids = set()

        # Show only links for relevant cases
        # NB Filter also prevents showing all links if case_ids is empty
        if not r.id:
            if len(group_ids) == 1:
                r.resource.add_filter(FS("group_id") == group_id)
            else:
                r.resource.add_filter(FS("group_id").belongs(group_ids))

        list_fields = ["person_id",
                       "person_id$gender",
                       "person_id$date_of_birth",
                       ]

        if len(group_ids) == 0:
            # No case group exists, will be auto-generated on POST,
            # hide the field in the form:
            field = table.group_id
            field.readable = field.writable = False
        elif len(group_ids) == 1:
            field = table.group_id
            field.default = group_id
            # If we have only one relevant case, then hide the group ID:
            field.readable = field.writable = False
        elif len(group_ids) > 1:
            # Show the case ID in list fields if there is more than one
            # relevant case
            list_fields.insert(0, "group_id")
        r.resource.configure(list_fields = list_fields)

    # Do not allow update of person_id
    if r.id:
        field = table.person_id
        field.writable = False
        field.comment = None

    return True

# =============================================================================
class MRCMSCaseEventDateAxes:
    """
        Helper class for virtual date axes in case event statistics
    """

    def __init__(self):
        """
            Perform all slow lookups outside of the field methods
        """

        # Get timezone descriptions
        self.UTC = tz.tzutc()
        self.LOCAL = tz.gettz("Europe/Berlin")

        # Lookup FOOD event type_id
        table = current.s3db.dvr_case_event_type
        query = (table.code == "FOOD") & \
                (table.deleted != True)
        row = current.db(query).select(table.id, limitby=(0, 1)).first()
        self.FOOD = row.id if row else None

        self.SURPLUS_MEALS = s3_str(current.T("Surplus Meals"))

    # -------------------------------------------------------------------------
    def case_event_date_day(self, row):
        """
            Field method to reduce case event date/time to just date,
            used in pivot table reports to group case events by day
        """

        if hasattr(row, "dvr_case_event"):
            row = row.dvr_case_event

        try:
            date = row.date
        except AttributeError:
            date = None

        if date:
            # Get local hour
            date = date.replace(tzinfo=self.UTC).astimezone(self.LOCAL)
            hour = date.time().hour

            # Convert to date
            date = date.date()
            if hour <= 7:
                # Map early hours to previous day
                return date - datetime.timedelta(days=1)
        else:
            date = None
        return date

    # -------------------------------------------------------------------------
    @staticmethod
    def case_event_date_day_represent(value):
        """
            Representation method for case_event_date_day, needed in order
            to sort pivot axis values by raw date, but show them in locale
            format (default DD.MM.YYYY, doesn't sort properly).
        """

        return S3DateTime.date_represent(value, utc=True)

    # -------------------------------------------------------------------------
    def case_event_time_of_day(self, row):
        """
            Field method to group events by time of day
        """

        if hasattr(row, "dvr_case_event"):
            row = row.dvr_case_event

        try:
            date = row.date
        except AttributeError:
            date = None

        if date:
            try:
                person_id = row.person_id
                type_id = row.type_id
            except AttributeError:
                person_id = 0
                type_id = None

            if type_id == self.FOOD and person_id is None:
                tod = self.SURPLUS_MEALS
            else:
                date = date.replace(tzinfo=self.UTC).astimezone(self.LOCAL)
                hour = date.time().hour

                if 7 <= hour < 13:
                    tod = "07:00 - 13:00"
                elif 13 <= hour < 17:
                    tod = "13:00 - 17:00"
                elif 17 <= hour < 20:
                    tod = "17:00 - 20:00"
                else:
                    tod = "20:00 - 07:00"
        else:
            tod = "-"
        return tod

# =============================================================================
class MRCMSCreateSiteActivityReport(CRUDMethod):
    """ Custom method to create a dvr_site_activity entry """

    def apply_method(self, r, **attr):
        """
            Entry point for REST controller

            Args:
                r: the CRUDRequest
                attr: dict of controller parameters
        """

        if r.representation in ("html", "iframe"):
            if r.http in ("GET", "POST"):
                output = self.create_form(r, **attr)
            else:
                r.error(405, current.ERROR.BAD_METHOD)
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    def create_form(self, r, **attr):
        """
            Generate and process the form

            Args:
                r: the CRUDRequest
                attr: dict of controller parameters
        """

        # User must be permitted to create site activity reports
        authorised = self._permitted(method="create")
        if not authorised:
            r.unauthorised()

        s3db = current.s3db

        T = current.T
        response = current.response
        settings = current.deployment_settings

        # Page title
        output = {"title": T("Create Residents Report")}

        # Form fields
        table = s3db.dvr_site_activity
        table.date.default = r.utcnow.date()
        formfields = [table.site_id,
                      table.date,
                      ]

        # Form buttons
        submit_btn = INPUT(_class = "tiny primary button",
                           _name = "submit",
                           _type = "submit",
                           _value = T("Create Report"),
                           )
        cancel_btn = A(T("Cancel"),
                       _href = r.url(id=None, method=""),
                       _class = "action-lnk",
                       )
        buttons = [submit_btn, cancel_btn]

        # Generate the form and add it to the output
        resourcename = r.resource.name
        formstyle = settings.get_ui_formstyle()
        form = SQLFORM.factory(record = None,
                               showid = False,
                               formstyle = formstyle,
                               table_name = resourcename,
                               buttons = buttons,
                               *formfields)
        output["form"] = form

        # Process the form
        formname = "%s/manage" % resourcename
        if form.accepts(r.post_vars,
                        current.session,
                        formname = formname,
                        onvalidation = self.validate,
                        keepvalues = False,
                        hideerror = False,
                        ):

            from ..helpers import MRCMSSiteActivityReport
            formvars = form.vars
            report = MRCMSSiteActivityReport(site_id = formvars.site_id,
                                           date = formvars.date,
                                           )
            try:
                record_id = report.store()
            except S3PermissionError:
                # Redirect to list view rather than index page
                current.auth.permission.homepage = r.url(id=None, method="")
                r.unauthorised()

            r.resource.lastid = str(record_id)
            set_last_record_id("dvr_site_activity", record_id)

            current.response.confirmation = T("Report created")
            self.next = r.url(id=record_id, method="read")

        response.view = self._view(r, "create.html")

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def validate(form):
        """
            Validate the form

            Args:
                form: the FORM
        """

        T = current.T
        formvars = form.vars

        if "site_id" in formvars:
            site_id = formvars.site_id
        else:
            # Fall back to default site
            site_id = current.deployment_settings.get_org_default_site()
        if not site_id:
            form.errors["site_id"] = T("No site specified")
        formvars.site_id = site_id

        if "date" in formvars:
            date = formvars.date
        else:
            # Fall back to today
            date = current.request.utcnow.date()
        formvars.date = date

# END =========================================================================
