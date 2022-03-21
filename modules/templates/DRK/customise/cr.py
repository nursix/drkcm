"""
    CR module customisations for DRK

    License: MIT
"""

from gluon import current, URL, \
                  A, DIV, H2, H3, H4, P, TABLE, TR, TD, XML, HR

from core import IS_ONE_OF

# -------------------------------------------------------------------------
def check_in_status(site, person):
    """
        Determine the current check-in status for a person

        Args:
            site: the site record (instance!)
            person: the person record

        See also:
            org_SiteCheckInMethod for details of the return value
    """

    T = current.T

    db = current.db
    s3db = current.s3db

    result = {"valid": False,
              "check_in_allowed": False,
              "check_out_allowed": False,
              }
    person_id = person.id

    # Check the case status
    ctable = s3db.dvr_case
    cstable = s3db.dvr_case_status
    query = (ctable.person_id == person_id) & \
            (cstable.id == ctable.status_id)
    status = db(query).select(cstable.is_closed,
                              limitby = (0, 1),
                              ).first()

    if status and status.is_closed:
        result["error"] = T("Not currently a resident")
        return result

    # Find the Registration
    stable = s3db.cr_shelter
    rtable = s3db.cr_shelter_registration
    query = (stable.site_id == site.site_id) & \
            (stable.id == rtable.shelter_id) & \
            (rtable.person_id == person_id) & \
            (rtable.deleted != True)
    registration = db(query).select(rtable.id,
                                    rtable.registration_status,
                                    limitby=(0, 1),
                                    ).first()
    if not registration:
        result["error"] = T("Registration not found")
        return result

    result["valid"] = True

    # Check current status
    reg_status = registration.registration_status
    if reg_status == 2:
        # Currently checked-in at this site
        status = 1
    elif reg_status == 3:
        # Currently checked-out from this site
        status = 2
    else:
        # No previous status
        status = None
    result["status"] = status

    check_in_allowed = True
    check_out_allowed = True

    # Check if we have any case flag to deny check-in or to show advise
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
        if flag.deny_check_in:
            check_in_allowed = False
        if flag.deny_check_out:
            check_out_allowed = False

        # Show flag instructions?
        if status == 1:
            advise = flag.advise_at_check_out
        elif status == 2:
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

    result["check_in_allowed"] = check_in_allowed
    result["check_out_allowed"] = check_out_allowed

    return result

# -------------------------------------------------------------------------
def site_check_in(site_id, person_id):
    """
        When a person is checked-in to a Shelter then update the
        Shelter Registration

        Args:
            site_id: the site_id of the shelter
            person_id: the person_id to check-in
    """

    s3db = current.s3db
    db = current.db

    # Find the Registration
    stable = s3db.cr_shelter
    rtable = s3db.cr_shelter_registration

    query = (stable.site_id == site_id) & \
            (stable.id == rtable.shelter_id) & \
            (rtable.person_id == person_id) & \
            (rtable.deleted != True)
    registration = db(query).select(rtable.id,
                                    rtable.registration_status,
                                    limitby = (0, 1)
                                    ).first()
    if not registration:
        return

    # Update the Shelter Registration
    registration.update_record(check_in_date = current.request.utcnow,
                               registration_status = 2,
                               )
    s3db.onaccept("cr_shelter_registration", registration, method="update")

# -------------------------------------------------------------------------
def site_check_out(site_id, person_id):
    """
        When a person is checked-out from a Shelter then update the
        Shelter Registration

        Args:
            site_id: the site_id of the shelter
            person_id: the person_id to check-in
    """

    s3db = current.s3db
    db = current.db

    # Find the Registration
    stable = s3db.cr_shelter
    rtable = s3db.cr_shelter_registration
    query = (stable.site_id == site_id) & \
            (stable.id == rtable.shelter_id) & \
            (rtable.person_id == person_id) & \
            (rtable.deleted != True)
    registration = db(query).select(rtable.id,
                                    rtable.registration_status,
                                    limitby = (0, 1)
                                    ).first()
    if not registration:
        return

    # Update the Shelter Registration
    registration.update_record(check_out_date = current.request.utcnow,
                               registration_status = 3,
                               )
    s3db.onaccept("cr_shelter_registration", registration, method="update")

# -------------------------------------------------------------------------
def cr_shelter_controller(**attr):

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def custom_prep(r):
        # Call standard prep
        if callable(standard_prep):
            result = standard_prep(r)
        else:
            result = True

        if r.method == "check-in":
            # Configure check-in methods
            current.s3db.configure("cr_shelter",
                                   site_check_in = site_check_in,
                                   site_check_out = site_check_out,
                                   check_in_status = check_in_status,
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
                    pois = dict(active = True,
                                layer_id = layer_id,
                                name = current.T("Buildings"),
                                id = "profile-header-%s-%s" % ("gis_poi", r.id),
                                )
                    profile_layers = s3db.get_config("cr_shelter", "profile_layers")
                    profile_layers += (pois,)
                    s3db.configure("cr_shelter",
                                   profile_layers = profile_layers,
                                   )
            else:
                has_role = current.auth.s3_has_role
                if has_role("SECURITY") and not has_role("ADMIN"):
                    # Security can access nothing in cr/shelter except
                    # Dashboard and Check-in/out UI
                    current.auth.permission.fail()

            if r.interactive:

                resource = r.resource
                resource.configure(filter_widgets = None,
                                   insertable = False,
                                   deletable = False,
                                   )

        if r.component_name == "shelter_unit":
            # Expose "transitory" flag for housing units
            utable = current.s3db.cr_shelter_unit
            field = utable.transitory
            field.readable = field.writable = True
            list_fields = ["name",
                           "transitory",
                           "capacity",
                           "population",
                           "available_capacity",
                           ]
            r.component.configure(list_fields=list_fields)

        return result
    s3.prep = custom_prep

    # Custom postp
    standard_postp = s3.postp
    def custom_postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        # Hide side menu and rheader for check-in
        if r.method == "check-in":
            current.menu.options = None
            if isinstance(output, dict):
                output["rheader"] = ""

        # Custom view for shelter inspection
        if r.method == "inspection":
            from core import CustomController
            CustomController._view("DRK", "shelter_inspection.html")

        return output
    s3.postp = custom_postp

    from ..rheaders import drk_cr_rheader
    attr = dict(attr)
    attr["rheader"] = drk_cr_rheader

    return attr

# -------------------------------------------------------------------------
def cr_shelter_registration_resource(r, tablename):

    table = current.s3db.cr_shelter_registration
    field = table.shelter_unit_id

    # Filter to available housing units
    from gluon import IS_EMPTY_OR
    field.requires = IS_EMPTY_OR(IS_ONE_OF(current.db, "cr_shelter_unit.id",
                                           field.represent,
                                           filterby = "status",
                                           filter_opts = (1,),
                                           orderby = "shelter_id",
                                           ))

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

            from ..helpers import drk_default_shelter

            # Prep runs before split into create/update (Create should never happen in Village)
            table = r.table
            shelter_id = drk_default_shelter()
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

# -------------------------------------------------------------------------
def profile_header(r):
    """
        Profile Header for Shelter Profile page
    """

    T = current.T
    db = current.db
    s3db = current.s3db

    rtable = s3db.cr_shelter_registration
    utable = s3db.cr_shelter_unit
    ctable = s3db.dvr_case
    stable = s3db.dvr_case_status

    record = r.record
    if not record:
        return ""

    shelter_id = record.id

    # Get nostats flags
    ftable = s3db.dvr_case_flag
    query = (ftable.nostats == True) & \
            (ftable.deleted == False)
    rows = db(query).select(ftable.id)
    nostats = set(row.id for row in rows)

    # Get person_ids with nostats-flags
    # (=persons who are registered as residents, but not BEA responsibility)
    if nostats:
        ltable = s3db.dvr_case_flag_case
        query = (ltable.flag_id.belongs(nostats)) & \
                (ltable.deleted == False)
        rows = db(query).select(ltable.person_id)
        exclude = set(row.person_id for row in rows)
    else:
        exclude = set()

    # Count total shelter registrations for non-BEA persons
    query = (rtable.person_id.belongs(exclude)) & \
            (rtable.shelter_id == shelter_id) & \
            (rtable.deleted != True)
    other_total = db(query).count()

    # Count number of shelter registrations for this shelter,
    # grouped by transitory-status of the housing unit
    left = utable.on(utable.id == rtable.shelter_unit_id)
    query = (~(rtable.person_id.belongs(exclude))) & \
            (rtable.shelter_id == shelter_id) & \
            (rtable.deleted != True)
    count = rtable.id.count()
    rows = db(query).select(utable.transitory,
                            count,
                            groupby = utable.transitory,
                            left = left,
                            )
    transitory = 0
    regular = 0
    for row in rows:
        if row[utable.transitory]:
            transitory += row[count]
        else:
            regular += row[count]
    total = transitory + regular

    # Children
    from dateutil.relativedelta import relativedelta
    EIGHTEEN = r.utcnow - relativedelta(years=18)
    ptable = s3db.pr_person
    query = (ptable.date_of_birth > EIGHTEEN) & \
            (~(ptable.id.belongs(exclude))) & \
            (ptable.id == rtable.person_id) & \
            (rtable.shelter_id == shelter_id)
    count = ptable.id.count()
    row = db(query).select(count).first()
    children = row[count]

    CHILDREN = TR(TD(T("Children")),
                  TD(children),
                  )

    # Families on-site
    gtable = s3db.pr_group
    mtable = s3db.pr_group_membership
    join = [mtable.on((~(mtable.person_id.belongs(exclude))) & \
                      (mtable.group_id == gtable.id) & \
                      (mtable.deleted != True)),
            rtable.on((rtable.person_id == mtable.person_id) & \
                      (rtable.shelter_id == shelter_id) & \
                      (rtable.deleted != True)),
            ]
    query = (gtable.group_type == 7) & \
            (gtable.deleted != True)

    rows = db(query).select(gtable.id,
                            having = (mtable.id.count() > 1),
                            groupby = gtable.id,
                            join = join,
                            )
    families = len(rows)
    FAMILIES = TR(TD(T("Families")),
                  TD(families),
                  )

    TOTAL = TR(TD(T("Population BEA")),
               TD(total),
               _class="dbstats-total",
               )
    TRANSITORY = TR(TD(T("in staging area (PX)")),
                    TD(transitory),
                    _class="dbstats-sub",
                    )
    REGULAR = TR(TD(T("in housing units")),
                 TD(regular),
                 _class="dbstats-sub",
                 )

    OTHER = TR(TD(T("Population Other")),
               TD(other_total),
               _class="dbstats-extra",
               )

    # Get the IDs of open case statuses
    query = (stable.is_closed == False) & (stable.deleted != True)
    rows = db(query).select(stable.id)
    OPEN = set(row.id for row in rows)

    # Count number of external persons
    ftable = s3db.dvr_case_flag
    ltable = s3db.dvr_case_flag_case
    left = [ltable.on((ltable.flag_id == ftable.id) & \
                      (ltable.deleted != True)),
            ctable.on((ctable.person_id == ltable.person_id) & \
                      (~(ctable.person_id.belongs(exclude))) & \
                      (ctable.status_id.belongs(OPEN)) & \
                      ((ctable.archived == False) | (ctable.archived == None)) & \
                      (ctable.deleted != True)),
            rtable.on((rtable.person_id == ltable.person_id) & \
                      (rtable.deleted != True)),
            ]
    query = (ftable.is_external == True) & \
            (ftable.deleted != True) & \
            (ltable.id != None) & \
            (ctable.id != None) & \
            (rtable.shelter_id == shelter_id)
    count = ctable.id.count()
    rows = db(query).select(count, left=left)
    external = rows.first()[count] if rows else 0

    EXTERNAL = TR(TD(T("External (Hospital / Police)")),
                  TD(external),
                  )

    # Get the number of free places in the BEA
    # => Non-BEA registrations do not occupy BEA capacity,
    #    so need to re-add the total here:
    free = record.available_capacity + other_total
    FREE = TR(TD(T("Free places")),
              TD(free),
              _class="dbstats-total",
              )

    # Announcements
    from s3db.cms import S3CMS
    resource_content = S3CMS.resource_content
    announce = resource_content("cr", "shelter", shelter_id,
                                hide_if_empty=True,
                                )

    # Weather (uses fake weather module/resource)
    table = s3db.cms_post
    ltable = db.cms_post_module
    query = (ltable.module == "weather") & \
            (ltable.resource == "weather") & \
            (ltable.record == shelter_id) & \
            (ltable.post_id == table.id) & \
            (table.deleted != True)
    _item = db(query).select(table.id,
                             table.body,
                             limitby=(0, 1)).first()
    auth = current.auth
    ADMIN = auth.get_system_roles().ADMIN
    ADMIN = auth.s3_has_role(ADMIN)
    if ADMIN:
        url_vars = {"module": "weather",
                    "resource": "weather",
                    "record": shelter_id,
                    # Custom redirect after CMS edit
                    # (required for fake module/resource)
                    "url": URL(c = "cr",
                               f = "shelter",
                               args = [shelter_id, "profile"],
                               ),
                    }
        EDIT_WEATHER = T("Edit Weather Widget")
        if _item:
            item = DIV(XML(_item.body),
                       A(EDIT_WEATHER,
                         _href=URL(c="cms", f="post",
                                   args = [_item.id, "update"],
                                   vars = url_vars,
                                   ),
                         _class="action-btn cms-edit",
                         ))
        else:
            item = A(EDIT_WEATHER,
                     _href=URL(c="cms", f="post",
                               args = "create",
                               vars = url_vars,
                               ),
                     _class="action-btn cms-edit",
                     )
    elif _item:
        item = XML(_item.body)
    else:
        item = ""

    weather = DIV(item, _id="cms_weather", _class="cms_content")

    # Show Check-in/Check-out action only if user is permitted
    # to update shelter registrations (NB controllers may be
    # read-only, therefore checking against default here):
    if auth.s3_has_permission("update",
                              "cr_shelter_registration",
                              c="default",
                              ):
        # Action button for check-in/out
        cico = A("%s / %s" % (T("Check-In"), T("Check-Out")),
                    _href=r.url(method="check-in"),
                    _class="action-btn dashboard-action",
                    )
    else:
        cico = ""

    # Generate profile header HTML
    output = DIV(H2(record.name),
                    P(record.comments or ""),
                    H3(T("Announcements")) if announce else "",
                    announce,
                    HR(),
                    # Current population overview
                    TABLE(TR(TD(TABLE(TOTAL,
                                      TRANSITORY,
                                      REGULAR,
                                      CHILDREN,
                                      FAMILIES,
                                      EXTERNAL,
                                      FREE,
                                      OTHER,
                                      _class="dbstats",
                                      ),
                                ),
                             TD(weather,
                                _class="show-for-large-up",
                                ),
                             ),
                          ),
                    cico,
                    _class="profile-header",
                    )

    return output

# END =========================================================================
