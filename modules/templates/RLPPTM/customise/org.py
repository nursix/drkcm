"""
    ORG module customisations for RLPPTM

    License: MIT
"""

from collections import OrderedDict

from gluon import current, URL, DIV, IS_EMPTY_OR, IS_IN_SET, IS_NOT_EMPTY

from core import FS, ICON, S3CRUD, S3Represent, \
                 get_filter_options, get_form_record_id, s3_fieldmethod

from ..helpers import workflow_tag_represent

SITE_WORKFLOW = ("MPAV", "HYGIENE", "LAYOUT", "STATUS", "PUBLIC")
SITE_REVIEW = ("MPAV", "HYGIENE", "LAYOUT")

# -------------------------------------------------------------------------
def add_org_tags():
    """
        Adds organisation tags as filtered components,
        for embedding in form, filtering and as report axis
    """

    s3db = current.s3db

    s3db.add_components("org_organisation",
                        org_organisation_tag = ({"name": "delivery",
                                                 "joinby": "organisation_id",
                                                 "filterby": {"tag": "DELIVERY"},
                                                 "multiple": False,
                                                 },
                                                {"name": "orgid",
                                                 "joinby": "organisation_id",
                                                 "filterby": {"tag": "OrgID"},
                                                 "multiple": False,
                                                 },
                                                {"name": "mgrinfo",
                                                 "joinby": "organisation_id",
                                                 "filterby": {"tag": "MGRINFO"},
                                                 "multiple": False,
                                                 },
                                                ),
                        )

# -------------------------------------------------------------------------
def mgrinfo_opts():
    """
        Options for the MGRINFO-tag, and their labels

        Returns:
            tuple list of options
    """

    T = current.T

    return (("N/A", T("not specified")),
            ("REVISE", T("Completion/Adjustment Required")),
            ("COMPLETE", T("complete")),
            )

# -------------------------------------------------------------------------
def configure_org_tags(resource):
    """
        Configures organisation tags
            - labels
            - selectable options
            - representation
    """

    T = current.T

    from ..requests import delivery_tag_opts

    # Configure delivery-tag
    delivery_opts = delivery_tag_opts()
    component = resource.components.get("delivery")
    ctable = component.table
    field = ctable.value
    field.default = "DIRECT"
    field.label = T("Delivery##supplying")
    field.requires = IS_IN_SET(delivery_opts, zero=None)
    field.represent = lambda v, row=None: delivery_opts.get(v, "-")

    # Configure mgrinfo-tag
    component = resource.components.get("mgrinfo")
    ctable = component.table
    field = ctable.value
    field.label = T("Documentation Test Station Manager")
    field.writable = False
    field.represent = workflow_tag_represent(dict(mgrinfo_opts()))

# -------------------------------------------------------------------------
def update_mgrinfo(organisation_id):
    """
        Updates the MGRINFO (Manager-Info) tag of a test station
        organisation.

        Args:
            organisation_id: the organisation ID

        Returns:
            the updated status (str)
    """

    from ..config import TESTSTATIONS
    from ..helpers import is_org_group

    # Check if the organisation belongs to the TESTSTATIONS group
    if not is_org_group(organisation_id, TESTSTATIONS):
        return None

    db = current.db
    s3db = current.s3db

    # Look up test station managers, and related data/tags
    htable = s3db.hrm_human_resource
    ptable = s3db.pr_person

    httable = s3db.hrm_human_resource_tag
    reg_tag = httable.with_alias("reg_tag")
    crc_tag = httable.with_alias("crc_tag")
    scp_tag = httable.with_alias("scp_tag")

    join = ptable.on(ptable.id == htable.person_id)
    left = [reg_tag.on((reg_tag.human_resource_id == htable.id) & \
                       (reg_tag.tag == "REGFORM") & \
                       (reg_tag.deleted == False)),
            crc_tag.on((crc_tag.human_resource_id == htable.id) & \
                       (crc_tag.tag == "CRC") & \
                       (crc_tag.deleted == False)),
            scp_tag.on((scp_tag.human_resource_id == htable.id) & \
                       (scp_tag.tag == "SCP") & \
                       (scp_tag.deleted == False)),
            ]

    query = (htable.organisation_id == organisation_id) & \
            (htable.org_contact == True) & \
            (htable.status == 1) & \
            (htable.deleted == False)

    rows = db(query).select(ptable.pe_id,
                            ptable.date_of_birth,
                            reg_tag.value,
                            crc_tag.value,
                            scp_tag.value,
                            join = join,
                            left = left,
                            )
    if not rows:
        # No managers selected
        status = "N/A"
    else:
        # Managers selected => check completeness of data/documentation
        status = "REVISE"
        ctable = s3db.pr_contact

        for row in rows:

            # Check that all documentation tags are set as approved
            doc_tags = True
            for t in (reg_tag, crc_tag, scp_tag):
                if row[t.value] != "APPROVED":
                    doc_tags = False
                    break
            if not doc_tags:
                continue

            # Check DoB
            if not row.pr_person.date_of_birth:
                continue

            # Check that there is at least one contact details
            # of phone/email type
            query = (ctable.pe_id == row.pr_person.pe_id) & \
                    (ctable.contact_method in ("SMS", "HOME_PHONE", "WORK_PHONE", "EMAIL")) & \
                    (ctable.value != None) & \
                    (ctable.deleted == False)
            contact = db(query).select(ctable.id, limitby=(0, 1)).first()
            if not contact:
                continue

            # All that given, the manager-data status of the organisation
            # can be set as complete
            status = "COMPLETE"
            break

    # Update or add MGRINFO-tag with status
    ottable = s3db.org_organisation_tag
    query = (ottable.organisation_id == organisation_id) & \
            (ottable.tag == "MGRINFO") & \
            (ottable.deleted == False)
    row = db(query).select(ottable.id, limitby=(0, 1)).first()
    if row:
        row.update_record(value=status)
        s3db.onaccept(ottable, row, method="update")
    else:
        tag = {"organisation_id": organisation_id,
               "tag": "MGRINFO",
               "value": status,
               }
        tag["id"] = ottable.insert(**tag)
        s3db.onaccept(ottable, tag, method="create")

    # Update test station approval workflow if MGRINFO is mandatory
    if current.deployment_settings.get_custom("test_station_manager_required"):
        facility_approval_update_mgrinfo(organisation_id, status)

    return status

# -----------------------------------------------------------------------------
def add_organisation_default_tags(organisation_id):
    """
        Adds default tags to a new organisation

        Args:
            organisation_id: the organisation record ID
    """

    db = current.db
    s3db = current.s3db

    # Look up current tags
    otable = s3db.org_organisation
    ttable = s3db.org_organisation_tag
    dttable = ttable.with_alias("delivery")
    ittable = ttable.with_alias("orgid")

    left = [dttable.on((dttable.organisation_id == otable.id) & \
                       (dttable.tag == "DELIVERY") & \
                       (dttable.deleted == False)),
            ittable.on((ittable.organisation_id == otable.id) & \
                       (ittable.tag == "OrgID") & \
                       (ittable.deleted == False)),
            ]
    query = (otable.id == organisation_id)
    row = db(query).select(otable.id,
                           otable.uuid,
                           dttable.id,
                           ittable.id,
                           left = left,
                           limitby = (0, 1),
                           ).first()
    if row:
        # Add default tags as required
        org = row.org_organisation

        # Add DELIVERY-tag
        dtag = row.delivery
        if not dtag.id:
            ttable.insert(organisation_id = org.id,
                          tag = "DELIVERY",
                          value = "DIRECT",
                          )
        # Add OrgID-tag
        itag = row.orgid
        if not itag.id:
            try:
                uid = int(org.uuid[9:14], 16)
            except (TypeError, ValueError):
                import uuid
                uid = int(uuid.uuid4().urn[9:14], 16)
            value = "%06d%04d" % (uid, org.id)
            ttable.insert(organisation_id = org.id,
                          tag = "OrgID",
                          value = value,
                          )
        # Set MGRINFO-tag
        update_mgrinfo(org.id)

# -------------------------------------------------------------------------
def organisation_create_onaccept(form):
    """
        Custom onaccept of organisations:
            - add default tags
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    add_organisation_default_tags(record_id)

# -------------------------------------------------------------------------
def org_organisation_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    # Add organisation tags
    add_org_tags()

    # Reports configuration
    if r.method == "report":
        axes = ["facility.location_id$L3",
                "facility.location_id$L2",
                "facility.location_id$L1",
                "facility.service_site.service_id",
                (T("Project"), "project.name"),
                (T("Organization Group"), "group_membership.group_id"),
                ]

        report_options = {
            "rows": axes,
            "cols": axes,
            "fact": [(T("Number of Organizations"), "count(id)"),
                     (T("Number of Facilities"), "count(facility.id)"),
                     ],
            "defaults": {"rows": "facility.location_id$L2",
                         "cols": None,
                         "fact": "count(id)",
                         "totals": True,
                         },
            }

        s3db.configure(tablename,
                       report_options = report_options,
                       )

    # Custom onaccept to create default tags
    s3db.add_custom_callback("org_organisation",
                             "onaccept",
                             organisation_create_onaccept,
                             method = "create",
                             )

# -------------------------------------------------------------------------
def org_organisation_controller(**attr):

    T = current.T
    s3 = current.response.s3
    settings = current.deployment_settings

    # Enable bigtable features
    settings.base.bigtable = True

    # Add managers component
    current.s3db.add_components("org_organisation",
                                hrm_human_resource = {"name": "managers",
                                                      "joinby": "organisation_id",
                                                      "filterby": {"org_contact": True,
                                                                   "status": 1, # active
                                                                   },
                                                      },
                                )

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        auth = current.auth
        s3db = current.s3db

        resource = r.resource

        is_org_group_admin = auth.s3_has_role("ORG_GROUP_ADMIN")

        # Configure organisation tags
        configure_org_tags(resource)

        # Add invite-method for ORG_GROUP_ADMIN role
        from ..helpers import InviteUserOrg
        s3db.set_method("org_organisation",
                        method = "invite",
                        action = InviteUserOrg,
                        )

        get_vars = r.get_vars
        mine = get_vars.get("mine")
        if mine == "1":
            # Filter to managed orgs
            managed_orgs = auth.get_managed_orgs()
            if managed_orgs is True:
                query = None
            elif managed_orgs is None:
                query = FS("id") == None
            else:
                query = FS("pe_id").belongs(managed_orgs)
            if query:
                resource.add_filter(query)
        else:
            # Filter by org_group_membership
            org_group_id = get_vars.get("g")
            if org_group_id:
                if isinstance(org_group_id, list):
                    query = FS("group.id").belongs(org_group_id)
                else:
                    query = FS("group.id") == org_group_id
                resource.add_filter(query)

        record = r.record
        component_name = r.component_name
        if not r.component:
            if r.interactive:

                ltable = s3db.project_organisation
                field = ltable.project_id
                field.represent = S3Represent(lookup="project_project")

                from core import S3SQLCustomForm, \
                                 S3SQLInlineComponent, \
                                 S3SQLInlineLink, \
                                 OptionsFilter, \
                                 TextFilter

                # Custom form
                if is_org_group_admin:
                    user = auth.user
                    if record and user:
                        # Only OrgGroupAdmins managing this organisation can change
                        # its org group membership (=organisation must be within realm):
                        realm = user.realms.get(auth.get_system_roles().ORG_GROUP_ADMIN)
                        groups_readonly = realm is not None and record.pe_id not in realm
                    else:
                        groups_readonly = False

                    # Show organisation types
                    types = S3SQLInlineLink("organisation_type",
                                            field = "organisation_type_id",
                                            search = False,
                                            label = T("Type"),
                                            multiple = settings.get_org_organisation_types_multiple(),
                                            widget = "multiselect",
                                            )

                    # Show org groups and projects
                    groups = S3SQLInlineLink("group",
                                             field = "group_id",
                                             label = T("Organization Group"),
                                             multiple = False,
                                             readonly = groups_readonly,
                                             )
                    projects = S3SQLInlineLink("project",
                                               field = "project_id",
                                               label = T("Project Partner for"),
                                               cols = 1,
                                               )

                    # Show delivery-tag
                    delivery = "delivery.value"

                else:
                    groups = projects = delivery = types = None

                crud_fields = [groups,
                               "name",
                               "acronym",
                               types,
                               projects,
                               delivery,
                               S3SQLInlineComponent(
                                    "contact",
                                    fields = [("", "value")],
                                    filterby = {"field": "contact_method",
                                                "options": "EMAIL",
                                                },
                                    label = T("Email"),
                                    multiple = False,
                                    name = "email",
                                    ),
                               "phone",
                               "website",
                               "logo",
                               "comments",
                               ]

                # Filters
                text_fields = ["name", "acronym", "website", "phone"]
                if is_org_group_admin:
                    text_fields.extend(["email.value", "orgid.value"])
                filter_widgets = [TextFilter(text_fields,
                                             label = T("Search"),
                                             ),
                                  ]
                if is_org_group_admin:
                    filter_widgets.extend([
                        OptionsFilter(
                            "group__link.group_id",
                            label = T("Group"),
                            options = lambda: get_filter_options("org_group"),
                            ),
                        OptionsFilter(
                            "organisation_type__link.organisation_type_id",
                            label = T("Type"),
                            options = lambda: get_filter_options("org_organisation_type"),
                            ),
                        OptionsFilter(
                            "mgrinfo.value",
                            label = T("TestSt Manager##abbr"),
                            options = OrderedDict(mgrinfo_opts()),
                            sort = False,
                            hidden = True,
                            ),
                        ])

                resource.configure(crud_form = S3SQLCustomForm(*crud_fields),
                                    filter_widgets = filter_widgets,
                                    )

            # Custom list fields
            list_fields = [#"group__link.group_id",
                           "name",
                           "acronym",
                           #"organisation_type__link.organisation_type_id",
                           "website",
                           "phone",
                           #"email.value"
                           ]
            if is_org_group_admin:
                list_fields.insert(2, (T("Type"), "organisation_type__link.organisation_type_id"))
                list_fields.insert(0, (T("Organization Group"), "group__link.group_id"))
                list_fields.append((T("Email"), "email.value"))
            r.resource.configure(list_fields = list_fields,
                                    )

        elif component_name == "facility":
            if r.component_id and \
                (is_org_group_admin or \
                record and auth.s3_has_role("ORG_ADMIN", for_pe=record.pe_id)):
                # Expose obsolete-flag
                ctable = r.component.table
                field = ctable.obsolete
                field.readable = field.writable = True

        elif component_name in ("human_resource", "managers"):

            phone_label = settings.get_ui_label_mobile_phone()
            site_id = None if component_name == "managers" else "site_id"
            list_fields = ["organisation_id",
                           "person_id",
                           "job_title_id",
                           site_id,
                           (T("Email"), "person_id$email.value"),
                           (phone_label, "person_id$phone.value"),
                           "status",
                           ]

            r.component.configure(list_fields = list_fields,
                                  )

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import rlpptm_org_rheader
    attr["rheader"] = rlpptm_org_rheader

    return attr

# -------------------------------------------------------------------------
def org_organisation_type_resource(r, tablename):

    T = current.T
    db = current.db
    s3db = current.s3db

    s3db.add_components("org_organisation_type",
                        org_organisation_type_tag = ({"name": "group",
                                                      "joinby": "organisation_type_id",
                                                      "filterby": {"tag": "OrgGroup"},
                                                      "multiple": False,
                                                      },
                                                     {"name": "commercial",
                                                      "joinby": "organisation_type_id",
                                                      "filterby": {"tag": "Commercial"},
                                                      "multiple": False,
                                                      },
                                                     ),
                        )

    if r.tablename == "org_organisation_type":

        T = current.T

        resource = r.resource
        component = resource.components.get("group")
        if component:

            # Look up organisation group names
            gtable = s3db.org_group
            groups = db(gtable.deleted == False).select(gtable.name,
                                                        cache = s3db.cache,
                                                        )
            options = [group.name for group in groups]

            # Configure them as options for the OrgGroup tag
            ctable = component.table
            field = ctable.value
            field.label = T("Organization Group")
            field.requires = IS_EMPTY_OR(IS_IN_SET(options))

        # Configure binary tag representation
        from ..helpers import configure_binary_tags
        configure_binary_tags(r.resource, ("commercial",))

        # Expose orderable item categories
        ltable = s3db.req_requester_category
        field = ltable.item_category_id
        field.represent = S3Represent(lookup="supply_item_category")

        # Custom form
        from core import S3SQLCustomForm, S3SQLInlineLink
        crud_form = S3SQLCustomForm("name",
                                    "group.value",
                                    (T("Commercial Providers"), "commercial.value"),
                                    S3SQLInlineLink("item_category",
                                                    field = "item_category_id",
                                                    label = T("Orderable Item Categories"),
                                                    ),
                                    "comments",
                                    )

        # Include tags and orderable item categories in list view
        list_fields = ["id",
                       "name",
                       "group.value",
                       (T("Commercial Providers"), "commercial.value"),
                       (T("Orderable Item Categories"), "requester_category.item_category_id"),
                       "comments",
                       ]

        resource.configure(crud_form = crud_form,
                           list_fields = list_fields,
                           )


# -------------------------------------------------------------------------
def add_site_tags():
    """
        Approval workflow tags as filtered components
            - for embedding in form
    """

    s3db = current.s3db
    s3db.add_components("org_site",
                        org_site_tag = (# Approval workflow status
                                        {"name": "status",
                                         "joinby": "site_id",
                                         "filterby": {"tag": "STATUS"},
                                         "multiple": False,
                                         },
                                        # MPAV qualification
                                        {"name": "mpav",
                                         "joinby": "site_id",
                                         "filterby": {"tag": "MPAV"},
                                         "multiple": False,
                                         },
                                        # Hygiene concept
                                        {"name": "hygiene",
                                         "joinby": "site_id",
                                         "filterby": {"tag": "HYGIENE"},
                                         "multiple": False,
                                         },
                                        # Facility layout
                                        {"name": "layout",
                                         "joinby": "site_id",
                                         "filterby": {"tag": "LAYOUT"},
                                         "multiple": False,
                                         },
                                        # In public registry
                                        {"name": "public",
                                         "joinby": "site_id",
                                         "filterby": {"tag": "PUBLIC"},
                                         "multiple": False,
                                         },
                                        ),
                        )

# -----------------------------------------------------------------------------
def configure_site_tags(resource, role="applicant", record_id=None):
    """
        Configures facility approval workflow tags

        Args:
            resource: the org_facility resource
            role: the user's role in the workflow (applicant|approver)
            record_id: the facility record ID

        Returns:
            the list of visible workflow tags [(label, selector)]
    """

    T = current.T
    components = resource.components

    visible_tags = []

    # Configure STATUS tag
    status_tag_opts = {"REVISE": T("Completion/Adjustment Required"),
                       "READY": T("Ready for Review"),
                       "REVIEW": T("Review Pending"),
                       "APPROVED": T("Approved##actionable"),
                       }
    selectable = None
    status_visible = False
    review_tags_visible = False

    if role == "applicant" and record_id:
        # Check current status
        db = current.db
        s3db = current.s3db
        ftable = s3db.org_facility
        ttable = s3db.org_site_tag
        join = ftable.on((ftable.site_id == ttable.site_id) & \
                         (ftable.id == record_id))
        query = (ttable.tag == "STATUS") & (ttable.deleted == False)
        row = db(query).select(ttable.value, join=join, limitby=(0, 1)).first()
        if row:
            if row.value == "REVISE":
                review_tags_visible = True
                selectable = (row.value, "READY")
            elif row.value == "REVIEW":
                review_tags_visible = True
        status_visible = True

    component = components.get("status")
    if component:
        ctable = component.table
        field = ctable.value
        field.default = "REVISE"
        field.readable = status_visible
        if status_visible:
            if selectable:
                selectable_statuses = [(status, status_tag_opts[status])
                                       for status in selectable]
                field.requires = IS_IN_SET(selectable_statuses, zero=None)
                field.writable = True
            else:
                field.writable = False
            visible_tags.append((T("Processing Status"), "status.value"))
        field.represent = workflow_tag_represent(status_tag_opts)

    # Configure review tags
    review_tag_opts = (("REVISE", T("Completion/Adjustment Required")),
                       ("REVIEW", T("Review Pending")),
                       ("APPROVED", T("Approved##actionable")),
                       )
    selectable = review_tag_opts if role == "approver" else None

    review_tags = (("mpav", T("MPAV Qualification")),
                   ("hygiene", T("Hygiene Plan")),
                   ("layout", T("Facility Layout Plan")),
                   )
    for cname, label in review_tags:
        component = components.get(cname)
        if component:
            ctable = component.table
            field = ctable.value
            field.default = "REVISE"
            if selectable:
                field.requires = IS_IN_SET(selectable, zero=None, sort=False)
                field.readable = field.writable = True
            else:
                field.readable = review_tags_visible
                field.writable = False
            if field.readable:
                visible_tags.append((label, "%s.value" % cname))
            field.represent = workflow_tag_represent(dict(review_tag_opts))

    # Configure PUBLIC tag
    binary_tag_opts = {"Y": T("Yes"),
                       "N": T("No"),
                       }
    selectable = binary_tag_opts if role == "approver" else None

    component = resource.components.get("public")
    if component:
        ctable = component.table
        field = ctable.value
        field.default = "N"
        if selectable:
            field.requires = IS_IN_SET(selectable, zero=None)
            field.writable = True
        else:
            field.requires = IS_IN_SET(binary_tag_opts, zero=None)
            field.writable = False
        field.represent = workflow_tag_represent(binary_tag_opts)
    visible_tags.append((T("In Public Registry"), "public.value"))
    visible_tags.append("site_details.authorisation_advice")

    return visible_tags

# -----------------------------------------------------------------------------
def add_facility_default_tags(facility_id, approve=False):
    """
        Add default tags to a new facility

        Args:
            facility_id: the facility record ID
            approve: whether to assume approval of the facility
    """

    db = current.db
    s3db = current.s3db

    ftable = s3db.org_facility
    ttable = s3db.org_site_tag
    left = ttable.on((ttable.site_id == ftable.site_id) & \
                     (ttable.tag.belongs(SITE_WORKFLOW)) & \
                     (ttable.deleted == False))
    query = (ftable.id == facility_id)
    rows = db(query).select(ftable.site_id,
                            ttable.id,
                            ttable.tag,
                            ttable.value,
                            left = left,
                            )
    if not rows:
        return
    else:
        site_id = rows.first().org_facility.site_id

    existing = {row.org_site_tag.tag: row.org_site_tag.value
                    for row in rows if row.org_site_tag.id}
    public = existing.get("PUBLIC") == "Y" or approve

    for tag in SITE_WORKFLOW:
        if tag in existing:
            continue
        elif tag == "PUBLIC":
            default = "Y" if public else "N"
        elif tag == "STATUS":
            if any(existing.get(t) == "REVISE" for t in SITE_REVIEW):
                default = "REVISE"
            elif any(existing.get(t) == "REVIEW" for t in SITE_REVIEW):
                default = "REVIEW"
            else:
                default = "APPROVED" if public else "REVIEW"
        else:
            default = "APPROVED" if public else "REVISE"
        ttable.insert(site_id = site_id,
                      tag = tag,
                      value = default,
                      )
        existing[tag] = default

# -----------------------------------------------------------------------------
def set_facility_code(facility_id):
    """
        Generate and set a unique facility code

        Args:
            facility_id: the facility ID

        Returns:
            the facility code
    """

    db = current.db
    s3db = current.s3db

    table = s3db.org_facility
    query = (table.id == facility_id)

    facility = db(query).select(table.id,
                                table.uuid,
                                table.code,
                                limitby = (0, 1),
                                ).first()

    if not facility or facility.code:
        return None

    try:
        uid = int(facility.uuid[9:14], 16) % 1000000
    except (TypeError, ValueError):
        import uuid
        uid = int(uuid.uuid4().urn[9:14], 16) % 1000000

    # Generate code
    import random
    suffix = "".join(random.choice("ABCFGHKLNPRSTWX12456789") for _ in range(3))
    code = "%06d-%s" % (uid, suffix)

    facility.update_record(code=code)

    return code

# -----------------------------------------------------------------------------
def facility_approval_status(tags, mgrinfo):
    """
        Determines which site approval tags to update after status change
        by OrgGroupAdmin

        Args:
            tags: the current approval tags
            mgrinfo: the current MGRINFO status of the organisation

        Returns:
            tuple (update, notify)
                update: dict {tag: value} for update
                notify: boolean, whether to notify the OrgAdmin
    """

    update, notify = {}, False

    status = tags.get("STATUS")
    if status == "REVISE":
        if all(tags[k] == "APPROVED" for k in SITE_REVIEW) and mgrinfo == "COMPLETE":
            update["PUBLIC"] = "Y"
            update["STATUS"] = "APPROVED"
            notify = True
        elif any(tags[k] == "REVIEW" for k in SITE_REVIEW):
            update["PUBLIC"] = "N"
            update["STATUS"] = "REVIEW"
        else:
            update["PUBLIC"] = "N"
            # Keep status REVISE

    elif status == "READY":
        update["PUBLIC"] = "N"
        if all(tags[k] == "APPROVED" for k in SITE_REVIEW) and mgrinfo == "COMPLETE":
            for k in SITE_REVIEW:
                update[k] = "REVIEW"
        else:
            for k in SITE_REVIEW:
                if tags[k] == "REVISE":
                    update[k] = "REVIEW"
        update["STATUS"] = "REVIEW"

    elif status == "REVIEW":
        if all(tags[k] == "APPROVED" for k in SITE_REVIEW) and mgrinfo == "COMPLETE":
            update["PUBLIC"] = "Y"
            update["STATUS"] = "APPROVED"
            notify = True
        elif any(tags[k] == "REVIEW" for k in SITE_REVIEW) or mgrinfo == "REVISE":
            update["PUBLIC"] = "N"
            # Keep status REVIEW
        elif any(tags[k] == "REVISE" for k in SITE_REVIEW) or mgrinfo == "N/A":
            update["PUBLIC"] = "N"
            update["STATUS"] = "REVISE"
            notify = True

    elif status == "APPROVED":
        if any(tags[k] == "REVIEW" for k in SITE_REVIEW) or mgrinfo == "REVISE":
            update["PUBLIC"] = "N"
            update["STATUS"] = "REVIEW"
        elif any(tags[k] == "REVISE" for k in SITE_REVIEW) or mgrinfo == "N/A":
            update["PUBLIC"] = "N"
            update["STATUS"] = "REVISE"
            notify = True

    return update, notify

# -----------------------------------------------------------------------------
def facility_approval_workflow(site_id):
    """
        Update facility approval workflow tags after status change by
        OrgGroupAdmin, and notify the OrgAdmin of the site when needed

        Args:
            site_id: the site ID
    """

    db = current.db
    s3db = current.s3db

    # Get facility and MGRINFO status
    ftable = s3db.org_facility
    ottable = s3db.org_organisation_tag
    left = ottable.on((ottable.organisation_id == ftable.organisation_id) & \
                      (ottable.tag == "MGRINFO") & \
                      (ottable.deleted == False))
    query = (ftable.site_id == site_id)
    facility = db(query).select(ftable.id,
                                ottable.value,
                                left = left,
                                limitby = (0, 1),
                                ).first()
    if not facility:
        return

    if current.deployment_settings.get_custom("test_station_manager_required"):
        mgrinfo = facility.org_organisation_tag.value
    else:
        # Treat like complete
        mgrinfo = "COMPLETE"

    # Get all tags for site
    ttable = s3db.org_site_tag
    query = (ttable.site_id == site_id) & \
            (ttable.tag.belongs(SITE_WORKFLOW)) & \
            (ttable.deleted == False)
    rows = db(query).select(ttable.id,
                            ttable.tag,
                            ttable.value,
                            )
    tags = {row.tag: row.value for row in rows}
    if any(k not in tags for k in SITE_WORKFLOW):
        add_facility_default_tags(facility.org_facility.id)
        facility_approval_workflow(site_id)
        return

    # Update tags
    update, notify = facility_approval_status(tags, mgrinfo)
    for row in rows:
        key = row.tag
        if key in update:
            row.update_record(value=update[key])

    T = current.T

    # Screen message on status change
    public = update.get("PUBLIC")
    if public and public != tags["PUBLIC"]:
        if public == "Y":
            msg = T("Facility added to public registry")
        else:
            msg = T("Facility removed from public registry pending review")
        current.response.information = msg

    # Send Notifications
    if notify:
        tags.update(update)
        if mgrinfo != "COMPLETE":
            tags["MGRINFO"] = "REVISE"
        msg = facility_review_notification(site_id, tags)
        if msg:
            current.response.warning = \
                T("Test station could not be notified: %(error)s") % {"error": msg}
        else:
            current.response.flash = \
                T("Test station notified")

# -----------------------------------------------------------------------------
def facility_approval_update_mgrinfo(organisation_id, mgrinfo):
    """
        Update the workflow status of test stations depending on the
        status of the test station manager info of the organisation

        Args:
            organisation_id: the organisation ID
            status: the MGRINFO status
    """

    db = current.db
    s3db = current.s3db

    # Propagate status to related facilities
    ftable = s3db.org_facility
    query = (ftable.organisation_id == organisation_id)
    facilities = db(query).select(ftable.id,
                                  ftable.site_id,
                                  ftable.obsolete,
                                  )

    for facility in facilities:
        # Get all workflow-tags for the facility
        ttable = s3db.org_site_tag
        query = (ttable.site_id == facility.site_id) & \
                (ttable.tag.belongs(SITE_WORKFLOW)) & \
                (ttable.deleted == False)
        rows = db(query).select(ttable.id,
                                ttable.tag,
                                ttable.value,
                                )
        tags = {row.tag: row.value for row in rows}

        # Update workflow status for the facility
        update, notify = {}, False
        if mgrinfo == "N/A":
            update["STATUS"] = "REVISE"
            update["PUBLIC"] = "N"
            # Notify if public-status changes for active facility
            notify = tags.get("PUBLIC") != "N" and not facility.obsolete
        elif mgrinfo == "REVISE":
            if not any(tags[t] == "REVISE" for t in SITE_REVIEW):
                update["STATUS"] = "REVIEW"
            update["PUBLIC"] = "N"
        elif tags.get("STATUS") == "REVISE":
            if all(tags[t] != "REVISE" for t in SITE_REVIEW):
                update["STATUS"] = "REVIEW"
            update["PUBLIC"] = "N"
        tags.update(update)

        # Update workflow tags (resp. insert missing tags)
        for row in rows:
            if row.tag in update:
                row.update_record(value = update[row.tag])
                del update[row.tag]
        if update:
            for key, value in update.items():
                ttable.insert(site_id = facility.site_id,
                              tag = key,
                              value = value,
                              )

        # Notify the OrgAdmin about approval status change
        if notify:
            if mgrinfo != "COMPLETE":
                tags["MGRINFO"] = "REVISE"
            facility_review_notification(facility.site_id, tags)

# -----------------------------------------------------------------------------
def facility_review_notification(site_id, tags):
    """
        Notify the OrgAdmin of a test station about the status of the review

        Args:
            site_id: the test facility site ID
            tags: the current workflow tags

        Returns:
            error message on error, else None
    """

    db = current.db
    s3db = current.s3db

    # Lookup the facility
    ftable = s3db.org_facility
    query = (ftable.site_id == site_id) & \
            (ftable.deleted == False)
    facility = db(query).select(ftable.id,
                                ftable.name,
                                ftable.organisation_id,
                                limitby = (0, 1),
                                ).first()
    if not facility:
        return "Facility not found"

    organisation_id = facility.organisation_id
    if not organisation_id:
        return "Organisation not found"

    # Find the OrgAdmin email addresses
    from ..helpers import get_role_emails
    email = get_role_emails("ORG_ADMIN",
                            organisation_id = organisation_id,
                            )
    if not email:
        return "No Organisation Administrator found"

    # Data for the notification email
    data = {"name": facility.name,
            "url": URL(c = "org",
                       f = "organisation",
                       args = [organisation_id, "facility", facility.id],
                       host = True,
                       ),
            }

    status = tags.get("STATUS")

    if status == "REVISE":
        template = "FacilityReview"

        # Add advice
        dtable = s3db.org_site_details
        query = (dtable.site_id == site_id) & \
                (dtable.deleted == False)
        details = db(query).select(dtable.authorisation_advice,
                                   limitby = (0, 1),
                                   ).first()
        if details and details.authorisation_advice:
            data["advice"] = details.authorisation_advice
        else:
            data["advice"] = "-"

        # Add explanations for relevant requirements
        review = (("MPAV", "FacilityMPAVRequirements"),
                  ("HYGIENE", "FacilityHygienePlanRequirements"),
                  ("LAYOUT", "FacilityLayoutRequirements"),
                  ("MGRINFO", "TestStationManagerRequirements"),
                  )
        ctable = s3db.cms_post
        ltable = s3db.cms_post_module
        join = ltable.on((ltable.post_id == ctable.id) & \
                         (ltable.module == "org") & \
                         (ltable.resource == "facility") & \
                         (ltable.deleted == False))
        explanations = []
        for tag, requirements in review:
            if tags.get(tag) == "REVISE":
                query = (ctable.name == requirements) & \
                        (ctable.deleted == False)
                row = db(query).select(ctable.body,
                                       join = join,
                                       limitby = (0, 1),
                                       ).first()
                if row:
                    explanations.append(row.body)
        data["explanations"] = "\n\n".join(explanations) if explanations else "-"

    elif status == "APPROVED":
        template = "FacilityApproved"

    else:
        # No notifications for this status
        return "invalid status"

    # Lookup email address of current user
    from ..notifications import CMSNotifications
    auth = current.auth
    if auth.user:
        cc = CMSNotifications.lookup_contact(auth.user.pe_id)
    else:
        cc = None

    # Send CMS Notification FacilityReview
    return CMSNotifications.send(email,
                                 template,
                                 data,
                                 module = "org",
                                 resource = "facility",
                                 cc = cc,
                                 )

# -------------------------------------------------------------------------
def check_blocked_l2(form, variable):
    """
        Checks if the L2 in a facility form is currently blocked
        for new registrations, and sets a form error if so

        Args:
            form: the FORM
            variable: the corresponding L2 variable of the location selector
    """

    if current.auth.s3_has_role("ADMIN"):
        # Admins bypass the check
        return

    try:
        district = form.vars[variable]
    except (AttributeError, KeyError):
        return

    blacklist = current.deployment_settings.get_custom("registration_blocked")

    if district and blacklist:

        ltable = current.s3db.gis_location
        query = (ltable.id == district)
        row = current.db(query).select(ltable.name, limitby=(0, 1)).first()
        if row and row.name in blacklist:
            form.errors[variable] = current.T("Due to excess capacities, no new test facilities can be registered in this district currently")

# -------------------------------------------------------------------------
def facility_create_onvalidation(form):
    """
        Onvalidation of new facility:
            - check if L2 is currently blocked for new registrations

        Args:
            form: the FORM
    """

    check_blocked_l2(form, "location_id_L2")

# -------------------------------------------------------------------------
def facility_create_onaccept(form):
    """
        Onaccept of new facility:
            - generate facility ID (code)
            - set default values for workflow tags

        Args:
            form: the FORM
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    # Generate facility ID and add default tags
    set_facility_code(record_id)
    add_facility_default_tags(record_id)

# -------------------------------------------------------------------------
def facility_postprocess(form):
    """
        Postprocess the facility form
            - Update workflow tags

        Args:
            form: the FORM
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    # Lookup site_id
    table = current.s3db.org_facility
    row = current.db(table.id == record_id).select(table.site_id,
                                                   limitby = (0, 1),
                                                   ).first()
    if row and row.site_id:
        # Update approval workflow
        facility_approval_workflow(row.site_id)

# -------------------------------------------------------------------------
def facility_mgrinfo(row):
    """
        Field method to determine the MGRINFO status of the organisation

        Args:
            row: the facility Row

        Returns:
            the value of the MGRINFO tag of the organisation
    """

    if hasattr(row, "org_mgrinfo_organisation_tag"):
        # Provided as extra-field
        tag = row.org_mgrinfo_organisation_tag.value

    else:
        # Must look up
        db = current.db
        s3db = current.s3db
        ttable = s3db.org_organisation_tag
        query = (ttable.organisation_id == row.org_facility.organisation_id) & \
                (ttable.tag == "MGRINFO") & \
                (ttable.deleted == False)
        row = db(query).select(ttable.value,
                               limitby = (0, 1),
                               ).first()
        tag = row.value if row else None

    return tag

# -------------------------------------------------------------------------
def configure_facility_form(r, is_org_group_admin=False):
    """
        Configures the facility management form

        Args:
            r: the current CRUD request
            is_org_group_admin: whether the user is ORG_GROUP_ADMIN
    """

    T = current.T
    s3db = current.s3db

    organisation = obsolete = services = documents = None

    resource = r.resource
    if r.tablename == "org_facility":
        # Primary controller
        fresource = resource
        record_id = r.id
    elif r.tablename == "org_organisation" and \
            r.component_name == "facility":
        # Facility tab of organisation
        fresource = resource.components.get("facility")
        record_id = r.component_id
        obsolete = "obsolete"
    else:
        # Other view
        fresource = record_id = None


    from core import S3SQLCustomForm, \
                     S3SQLInlineComponent, \
                     S3SQLInlineLink, \
                     WithAdvice

    visible_tags = postprocess = None
    if fresource:
        # Inline service selector and documents
        services = S3SQLInlineLink(
                        "service",
                        label = T("Services"),
                        field = "service_id",
                        widget = "groupedopts",
                        cols = 1,
                        )
        documents = S3SQLInlineComponent(
                        "document",
                        name = "file",
                        label = T("Documents"),
                        fields = ["name", "file", "comments"],
                        filterby = {"field": "file",
                                    "options": "",
                                    "invert": True,
                                    },
                        )

        if is_org_group_admin:
            # Show organisation
            organisation = "organisation_id"

            # Add workflow tags
            if record_id:
                visible_tags = configure_site_tags(fresource,
                                                   role = "approver",
                                                   record_id = record_id,
                                                   )
        else:
            # Add Intros for services and documents
            services = WithAdvice(services,
                                  text = ("org", "facility", "SiteServiceIntro"),
                                  )
            documents = WithAdvice(documents,
                                   text = ("org", "facility", "SiteDocumentsIntro"),
                                   )
            # Add workflow tags
            if record_id:
                visible_tags = configure_site_tags(fresource,
                                                   role = "applicant",
                                                   record_id = record_id,
                                                   )

    crud_fields = [organisation,
                   # -- Facility
                   "name",
                   "code",
                   S3SQLInlineLink(
                       "facility_type",
                       label = T("Facility Type"),
                       field = "facility_type_id",
                       widget = "groupedopts",
                       cols = 3,
                       ),
                   # -- Address
                   "location_id",
                   # -- Service Offer
                   (T("Opening Hours"), "opening_times"),
                   "site_details.service_mode_id",
                   services,
                   # -- Appointments and Contact
                   (T("Telephone"), "phone1"),
                   "email",
                   "website",
                   (T("Appointments via"), "site_details.booking_mode_id"),
                   "comments",
                   # -- Administrative
                   documents,
                   obsolete,
                   ]

    subheadings = {"name": T("Facility"),
                   "location_id": T("Address"),
                   "opening_times": T("Service Offer"),
                   "phone1": T("Contact and Appointments"),
                   "filedocument": T("Administrative"),
                   }

    if visible_tags:

        table = fresource.table
        table.mgrinfo = s3_fieldmethod("mgrinfo", facility_mgrinfo,
                                       represent = workflow_tag_represent(dict(mgrinfo_opts())),
                                       )
        extra_fields = ["organisation_id$mgrinfo.value"]

        if is_org_group_admin:
            # Include MGRINFO status
            from core import S3SQLVirtualField
            crud_fields.append(S3SQLVirtualField("mgrinfo",
                                                 label = T("Documentation Test Station Manager"),
                                                 ))
            fname = "mgrinfo"
        else:
            fname = visible_tags[0][1].replace(".", "_")

        # Append workflow tags in separate section
        subheadings[fname] = T("Approval and Publication")
        crud_fields.extend(visible_tags)

        # Add postprocess to update workflow statuses
        postprocess = facility_postprocess

    else:
        extra_fields = None

    s3db.configure("org_facility",
                   crud_form = S3SQLCustomForm(*crud_fields,
                                               postprocess = postprocess,
                                               ),
                   extra_fields = extra_fields,
                   subheadings = subheadings,
                   )

# -------------------------------------------------------------------------
def org_facility_resource(r, tablename):

    T = current.T
    auth = current.auth
    s3db = current.s3db
    settings = current.deployment_settings

    is_org_group_admin = current.auth.s3_has_role("ORG_GROUP_ADMIN")

    # Add tags for both orgs and sites
    add_org_tags()
    add_site_tags()

    # Custom onvalidation to check L2 against blocked-list
    s3db.add_custom_callback("org_facility",
                             "onvalidation",
                             facility_create_onvalidation,
                             method = "create",
                             )

    # Custom onaccept to add default tags
    s3db.add_custom_callback("org_facility",
                             "onaccept",
                             facility_create_onaccept,
                             method = "create",
                             )

    if not is_org_group_admin and \
        not settings.get_custom(key="test_station_registration"):
        # If test station registration is disabled, no new test
        # facilities can be added either
        s3db.configure(tablename, insertable = False)

    # Configure fields
    in_org_controller = r.tablename == "org_organisation"
    from core import (S3SQLCustomForm,
                      S3SQLInlineLink,
                      LocationFilter,
                      S3LocationSelector,
                      OptionsFilter,
                      TextFilter,
                      s3_text_represent,
                      )

    table = s3db.org_facility

    # Custom representation of organisation_id including type
    field = table.organisation_id
    from ..helpers import OrganisationRepresent
    field.represent = OrganisationRepresent()
    field.comment = None

    # Expose code (r/o)
    field = table.code
    field.label = T("Test Station ID")
    field.readable = True
    field.writable = False

    # Configure location selector incl. Geocoder
    field = table.location_id
    # Address/Postcode are required
    # - except for OrgGroupAdmin, who need to be able to
    #   update the record even when this detail is missing
    address_required = not is_org_group_admin
    field.widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                      required_levels = ("L1", "L2", "L3"),
                                      show_address = True,
                                      show_postcode = True,
                                      address_required = address_required,
                                      postcode_required = address_required,
                                      show_map = True,
                                      )
    current.response.s3.scripts.append("/%s/static/themes/RLP/js/geocoderPlugin.js" % r.application)

    # Custom tooltip for comments field
    field = table.comments
    if in_org_controller:
        field.comment = DIV(_class="tooltip",
                            _title="%s|%s" % (T("Comments"),
                                              T("Additional information and advice regarding facility and services"),
                                              ),
                            )
    else:
        field.writable = False
        field.comment = None

    # Custom label for obsolete-Flag
    field = table.obsolete
    field.label = T("Defunct")
    if r.interactive or r.representation == "aadata":
        field.represent = lambda v, row=None: ICON("remove") if v else ""
    else:
        from core import s3_yes_no_represent
        field.represent = s3_yes_no_represent
    field.comment = DIV(_class="tooltip",
                        _title="%s|%s" % (T("Defunct"),
                                          T("Please mark this field when the facility is no longer in operation"),
                                          ),
                        )

    # Opening times are mandatory
    # - except for OrgGroupAdmin, who need to be able to
    #   update the record even when this detail is missing
    if not is_org_group_admin:
        field = table.opening_times
        field.requires = IS_NOT_EMPTY()

    # Custom representation of service links
    stable = s3db.org_service_site
    field = stable.service_id
    from ..helpers import ServiceListRepresent
    field.represent = ServiceListRepresent(lookup = "org_service",
                                           show_link = False,
                                           )

    # Expose site details
    dtable = s3db.org_site_details
    field = dtable.booking_mode_id
    field.readable = True
    field.writable = in_org_controller

    field = dtable.service_mode_id
    field.readable = True
    field.writable = in_org_controller
    requires = field.requires
    if isinstance(requires, IS_EMPTY_OR):
        field.requires = requires.other

    field = dtable.authorisation_advice
    field.label = T("Advice")
    css = "approve-workflow"
    field.represent = lambda v, row=None: \
                        s3_text_represent(v,
                            truncate = False,
                            _class = ("%s workflow-advice" % css) if v else css,
                            )
    field.readable = True
    if is_org_group_admin:
        field.comment = DIV(_class="tooltip",
                            _title="%s|%s" % (T("Advice"),
                                              T("Instructions/advice for the test station how to proceed with regard to authorization"),
                                              ),
                            )
        field.writable = True
    else:
        field.writable = False

    # Special views
    get_vars = r.get_vars
    if is_org_group_admin and not in_org_controller:
        show_all = get_vars.get("$$all") == "1"
        show_pnd = get_vars.get("$$pending") == "1"
        show_rvw = get_vars.get("$$review") == "1"
        #show_obs = get_vars.get("$$obsolete") == "1"
    else:
        show_all = show_pnd = show_rvw = False #show_obs = False

    # Custom list fields
    list_fields = ["name",
                   #"organisation_id",
                   "organisation_id$organisation_type__link.organisation_type_id",
                   (T("Telephone"), "phone1"),
                   "email",
                   (T("Opening Hours"), "opening_times"),
                   "site_details.service_mode_id",
                   "service_site.service_id",
                   "location_id$addr_street",
                   "location_id$addr_postcode",
                   "location_id$L4",
                   "location_id$L3",
                   "location_id$L2",
                   ]

    if show_pnd or show_rvw:
        list_fields.insert(1, "organisation_id")
    if show_all or in_org_controller:
        list_fields.append("obsolete")

    s3db.configure(tablename, list_fields=list_fields)

    # Custom filter widgets
    text_fields = ["name",
                   "location_id$L2",
                   "location_id$L3",
                   "location_id$L4",
                   "location_id$addr_postcode",
                   ]
    if is_org_group_admin:
        text_fields.append("code")
    filter_widgets = [
        TextFilter(text_fields,
                   label = T("Search"),
                   ),
        LocationFilter("location_id",
                       levels = ("L1", "L2", "L3", "L4"),
                       translate = False,
                       ),
        OptionsFilter("service_site.service_id",
                      label = T("Services"),
                      options = lambda: get_filter_options("org_service"),
                      cols = 1,
                      hidden = True,
                      ),
        ]

    if is_org_group_admin:
        from ..requests import delivery_tag_opts
        delivery_opts = delivery_tag_opts()
        filter_widgets.extend([
            OptionsFilter("organisation_id$delivery.value",
                          label = T("Delivery##supplying"),
                          options = delivery_opts,
                          hidden = True,
                          ),
            OptionsFilter("organisation_id$organisation_type__link.organisation_type_id",
                          hidden = True,
                          options = lambda: get_filter_options("org_organisation_type",
                                                               translate = True,
                                                               ),
                          ),
            ])

        if show_all or r.method == "report":
            binary_tag_opts = OrderedDict([("Y", T("Yes")), ("N", T("No"))])
            filter_widgets.extend([
                OptionsFilter("organisation_id$project_organisation.project_id",
                              options = lambda: get_filter_options("project_project"),
                              hidden = True,
                              ),
                OptionsFilter("public.value",
                              label = T("Approved##actionable"),
                              options = binary_tag_opts,
                              cols = 2,
                              hidden = True,
                              ),
                ])
        if show_all:
            filter_widgets.append(OptionsFilter("obsolete",
                                                label = T("Status"),
                                                options = {True: T("Defunct"),
                                                           False: T("Active"),
                                                           },
                                                cols = 2,
                                                hidden = True,
                                                ))

    s3db.configure(tablename, filter_widgets=filter_widgets)

    # Custom CRUD form
    record = r.record
    public_view = r.tablename == "org_facility" and \
                    (not record or
                     not auth.s3_has_permission("update", r.table, record_id=record.id))
    if public_view:
        crud_form = S3SQLCustomForm(
                "name",
                S3SQLInlineLink(
                    "facility_type",
                    label = T("Facility Type"),
                    field = "facility_type_id",
                    widget = "groupedopts",
                    cols = 3,
                    ),
                "location_id",
                (T("Opening Hours"), "opening_times"),
                "site_details.service_mode_id",
                S3SQLInlineLink(
                    "service",
                    label = T("Services"),
                    field = "service_id",
                    widget = "groupedopts",
                    cols = 1,
                    ),
                (T("Telephone"), "phone1"),
                "email",
                "website",
                (T("Appointments via"), "site_details.booking_mode_id"),
                "comments",
                )
        s3db.configure(tablename, crud_form=crud_form)
    else:
        configure_facility_form(r, is_org_group_admin=is_org_group_admin)

    # Report options
    if r.method == "report":
        axes = ["organisation_id",
                "location_id$L3",
                "location_id$L2",
                "location_id$L1",
                "service_site.service_id",
                (T("Project"), "organisation_id$project.name"),
                (T("Organization Group"), "organisation_id$group_membership.group_id"),
                "organisation_id$organisation_type__link.organisation_type_id",
                (T("Requested Items"), "req.req_item.item_id"),
                ]

        report_options = {
            "rows": axes,
            "cols": axes,
            "fact": [(T("Number of Facilities"), "count(id)"),
                     (T("List of Facilities"), "list(name)"),
                     ],
            "defaults": {"rows": "location_id$L2",
                         "cols": None,
                         "fact": "count(id)",
                         "totals": True,
                         },
            }

        s3db.configure(tablename,
                       report_options = report_options,
                       )

    # Custom method to produce KV report
    from ..helpers import TestFacilityInfo
    s3db.set_method("org_facility",
                    method = "info",
                    action = TestFacilityInfo,
                    )


# -------------------------------------------------------------------------
def org_facility_controller(**attr):

    T = current.T
    s3 = current.response.s3
    settings = current.deployment_settings

    auth = current.auth
    is_org_group_admin = auth.s3_has_role("ORG_GROUP_ADMIN")

    # Load model for default CRUD strings
    current.s3db.table("org_facility")

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        # Restrict data formats
        from ..helpers import restrict_data_formats
        restrict_data_formats(r)

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        s3db = current.s3db

        resource = r.resource
        table = resource.table
        record = r.record

        if not record:
            # Filter out defunct facilities
            #resource.add_filter(FS("obsolete") == False)

            # Open read-view first, even if permitted to edit
            settings.ui.open_read_first = True

            if is_org_group_admin and r.method == "report":
                s3.crud_strings["org_facility"].title_report = T("Facilities Statistics")
            else:
                get_vars = r.get_vars
                if is_org_group_admin:
                    show_all = get_vars.get("$$all") == "1"
                    show_pnd = get_vars.get("$$pending") == "1"
                    show_rvw = get_vars.get("$$review") == "1"
                    show_obs = get_vars.get("$$obsolete") == "1"
                else:
                    show_all = show_pnd = show_rvw = show_obs = False

                public_list = False
                title_list = T("Find Test Station")
                query = None
                if show_all:
                    title_list = T("All Test Stations")
                elif show_pnd:
                    title_list = T("Unapproved Test Stations")
                    query = (FS("public.value") == "N")
                elif show_rvw:
                    title_list = T("Test Stations to review")
                    query = (FS("status.value") == "REVIEW")
                elif show_obs:
                    title_list = T("Defunct Test Stations")
                    query = (FS("obsolete") == True)
                else:
                    public_list = True
                    query = (FS("public.value") == "Y")

                if query:
                    resource.add_filter(query)
                if not (show_all or show_obs):
                    resource.add_filter(FS("obsolete") == False)

                if public_list:
                    if not is_org_group_admin:
                        # No Side Menu
                        current.menu.options = None

                    # Filter list by project code
                    # - re-use last used $$code filter of this session
                    # - default to original subset for consistency in bookmarks/links
                    session_s3 = current.session.s3
                    default_filter = session_s3.get("rlp_facility_filter", "TESTS-SCHOOLS")
                    code = r.get_vars.get("$$code", default_filter)
                    if code:
                        session_s3.rlp_facility_filter = code
                        query = FS("~.organisation_id$project.code") == code
                        resource.add_filter(query)
                        if code == "TESTS-SCHOOLS":
                            title_list = T("Test Stations for School and Child Care Staff")
                        elif code == "TESTS-PUBLIC":
                            title_list = T("Test Stations for Everybody")

                s3.crud_strings["org_facility"].title_list = title_list

        elif r.representation == "plain":
            # Bypass REST method, return map popup directly
            from ..helpers import facility_map_popup
            result = {"bypass": True,
                      "output": facility_map_popup(record),
                      }
        else:
            # Single facility read view

            # No facility details editable here except comments
            for fn in table.fields:
                if fn != "comments":
                    table[fn].writable = False

            # No side menu except for OrgGroupAdmin
            if not is_org_group_admin:
                current.menu.options = None

            if not is_org_group_admin and \
                not auth.s3_has_role("ORG_ADMIN", for_pe=record.pe_id):

                s3.hide_last_update = True

                field = table.obsolete
                field.readable = field.writable = False

                field = table.organisation_id
                field.represent = s3db.org_OrganisationRepresent(show_link=False)

        resource.configure(summary = ({"name": "table",
                                       "label": "Table",
                                       "widgets": [{"method": "datatable"}]
                                       },
                                       {"name": "map",
                                       "label": "Map",
                                       "widgets": [{"method": "map", "ajax_init": True}],
                                       },
                                      ),
                           insertable = False,
                           deletable = False,
                           )

        return result
    s3.prep = prep

    standard_postp = s3.postp
    def postp(r, output):

        if r.representation == "plain" and r.record:
            # Prevent standard postp rewriting output
            pass
        elif callable(standard_postp):
            output = standard_postp(r, output)

        if not is_org_group_admin and \
            r.record and isinstance(output, dict):
            # Override list-button to go to summary
            buttons = output.get("buttons")
            if isinstance(buttons, dict) and "list_btn" in buttons:
                summary = r.url(method="summary", id="", component="")
                buttons["list_btn"] = S3CRUD.crud_button(label = T("List Facilities"),
                                                         _href = summary,
                                                         )
        return output
    s3.postp = postp

    # No rheader
    attr["rheader"] = None

    return attr

# END =========================================================================
