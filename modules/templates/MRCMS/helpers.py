"""
    Helper functions and classes for MRCMS

    License: MIT
"""

from gluon import current, URL, A

from core import FS, S3DateTime, WorkflowOptions, s3_fullname, s3_str

# =============================================================================
def get_role_realms(role):
    """
        Get all realms for which a role has been assigned

        Args:
            role: the role ID or role UUID

        Returns:
            - list of pe_ids the current user has the role for,
            - None if the role is assigned site-wide, or an
            - empty list if the user does not have the role, or has the role
              without realm
    """

    auth = current.auth

    if isinstance(role, str):
        role_id = auth.get_role_id(role)
    else:
        role_id = role

    role_realms = []
    user = auth.user
    if user and role_id:
        role_realms = user.realms.get(role_id, role_realms)

    return role_realms

# -----------------------------------------------------------------------------
def get_role_users(role_uid, pe_id=None, organisation_id=None):
    """
        Look up users with a certain user role for a certain organisation

        Args:
            role_uid: the role UUID
            pe_id: the pe_id of the organisation, or
            organisation_id: the organisation_id

        Returns:
            a dict {user_id: pe_id} of all active users with this
            role for the organisation
    """

    db = current.db

    auth = current.auth
    s3db = current.s3db

    if not pe_id and organisation_id:
        # Look up the realm pe_id from the organisation
        otable = s3db.org_organisation
        query = (otable.id == organisation_id) & \
                (otable.deleted == False)
        organisation = db(query).select(otable.pe_id,
                                        limitby = (0, 1),
                                        ).first()
        pe_id = organisation.pe_id if organisation else None

    # Get all users with this realm as direct OU ancestor
    from s3db.pr import pr_realm_users
    users = pr_realm_users(pe_id) if pe_id else None
    if users:
        # Look up those among the realm users who have
        # the role for either pe_id or for their default realm
        gtable = auth.settings.table_group
        mtable = auth.settings.table_membership
        ltable = s3db.pr_person_user
        utable = auth.settings.table_user
        join = [mtable.on((mtable.user_id == ltable.user_id) & \
                          ((mtable.pe_id == None) | (mtable.pe_id == pe_id)) & \
                          (mtable.deleted == False)),
                gtable.on((gtable.id == mtable.group_id) & \
                          (gtable.uuid == role_uid)),
                # Only verified+active accounts:
                utable.on((utable.id == mtable.user_id) & \
                          ((utable.registration_key == None) | \
                           (utable.registration_key == "")))
                ]
        query = (ltable.user_id.belongs(set(users.keys()))) & \
                (ltable.deleted == False)
        rows = db(query).select(ltable.user_id,
                                ltable.pe_id,
                                join = join,
                                )
        users = {row.user_id: row.pe_id for row in rows}

    return users if users else None

# -----------------------------------------------------------------------------
def get_role_emails(role_uid, pe_id=None, organisation_id=None):
    """
        Look up the emails addresses of users with a certain user role
        for a certain organisation

        Args:
            role_uid: the role UUID
            pe_id: the pe_id of the organisation, or
            organisation_id: the organisation_id

        Returns:
            a list of email addresses
    """

    contacts = None

    users = get_role_users(role_uid,
                           pe_id = pe_id,
                           organisation_id = organisation_id,
                           )

    if users:
        # Look up their email addresses
        ctable = current.s3db.pr_contact
        query = (ctable.pe_id.belongs(set(users.values()))) & \
                (ctable.contact_method == "EMAIL") & \
                (ctable.deleted == False)
        rows = current.db(query).select(ctable.value,
                                        orderby = ~ctable.priority,
                                        )
        contacts = list(set(row.value for row in rows))

    return contacts if contacts else None

# -----------------------------------------------------------------------------
def get_managed_orgs(role="ORG_ADMIN", group=None, cacheable=True):
    """
        Get organisations managed by the current user

        Args:
            role: the managing user role (default: ORG_ADMIN)
            group: the organisation group
            cacheable: whether the result can be cached

        Returns:
            list of organisation_ids
    """

    s3db = current.s3db

    otable = s3db.org_organisation
    query = (otable.deleted == False)

    realms = get_role_realms(role)
    if realms:
        query = (otable.realm_entity.belongs(realms)) & query
    elif realms is not None:
        # User does not have the required role, or at least not for any realms
        return []

    if group:
        gtable = s3db.org_group
        mtable = s3db.org_group_membership
        join = [gtable.on((mtable.organisation_id == otable.id) & \
                          (mtable.deleted == False) & \
                          (gtable.id == mtable.group_id) & \
                          (gtable.name == group)
                          )]
    else:
        join = None

    orgs = current.db(query).select(otable.id,
                                    cache = s3db.cache if cacheable else None,
                                    join = join,
                                    )
    return [o.id for o in orgs]

# =============================================================================
def get_user_orgs(roles=None, cacheable=True, limit=None):
    """
        Get the IDs of all organisations the user has any of the
        given roles for (default: STAFF|ORG_ADMIN)

        Args:
            roles: tuple|list of role IDs/UIDs
            cacheable: the result can be cached
            limit: limit to this number of organisation IDs

        Returns:
            list of organisation_ids (can be empty)
    """

    s3db = current.s3db

    if not roles:
        roles = ("STAFF", "ORG_ADMIN")

    realms = set()

    for role in roles:
        role_realms = get_role_realms(role)
        if role_realms is None:
            realms = None
            break
        if role_realms:
            realms.update(role_realms)

    otable = s3db.org_organisation
    query = (otable.deleted == False)
    if realms:
        query = (otable.pe_id.belongs(realms)) & query
    elif realms is not None:
        return []

    rows = current.db(query).select(otable.id,
                                    cache = s3db.cache if cacheable else None,
                                    limitby = (0, limit) if limit else None,
                                    )

    return [row.id for row in rows]

# -----------------------------------------------------------------------------
def get_user_sites(roles=None, site_type="cr_shelter", cacheable=True, limit=None):
    """
        Get the instance record IDs of all sites of the given type
        that belong to any of the user organisations

        Args:
            roles: tuple|list of role IDs/UIDs (see get_user_orgs)
            site_type: the instance table name
            cacheable: the result can be cached
            limit: limit to this number of organisation IDs

        Returns:
            list of instance record IDs (can be empty)
    """

    organisation_ids = get_user_orgs(roles=roles, cacheable=cacheable)

    if organisation_ids:
        s3db = current.s3db
        table = s3db.table(site_type)
        query = (table.organisation_id.belongs(organisation_ids)) & \
                (table.deleted == False)
        rows = current.db(query).select(table.id,
                                        cache = s3db.cache if cacheable else None,
                                        limitby = (0, limit) if limit else None,
                                        )
        site_ids = [row.id for row in rows]
    else:
        site_ids = []

    return site_ids

# =============================================================================
def get_current_site_organisation():
    """
        The organisation that manages the site where the user is currently
        registered as present;

        Returns:
            organisation ID
    """

    person_id = current.auth.s3_logged_in_person()
    if not person_id:
        return None

    from core import SitePresence
    site_id = SitePresence.get_current_site(person_id)

    table = current.s3db.org_site
    query = (table.site_id == site_id)
    row = current.db(query).select(table.organisation_id,
                                   limitby = (0, 1),
                                   ).first()

    return row.organisation_id if row else None

# =============================================================================
def get_default_organisation():
    """
        The organisation the user has the STAFF or ORG_ADMIN role for
        (if only one organisation)

        Returns:
            organisation ID
    """

    auth = current.auth
    if not auth.s3_logged_in() or auth.s3_has_roles("ADMIN", "ORG_GROUP_ADMIN"):
        return None

    s3 = current.response.s3
    organisation_id = s3.mrcms_default_organisation

    if organisation_id is None:

        organisation_ids = get_user_orgs(limit=2)
        if len(organisation_ids) == 1:
            organisation_id = organisation_ids[0]
        else:
            organisation_id = None
        s3.mrcms_default_organisation = organisation_id

    return organisation_id

# -----------------------------------------------------------------------------
def get_default_shelter():
    """
        The single shelter of the default organisation (if there is a default
        organisation with only a single shelter)

        Returns:
            shelter ID
    """
    # TODO refactor
    #      - use default organisation instead of user orgs (i.e. no default
    #        shelter without default organisation)

    auth = current.auth
    if not auth.s3_logged_in() or auth.s3_has_role("ADMIN"):
        return None

    s3 = current.response.s3
    shelter_id = s3.mrcms_default_shelter

    if shelter_id is None:

        shelter_ids = get_user_sites(site_type="cr_shelter", limit=2)
        if len(shelter_ids) == 1:
            shelter_id = shelter_ids[0]
        else:
            shelter_id = None
        s3.mrcms_default_shelter = shelter_id

    return shelter_id

# =============================================================================
def get_default_case_organisation():
    """
        The organisation the user can access case files for (if only one
        organisation)

        Returns:
            organisation ID
    """
    # TODO parametrize permission

    auth = current.auth
    if not auth.s3_logged_in() or auth.s3_has_role("ADMIN"):
        return None

    permissions = auth.permission
    permitted_realms = permissions.permitted_realms("dvr_case", "read")

    db = current.db
    s3db = current.s3db

    table = s3db.org_organisation
    query = (table.pe_id.belongs(permitted_realms)) & \
            (table.deleted == False)
    rows = db(query).select(table.id)
    if not rows:
        return None
    if len(rows) == 1:
        return rows.first().id

    # TODO remove this fallback?
    site_org = get_current_site_organisation()
    if site_org:
        organisation_ids = [row.id for row in rows]
        if site_org in organisation_ids:
            return site_org

    return None

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
        current_shelter = reg.shelter_id if reg else None
    else:
        current_shelter = None

    stable = s3db.cr_shelter
    status_query = (stable.status == 2) & \
                   (stable.obsolete == False)
    if current_shelter:
        status_query |= (stable.id == current_shelter)

    query = (stable.organisation_id == organisation_id) & \
            status_query & \
            (stable.deleted == False)
    rows = db(query).select(stable.id)
    shelters = [row.id for row in rows]

    return shelters

# -----------------------------------------------------------------------------
def get_default_case_shelter(person_id):
    """
        Get the default shelter (and housing unit) for a case

        Args:
            person_id: use the shelter registration of this person as
                       reference, if available
        Returns:
            tuple (shelter_id, unit_id)
    """

    db = current.db
    s3db = current.s3db

    shelter_id = unit_id = None

    if person_id:
        # Get the current shelter_id and unit_id for the person_id
        # if they are registered as planned or checked-in to a shelter
        rtable = s3db.cr_shelter_registration
        query = (rtable.person_id == person_id) & \
                (rtable.deleted == False)
        row = db(query).select(rtable.shelter_id,
                               rtable.shelter_unit_id,
                               rtable.registration_status,
                               limitby = (0, 1),
                               ).first()
        if row:
            shelter_id = row.shelter_id
            if row.registration_status != 3:
                unit_id = row.shelter_unit_id
            else:
                # Person is checked-out, so housing unit no longer valid
                unit_id = None

    if not shelter_id:
        # Look up the only available shelter from the default case organisation
        organisation_id = get_default_case_organisation()
        if organisation_id:
            available_shelters = get_available_shelters(organisation_id)
            if len(available_shelters) == 1:
                shelter_id = available_shelters[0]

    return shelter_id, unit_id

# =============================================================================
def account_status(record, represent=True):
    """
        Checks the status of the user account for a person

        Args:
            record: the person record
            represent: represent the result as workflow option

        Returns:
            workflow option HTML if represent=True, otherwise boolean
    """

    db = current.db
    s3db = current.s3db

    ltable = s3db.pr_person_user
    utable = current.auth.table_user()

    query = (ltable.pe_id == record.pe_id) & \
            (ltable.deleted == False) & \
            (utable.id == ltable.user_id)

    account = db(query).select(utable.id,
                               utable.registration_key,
                               cache = s3db.cache,
                               limitby = (0, 1),
                               ).first()

    if account:
        status = "DISABLED" if account.registration_key else "ACTIVE"
    else:
        status = "N/A"

    if represent:
        represent = WorkflowOptions(("N/A", "nonexistent", "grey"),
                                    ("DISABLED", "disabled##account", "red"),
                                    ("ACTIVE", "active", "green"),
                                    ).represent
        status = represent(status)

    return status

# -----------------------------------------------------------------------------
def hr_details(record):
    """
        Looks up relevant HR details for a person

        Args:
            record: the pr_person record in question

        Returns:
            dict {"organisation": organisation name,
                  "account": account status,
                  }

        Note:
            all data returned are represented (not raw data)
    """

    db = current.db
    s3db = current.s3db

    person_id = record.id

    # Get HR record
    htable = s3db.hrm_human_resource
    query = (htable.person_id == person_id)

    hr_id = current.request.get_vars.get("human_resource.id")
    if hr_id:
        query &= (htable.id == hr_id)
    query &= (htable.deleted == False)

    rows = db(query).select(htable.organisation_id,
                            htable.org_contact,
                            htable.status,
                            orderby = htable.created_on,
                            )
    if not rows:
        human_resource = None
    elif len(rows) > 1:
        rrows = rows
        rrows = rrows.filter(lambda row: row.status == 1) or rrows
        rrows = rrows.filter(lambda row: row.org_contact) or rrows
        human_resource = rrows.first()
    else:
        human_resource = rows.first()

    output = {"organisation": "",
              "account": account_status(record),
              }

    if human_resource:
        otable = s3db.org_organisation

        # Link to organisation
        query = (otable.id == human_resource.organisation_id)
        organisation = db(query).select(otable.id,
                                        otable.name,
                                        limitby = (0, 1),
                                        ).first()
        output["organisation"] = A(organisation.name,
                                   _href = URL(c = "org",
                                               f = "organisation",
                                               args = [organisation.id],
                                               ),
                                   )
    return output

# =============================================================================
class MRCMSSiteActivityReport:
    """
        Helper class to produce site activity reports ("Residents Report")
    """

    def __init__(self, site_id=None, date=None):
        """
            Args:
                site_id: the site ID (defaults to default site)
                date: the date of the report (defaults to today)
        """

        if site_id is None:
            site_id = current.deployment_settings.get_org_default_site()
        self.site_id = site_id

        if date is None:
            date = current.request.utcnow.date()
        self.date = date

    # -------------------------------------------------------------------------
    def extract(self):
        """
            Extract the data for this report
        """

        db = current.db
        s3db = current.s3db

        T = current.T

        site_id = self.site_id
        date = self.date

        # Get all flags for which cases shall be excluded
        ftable = s3db.dvr_case_flag
        query = (ftable.nostats == True) & \
                (ftable.deleted == False)
        rows = db(query).select(ftable.id)
        nostats = set(row.id for row in rows)

        # Identify the relevant cases
        ctable = s3db.dvr_case
        ltable = s3db.dvr_case_flag_case

        num_nostats_flags = ltable.id.count()
        left = ltable.on((ltable.person_id == ctable.person_id) & \
                         (ltable.flag_id.belongs(nostats)) & \
                         (ltable.deleted == False))

        query = (ctable.site_id == site_id) & \
                ((ctable.date == None) | (ctable.date <= date)) & \
                ((ctable.closed_on == None) | (ctable.closed_on >= date)) & \
                (ctable.archived != True) & \
                (ctable.deleted != True)


        rows = db(query).select(ctable.id,
                                ctable.person_id,
                                ctable.date,
                                ctable.closed_on,
                                num_nostats_flags,
                                groupby = ctable.id,
                                left = left,
                                )

        # Count them
        old_total, ins, outs = 0, 0, 0
        person_ids = set()
        for row in rows:
            if row[num_nostats_flags]:
                continue
            case = row.dvr_case
            person_ids.add(case.person_id)
            if not case.date or case.date < date:
                old_total += 1
            else:
                ins += 1
            if case.closed_on and case.closed_on == date:
                outs += 1
        result = {"old_total": old_total,
                  "new_total": old_total - outs + ins,
                  "ins": ins,
                  "outs": outs,
                  }

        # Add completed appointments as pr_person components
        atypes = {"BAMF": None,
                  "GU": None,
                  "Transfer": None,
                  "X-Ray": None,
                  "Querverlegung": None,
                  }
        COMPLETED = 4
        attable = s3db.dvr_case_appointment_type
        query = attable.name.belongs(set(atypes.keys()))
        rows = db(query).select(attable.id,
                                attable.name,
                                )
        add_components = s3db.add_components
        hooks = []
        for row in rows:
            type_id = row.id
            name = row.name
            atypes[name] = alias = "appointment%s" % type_id
            hook = {"name": alias,
                    "joinby": "person_id",
                    "filterby": {"type_id": type_id,
                                 "status": COMPLETED,
                                 },
                    }
            hooks.append(hook)
        s3db.add_components("pr_person", dvr_case_appointment = hooks)
        date_completed = lambda t: (T("%(event)s on") % {"event": T(t)},
                                    "%s.date" % atypes[t],
                                    )

        # Filtered component for paid allowances
        PAID = 2
        add_components("pr_person",
                       dvr_allowance = {"name": "payment",
                                        "joinby": "person_id",
                                        "filterby": {"status": PAID},
                                        },
                       )

        # Represent paid_on as date
        atable = s3db.dvr_allowance
        atable.paid_on.represent = lambda dt: \
                                   S3DateTime.date_represent(dt,
                                                             utc=True,
                                                             )

        # Filtered component for preliminary residence permit
        s3db.add_components("pr_person",
                            pr_identity = {"name": "residence_permit",
                                           "joinby": "person_id",
                                           "filterby": {"type": 5},
                                           "multiple": False,
                                           },
                            )

        # Filtered component for family
        s3db.add_components("pr_person",
                            pr_group = {"name": "family",
                                        "link": "pr_group_membership",
                                        "joinby": "person_id",
                                        "key": "group_id",
                                        "filterby": {"group_type": 7},
                                        },
                            )

        # Get family roles
        gtable = s3db.pr_group
        mtable = s3db.pr_group_membership
        join = gtable.on(gtable.id == mtable.group_id)
        query = (mtable.person_id.belongs(person_ids)) & \
                (mtable.deleted != True) & \
                (gtable.group_type == 7)
        rows = db(query).select(mtable.person_id,
                                mtable.group_head,
                                mtable.role_id,
                                join = join,
                                )

        # Bulk represent all possible family roles (to avoid repeated lookups)
        represent = mtable.role_id.represent
        rtable = s3db.pr_group_member_role
        if hasattr(represent, "bulk"):
            query = (rtable.group_type == 7) & (rtable.deleted != True)
            roles = db(query).select(rtable.id)
            role_ids = [role.id for role in roles]
            represent.bulk(role_ids)

        # Create a dict of {person_id: role}
        roles = {}
        HEAD_OF_FAMILY = T("Head of Family")
        for row in rows:
            person_id = row.person_id
            role = row.role_id
            if person_id in roles:
                continue
            if (row.group_head):
                roles[person_id] = HEAD_OF_FAMILY
            elif role:
                roles[person_id] = represent(role)

        # Field method to determine the family role
        def family_role(row):
            person_id = row["pr_person.id"]
            return roles.get(person_id, "")

        # Dummy virtual fields to produce empty columns
        from s3dal import Field
        ptable = s3db.pr_person
        empty = lambda row: ""
        if not hasattr(ptable, "xray_place"):
            ptable.xray_place = Field.Method("xray_place", empty)
        if not hasattr(ptable, "family_role"):
            ptable.family_role = Field.Method("family_role", family_role)

        # List fields for the report
        list_fields = ["family.id",
                       (T("ID"), "pe_label"),
                       (T("Name"), "last_name"),
                       "first_name",
                       "date_of_birth",
                       "gender",
                       "person_details.nationality",
                       (T("Family Role"), "family_role"),
                       (T("Room No."), "shelter_registration.shelter_unit_id"),
                       "case_flag_case.flag_id",
                       "dvr_case.comments",
                       date_completed("GU"),
                       date_completed("X-Ray"),
                       (T("X-Ray Place"), "xray_place"),
                       date_completed("BAMF"),
                       (T("BÜMA valid until"), "dvr_case.valid_until"),
                       (T("Preliminary Residence Permit until"), "residence_permit.valid_until"),
                       (T("Allowance Payments"), "payment.paid_on"),
                       (T("Admitted on"), "dvr_case.date"),
                       "dvr_case.origin_site_id",
                       date_completed("Transfer"),
                       date_completed("Querverlegung"),
                       #"dvr_case.closed_on",
                       "dvr_case.status_id",
                       "dvr_case.destination_site_id",
                       ]

        query = FS("id").belongs(person_ids)
        resource = s3db.resource("pr_person", filter = query)

        data = resource.select(list_fields,
                               represent = True,
                               raw_data = True,
                               # Keep families together, eldest member first
                               orderby = ["pr_family_group.id",
                                          "pr_person.date_of_birth",
                                          ],
                               )

        # Generate headers, labels, types for XLS report
        rfields = data.rfields
        columns = []
        headers = {}
        types = {}
        for rfield in rfields:
            colname = rfield.colname
            if colname in ("dvr_case_flag_case.flag_id",
                           "pr_family_group.id",
                           ):
                continue
            columns.append(colname)
            headers[colname] = rfield.label
            types[colname] = rfield.ftype

        # Post-process rows
        rows = []
        for row in data.rows:

            flags = "dvr_case_flag_case.flag_id"
            comments = "dvr_case.comments"

            raw = row["_row"]
            if raw[flags]:
                items = ["%s: %s" % (T("Advice"), s3_str(row[flags]))]
                if raw[comments]:
                    items.insert(0, raw[comments])
                row[comments] = ", ".join(items)
            rows.append(row)

        # Add XLS report data to result
        report = {"columns": columns,
                  "headers": headers,
                  "types": types,
                  "rows": rows,
                  }
        result["report"] = report

        return result

    # -------------------------------------------------------------------------
    def store(self, authorised=None):
        """
            Store this report in dvr_site_activity
        """

        db = current.db
        s3db = current.s3db
        auth = current.auth
        settings = current.deployment_settings

        # Table name and table
        tablename = "dvr_site_activity"
        table = s3db.table(tablename)
        if not table:
            return None

        # Get the current site activity record
        query = (table.date == self.date) & \
                (table.site_id == self.site_id) & \
                (table.deleted != True)
        row = db(query).select(table.id,
                               limitby = (0, 1),
                               orderby = ~table.created_on,
                               ).first()

        # Check permission
        if authorised is None:
            has_permission = current.auth.s3_has_permission
            if row:
                authorised = has_permission("update", tablename, record_id=row.id)
            else:
                authorised = has_permission("create", tablename)
        if not authorised:
            from core import S3PermissionError
            raise S3PermissionError

        # Extract the data
        data = self.extract()

        # Custom header for Excel Export (disabled for now)
        settings.base.xls_title_row = lambda sheet: self.summary(sheet, data)

        # Export as XLS
        title = current.T("Resident List")
        from core import DataExporter
        exporter = DataExporter.xls
        report = exporter(data["report"],
                          title = title,
                          as_stream = True,
                          )

        # Construct the filename
        filename = "%s_%s_%s.xls" % (title, self.site_id, str(self.date))

        # Store the report
        report_ = table.report.store(report, filename)
        record = {"site_id": self.site_id,
                  "old_total": data["old_total"],
                  "new_total": data["new_total"],
                  "cases_new": data["ins"],
                  "cases_closed": data["outs"],
                  "date": self.date,
                  "report": report_,
                  }

        # Customize resource
        from core import CRUDRequest
        r = CRUDRequest("dvr", "site_activity",
                        current.request,
                        args = [],
                        get_vars = {},
                        )
        r.customise_resource("dvr_site_activity")

        if row:
            # Trigger auto-delete of the previous file
            row.update_record(report=None)
            # Update it
            success = row.update_record(**record)
            if success:
                s3db.onaccept(table, record, method="create")
                result = row.id
            else:
                result = None
        else:
            # Create a new one
            record_id = table.insert(**record)
            if record_id:
                record["id"] = record_id
                s3db.update_super(table, record)
                auth.s3_set_record_owner(table, record_id)
                auth.s3_make_session_owner(table, record_id)
                s3db.onaccept(table, record, method="create")
                result = record_id
            else:
                result = None

        return result

    # -------------------------------------------------------------------------
    def summary(self, sheet, data=None):
        """
            Header for the Excel sheet

            Args:
                sheet: the sheet
                data: the data dict from extract()

            Returns:
                the number of rows in the header
        """

        length = 3

        if sheet is not None and data is not None:

            T = current.T
            output = (("Date", S3DateTime.date_represent(self.date, utc=True)),
                      ("Previous Total", data["old_total"]),
                      ("Admissions", data["ins"]),
                      ("Departures", data["outs"]),
                      ("Current Total", data["new_total"]),
                      )

            import xlwt
            label_style = xlwt.XFStyle()
            label_style.font.bold = True

            col_index = 3
            for label, value in output:
                label_ = s3_str(T(label))
                value_ = s3_str(value)

                # Adjust column width
                width = max(len(label_), len(value_))
                sheet.col(col_index).width = max(width * 310, 2000)

                # Write the label
                current_row = sheet.row(0)
                current_row.write(col_index, label_, label_style)

                # Write the data
                current_row = sheet.row(1)
                current_row.write(col_index, value_)
                col_index += 1

        return length

# END =========================================================================
