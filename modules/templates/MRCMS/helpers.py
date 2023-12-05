"""
    Helper functions and classes for MRCMS

    License: MIT
"""

import datetime

from dateutil.relativedelta import relativedelta

from gluon import current, URL, A, DIV, I, LABEL, OPTION, SELECT, SPAN, TAG

from core import FS, WorkflowOptions, RangeFilter, s3_fullname

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
def client_name_age(record):
    """
        Represent a client as name, gender and age; for case file rheader

        Args:
            record: the client record (pr_person)

        Returns:
            HTML
    """

    T = current.T

    pr_age = current.s3db.pr_age

    age = pr_age(record)
    if age is None:
        age = "?"
        unit = T("years")
    elif age == 0:
        age = pr_age(record, months=True)
        unit = T("months") if age != 1 else T("month")
    else:
        unit = T("years") if age != 1 else T("year")

    icons = {2: "fa fa-venus",
             3: "fa fa-mars",
             4: "fa fa-transgender-alt",
             }
    icon = I(_class=icons.get(record.gender, "fa fa-genderless"))

    client = TAG[""](s3_fullname(record),
                     SPAN(icon, "%s %s" % (age, unit), _class="client-gender-age"),
                     )
    return client

# -----------------------------------------------------------------------------
def last_seen_represent(date, label):
    """
        Represent last-seen-on date as warning if more than 3/5 days back;
        for case file rheader

        Args:
            date: the date (datetime.datetime)
            label: the represented date

        Returns:
            HTML or label
    """

    if date:
        days = relativedelta(datetime.datetime.utcnow(), date).days
        if days > 5:
            icon = I(_class="fa fa-exclamation-triangle")
            title = "> %s %s" % (days, current.T("days"))
            label = SPAN(label, icon, _class="last-seen-critical", _title=title)
        elif days > 3:
            icon = I(_class="fa fa-exclamation-circle")
            title = "> %s %s" % (days, current.T("days"))
            label = SPAN(label, icon, _class="last-seen-warning", _title=title)

    return label

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
class AbsenceFilter(RangeFilter):
    """ Custom filter for last-seen-on date, represented as "days since" """

    operator = ["gt"]

    # Untranslated labels for individual input boxes.
    input_labels = {"gt": "More than"}

    # -------------------------------------------------------------------------
    @classmethod
    def _variable(cls, selector, operator):

        return super()._variable("$$absence", operator)

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        css_base = self.css_base

        attr = self.attr
        css = attr.get("class")
        attr["_class"] = "%s %s" % (css, css_base) if css else css_base

        input_class = "%s-%s" % (css_base, "input")
        input_labels = self.input_labels
        input_elements = DIV()
        ie_append = input_elements.append

        _id = attr["_id"]
        _variable = self._variable
        selector = self.selector

        opts = self.opts
        minimum = opts.get("minimum", 1)
        maximum = opts.get("maximum", 7)

        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)

            # Selectable options
            input_opts = [OPTION("%s" % i, value=i)
                          for i in range(minimum, maximum + 1)
                          ]
            input_opts.insert(0, OPTION("", value=""))

            # Input Element
            input_box = SELECT(input_opts,
                               _id = input_id,
                               _class = input_class,
                               )

            variable = _variable(selector, operator)

            # Populate with the value, if given
            # if user has not set any of the limits, we get [] in values.
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = value
                input_box["value"] = value

            label = input_labels[operator]
            if label:
                label = DIV(LABEL("%s:" % T(input_labels[operator]),
                                  _for = input_id,
                                  ),
                            _class = "age-filter-label",
                            _style = "display:inline-block",
                            )

            ie_append(DIV(label,
                          DIV(input_box,
                              _class = "range-filter-widget",
                              _style = "display:inline-block",
                              ),
                          _class = "range-filter-field",
                          ))

        ie_append(DIV(LABEL(T("Days")),
                      _class = "age-filter-unit",
                      ))

        return input_elements

    # -------------------------------------------------------------------------
    @staticmethod
    def apply_filter(resource, get_vars):
        """
            Filter out volunteers who have a confirmed deployment during
            selected date interval
        """

        days = get_vars.get("$$absence__gt")
        if days:
            try:
                days = int(days)
            except (ValueError, TypeError):
                return

            now = current.request.utcnow
            latest = now - datetime.timedelta(hours = days * 24)
            resource.add_filter((FS("dvr_case.last_seen_on") != None) & \
                                (FS("dvr_case.last_seen_on") < latest))

# END =========================================================================
