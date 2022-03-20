"""
    Helper functions and classes for GIMS

    License: MIT
"""

from gluon import current, A, DIV, LI, SPAN, UL

from core import ICON, S3Represent, s3_str

# =============================================================================
def get_role_realms(role):
    """
        Get all realms for which a role has been assigned

        Args:
            role: the role ID or role UUID

        Returns:
            list of pe_ids the current user has the role for,
            None if the role is assigned site-wide, or an
            empty list if the user does not have the role, or
            no realm for the role
    """

    db = current.db
    auth = current.auth
    s3db = current.s3db

    if isinstance(role, str):
        gtable = auth.settings.table_group
        query = (gtable.uuid == role) & \
                (gtable.deleted == False)
        row = db(query).select(gtable.id,
                               cache = s3db.cache,
                               limitby = (0, 1),
                               ).first()
        role_id = row.id if row else None
    else:
        role_id = role

    role_realms = []
    user = auth.user
    if user:
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

# =============================================================================
def restrict_data_formats(r):
    """
        Restrict data exports (prevent S3XML/S3JSON of records)

        Args:
            the CRUDRequest
    """

    settings = current.deployment_settings

    allowed = ("html", "iframe", "popup", "aadata", "plain", "geojson", "pdf", "xls")
    if r.method in ("report", "timeplot", "filter", "validate"):
        allowed += ("json",)
    if r.method == "options":
        allowed += ("s3json",)
    settings.ui.export_formats = ("pdf", "xls")
    if r.representation not in allowed:
        r.error(403, current.ERROR.NOT_PERMITTED)

# =============================================================================
class ShelterDetails:
    """
        Field methods for compact representation of place and
        contact information of shelters
    """

    # -------------------------------------------------------------------------
    @staticmethod
    def place(row):

        if hasattr(row, "gis_location"):
            location = row.gis_location
        else:
            location = row

        return tuple(location.get(level)
                     for level in ("L3", "L2", "L1"))

    # -------------------------------------------------------------------------
    @staticmethod
    def place_represent(value, row=None):

        if isinstance(value, tuple) and len(value) == 3:
            l3 = value[0]
            lx = tuple(n if n else "-" for n in value[1:])
            output = DIV(_class = "place-repr",
                         )
            if l3:
                output.append(DIV(l3,
                                  _class = "place-name",
                                  ))
            if lx:
                output.append(DIV("%s / %s" % lx,
                                  _class = "place-info",
                                  ))
            return output
        else:
            return value if value else "-"

    # -------------------------------------------------------------------------
    @staticmethod
    def contact(row):

        if hasattr(row, "cr_shelter"):
            offer = row.cr_shelter
        else:
            offer = row

        return tuple(offer.get(detail)
                     for detail in ("contact_name",
                                    "phone",
                                    "email",
                                    ))

    # -------------------------------------------------------------------------
    @staticmethod
    def contact_represent(value, row=None):

        if isinstance(value, tuple) and len(value) == 3:

            if not any(value):
                return ""
            name, phone, email = value

            output = DIV(_class = "contact-repr",
                         )
            if name:
                output.append(SPAN(name,
                                   _class = "contact-name",
                                   ))

            if email or phone:
                details = DIV(_class="contact-details")
                if phone:
                    details.append(DIV(ICON("phone"),
                                       SPAN(phone,
                                            _class = "contact-phone"),
                                       _class = "contact-info",
                                       ))
                if email:
                    details.append(DIV(ICON("mail"),
                                       SPAN(A(email,
                                              _href="mailto:%s" % email,
                                              ),
                                            _class = "contact-email"),
                                       _class = "contact-info",
                                       ))
                output.append(details)

            return output
        else:
            return value if value else "-"

# =============================================================================
class ServiceListRepresent(S3Represent):

    always_list = True

    def render_list(self, value, labels, show_link=True):
        """
            Helper method to render list-type representations from
            bulk()-results.

            Args:
                value: the list
                labels: the labels as returned from bulk()
                show_link: render references as links, should
                           be the same as used with bulk()
        """

        show_link = show_link and self.show_link

        values = [v for v in value if v is not None]
        if not len(values):
            return ""

        if show_link:
            labels_ = (labels[v] if v in labels else self.default for v in values)
        else:
            labels_ = sorted(s3_str(labels[v]) if v in labels else self.default for v in values)

        if current.auth.permission.format == "xls":
            return ", ".join(labels_)

        html = UL(_class="service-list")
        for label in labels_:
            html.append(LI(label))

        return html

# END =========================================================================
