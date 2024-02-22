"""
    Custom rheaders for MRCMS

    License: MIT
"""

from gluon import current, A, I, URL, SPAN

from core import S3ResourceHeader, s3_fullname, s3_rheader_resource

from .helpers import client_name_age, hr_details, last_seen_represent

# =============================================================================
def act_rheader(r, tabs=None):
    """ Custom resource headers for ACT module """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:

        T = current.T

        if tablename == "act_activity":
            if not tabs:
                tabs = [(T("Basic Details"), None),
                        (T("Participants"), "beneficiary"),
                        (T("Documents"), "document"),
                        ]
            rheader_fields = [["type_id", "place"],
                              ["date", "time"],
                              ["end_date"],
                              ]
            rheader_title = "name"

            rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
            rheader = rheader(r, table=resource.table, record=record)

    return rheader

# =============================================================================
def dvr_rheader(r, tabs=None):
    """ Custom resource headers for DVR module """

    auth = current.auth
    has_permission = auth.s3_has_permission
    accessible_url = auth.permission.accessible_url

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:
        T = current.T

        if tablename == "pr_person":
            # Case file

            # "Case Archived" hint
            hint = lambda record: SPAN(T("Invalid Case"), _class="invalid-case")

            c = r.controller

            if c == "security":

                # No rheader except archived-hint
                case = resource.select(["dvr_case.archived"], as_rows=True)
                if case and case[0]["dvr_case.archived"]:
                    rheader_fields = [[(None, hint)]]
                    tabs = None
                else:
                    return None

            else:
                if not tabs:
                    tabs = [# Common
                            (T("Basic Details"), None),
                            (T("Family Members"), "group_membership/"),
                            # identity
                            (T("Appointments"), "case_appointment"),

                            # Counsel only:
                            # vulnerability
                            # case_activity
                            # response_action

                            # Case Administration only:
                            # case_event
                            # site_presence
                            # image
                            # seized_item

                            # Both
                            # "document"
                            # "case_note"
                            ]

                    has_roles = current.auth.s3_has_roles
                    if c == "counsel":
                        if has_roles(("CASE_ADMIN", "CASE_MANAGER")):
                            tabs.extend([(T("Vulnerabilities"), "vulnerability"),
                                         (T("Needs"), "case_activity"),
                                         (T("Measures"), "response_action"),
                                         ])
                    else:
                        tabs[2:2] = [(T("ID"), "identity"),
                                     (T("Service Contacts"), "service_contact"),
                                     ]
                        if has_roles(("CASE_ADMIN",)):
                            tabs.append((T("Events"), "case_event"))
                        if has_roles(("SHELTER_ADMIN", "SHELTER_MANAGER")):
                            tabs.append((T("Presence"), "site_presence_event"))
                        tabs.extend([(T("Photos"), "image"),
                                     #(T("Confiscation"), "seized_item"),
                                     ])

                    tabs.extend([(T("Documents"), "document/"),
                                 (T("Notes"), "case_note"),
                                 ])

                case = resource.select(["dvr_case.status_id",
                                        "dvr_case.archived",
                                        "dvr_case.reference",
                                        "dvr_case.household_size",
                                        #"dvr_case.transferable",
                                        "dvr_case.last_seen_on",
                                        "first_name",
                                        "last_name",
                                        "person_details.nationality",
                                        "shelter_registration.shelter_id",
                                        "shelter_registration.shelter_unit_id",
                                        #"absence",
                                        ],
                                        represent = True,
                                        raw_data = True,
                                        ).rows

                if case:
                    # Extract case data
                    case = case[0]
                    raw = case["_row"]

                    case_reference = lambda row: case["dvr_case.reference"]
                    nationality_label = case["pr_person_details.nationality"]
                    nationality = lambda row: SPAN(raw["pr_person_details.nationality"],
                                                   _title = nationality_label,
                                                   _style = "cursor:pointer",
                                                   )

                    case_status = lambda row: case["dvr_case.status_id"]
                    household_size = lambda row: case["dvr_case.household_size"]

                    # Represent shelter as link to shelter overview, if permitted
                    shelter_id = raw["cr_shelter_registration.shelter_id"]
                    shelter_url = accessible_url(c="cr", f="shelter", args=[shelter_id, "overview"])
                    shelter_name = case["cr_shelter_registration.shelter_id"]
                    if shelter_url:
                        shelter = lambda row: A(shelter_name, _href=shelter_url)
                    else:
                        shelter = lambda row: shelter_name
                    unit = lambda row: case["cr_shelter_registration.shelter_unit_id"]

                    # Represent last-seen-date as warning, if too long ago
                    last_seen_on = lambda row: \
                                   last_seen_represent(raw["dvr_case.last_seen_on"],
                                                       case["dvr_case.last_seen_on"],
                                                       )
                else:
                    # Target record exists, but doesn't match filters
                    return None

                rheader_fields = [[(T("ID"), "pe_label"),
                                   (T("Principal Ref.No."), case_reference),
                                   (T("Shelter"), shelter),
                                   ],
                                  ["date_of_birth",
                                   (T("Case Status"), case_status),
                                   (T("Housing Unit"), unit),
                                   ],
                                  [(T("Nationality"), nationality),
                                   (T("Size of Family"), household_size),
                                   (T("Last seen on"), last_seen_on),
                                   ],
                                  ]

                icon = link = None
                if raw["dvr_case.archived"]:
                    rheader_fields.insert(0, [(None, hint)])

                # Link to switch case file perspective
                elif c == "dvr" and \
                     has_permission("read", "pr_person", c="counsel", f="person", record_id=record.id):
                    icon = "arrow-circle-right"
                    link = A(T("Counseling"), _href=URL(c="counsel", f="person", args=[record.id]))
                elif c == "counsel" and \
                     has_permission("read", "pr_person", c="dvr", f="person", record_id=record.id):
                    icon = "arrow-circle-left"
                    link = A(T("Manage"), _href=URL(c="dvr", f="person", args=[record.id]))
                if link:
                    # TODO move CSS into theme
                    link.insert(0, I(_class = "fa fa-%s" % icon,
                                     _style = "margin-right:0.3rem;vertical-align:middle;",
                                     ))
                    rheader_fields.append([(None, lambda item: link, 6)])

                rheader_title = client_name_age

                # Generate rheader XML
                rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
                rheader = rheader(r, table=resource.table, record=record)

                # Add profile picture
                from core import s3_avatar_represent
                record_id = record.id
                rheader.insert(0, A(s3_avatar_represent(record_id,
                                                        "pr_person",
                                                        _class = "rheader-avatar",
                                                        ),
                                    _href=URL(f = "person",
                                              args = [record_id, "image"],
                                              vars = r.get_vars,
                                              ),
                                    )
                               )

                return rheader

        rheader = S3ResourceHeader(rheader_fields, tabs)(r,
                                                         table=resource.table,
                                                         record=record,
                                                         )

    return rheader

# =============================================================================
def org_rheader(r, tabs=None):
    """ Custom resource headers for ORG module """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:
        T = current.T
        auth = current.auth

        if tablename == "org_group":

            if not tabs:
                tabs = [(T("Basic Details"), None),
                        (T("Member Organizations"), "organisation"),
                        (T("Documents"), "document"),
                        ]

            rheader_fields = []
            rheader_title = "name"

        elif tablename == "org_organisation":

            if not tabs:
                tabs = [(T("Basic Details"), None),
                        #(T("Offices"), "office"),
                        #(T("Staff"), "human_resource"),
                        (T("Documents"), "document"),
                        ]
                if auth.s3_has_permission("read", "pr_person", c="hrm", f="person"):
                    tabs.insert(-1, (T("Staff"), "human_resource"))

            rheader_fields = []
            rheader_title = "name"

        elif tablename == "org_facility":

            if not tabs:
                tabs = [(T("Basic Details"), None),
                        ]

            rheader_fields = [["name", "email"],
                              ["organisation_id", "phone1"],
                              ["location_id", "phone2"],
                              ]
            rheader_title = None

        rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
        rheader = rheader(r, table=resource.table, record=record)

    return rheader

# =============================================================================
def cr_rheader(r, tabs=None):
    """ Custom resource headers for shelter registry """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:
        T = current.T

        if tablename == "cr_shelter":

            if not tabs:
                tabs = [(T("Basic Details"), None, {}, "read"),
                        (T("Overview"), "overview"),
                        (T("Housing Units"), "shelter_unit"),
                        (T("Journal"), "shelter_note"),
                        (T("Images"), "image"),
                        (T("Documents"), "document"),
                        ]

            rheader_fields = [["organisation_id",
                               ],
                              ["location_id",
                               ],
                              ]
            rheader_title = "name"

        rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
        rheader = rheader(r, table=resource.table, record=record)

    return rheader

# -----------------------------------------------------------------------------
def hrm_rheader(r, tabs=None):
    """ Custom resource headers for HRM """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:

        T = current.T

        if tablename == "pr_person":
            # Staff file

            tabs = [(T("Person Details"), None, {}, "read"),
                    (T("Contact Information"), "contacts"),
                    (T("Address"), "address"),
                    (T("ID"), "identity"),
                    (T("Staff Record"), "human_resource"),
                    (T("Photos"), "image"),
                    ]

            details = hr_details(record)
            rheader_fields = [[(T("User Account"), lambda i: details["account"])],
                              ]

            organisation = details["organisation"]
            if organisation:
                rheader_fields[0].insert(0, (T("Organization"), lambda i: organisation))

            rheader_title = s3_fullname

            rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
            rheader = rheader(r, table=resource.table, record=record)

            # Add profile picture
            from core import s3_avatar_represent
            record_id = record.id
            rheader.insert(0, A(s3_avatar_represent(record_id,
                                                    "pr_person",
                                                    _class = "rheader-avatar",
                                                    ),
                                _href=URL(f = "person",
                                          args = [record_id, "image"],
                                          vars = r.get_vars,
                                          ),
                                ))

    return rheader

# -----------------------------------------------------------------------------
def default_rheader(r, tabs=None):
    """ Custom resource header for user profile """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:

        T = current.T

        if tablename == "pr_person":
            # Personal profile
            tabs = [(T("Person Details"), None),
                    (T("User Account"), "user_profile"),
                    (T("ID"), "identity"),
                    (T("Contact Information"), "contacts"),
                    (T("Address"), "address"),
                    (T("Staff Record"), "human_resource"),
                    ]
            rheader_fields = []
            rheader_title = s3_fullname

            rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
            rheader = rheader(r, table=resource.table, record=record)

    return rheader

# -----------------------------------------------------------------------------
def security_rheader(r, tabs=None):
    """ Custom resource header for SECURITY module """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:

        T = current.T

        if tablename == "security_seized_item_depository":
            if not tabs:
                tabs = [(T("Basic Details"), None),
                        (T("Seized Items"), "seized_item"),
                        ]
            rheader_fields = []
            rheader_title = "name"

            rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
            rheader = rheader(r, table=resource.table, record=record)

    return rheader

# END =========================================================================
