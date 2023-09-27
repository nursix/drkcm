"""
    Custom rheaders for MRCMS

    License: MIT
"""

from gluon import current, A, URL, SPAN

from core import S3ResourceHeader, s3_fullname, s3_rheader_resource

from .helpers import hr_details

# =============================================================================
def dvr_rheader(r, tabs=None):
    """ Custom resource headers for DVR module """

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
            hint = lambda record: SPAN(T("Invalid Case"),
                                       _class="invalid-case",
                                       )

            if current.request.controller == "security":

                # No rheader except archived-hint
                case = resource.select(["dvr_case.archived"], as_rows=True)
                if case and case[0]["dvr_case.archived"]:
                    rheader_fields = [[(None, hint)]]
                    tabs = None
                else:
                    return None

            else:

                if not tabs:
                    tabs = [(T("Basic Details"), None),
                            (T("Family Members"), "group_membership/"),
                            (T("ID"), "identity"),
                            (T("Needs"), "case_activity"),
                            (T("Appointments"), "case_appointment"),
                            # case events
                            # site presence
                            (T("Photos"), "image"),
                            (T("Notes"), "case_note"),
                            #(T("Confiscation"), "seized_item"),
                            ]
                    if current.auth.s3_has_roles(("ORG_ADMIN",
                                                  "CASE_ADMIN",
                                                  #"CASE_MANAGER",
                                                  )):
                        tabs[5:5] = [(T("Presence"), "site_presence_event"),
                                     #(T("Events"), "case_event"),
                                     ]

                case = resource.select(["dvr_case.status_id",
                                        "dvr_case.archived",
                                        "dvr_case.household_size",
                                        #"dvr_case.transferable",
                                        "dvr_case.last_seen_on",
                                        "first_name",
                                        "last_name",
                                        "shelter_registration.shelter_unit_id",
                                        #"absence",
                                        ],
                                        represent = True,
                                        raw_data = True,
                                        ).rows

                if case:
                    # Extract case data
                    case = case[0]
                    archived = case["_row"]["dvr_case.archived"]
                    case_status = lambda row: case["dvr_case.status_id"]
                    household_size = lambda row: case["dvr_case.household_size"]
                    last_seen_on = lambda row: case["dvr_case.last_seen_on"]
                    shelter = lambda row: case["cr_shelter_registration.shelter_unit_id"]
                    #absence = lambda row: case["pr_person.absence"]
                    #transferable = lambda row: case["dvr_case.transferable"]
                else:
                    # Target record exists, but doesn't match filters
                    return None

                # TODO Refactor:
                # - presence and shelter information only if the organisation has a shelter
                # - presence and last_seen_on requires privileged role
                rheader_fields = [[(T("ID"), "pe_label"),
                                   (T("Case Status"), case_status),
                                   (T("Shelter"), shelter),
                                   ],
                                  #[("", None),
                                  # ("", None),
                                  # (T("Absent##presence"), absence),
                                  # ],
                                  ["date_of_birth",
                                   (T("Size of Family"), household_size),
                                   (T("Last seen on"), last_seen_on),
                                   ],
                                  ]

                if archived:
                    rheader_fields.insert(0, [(None, hint)])

                rheader_title = s3_fullname

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
                        (T("Housing Units"), "shelter_unit"),
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

# END =========================================================================
