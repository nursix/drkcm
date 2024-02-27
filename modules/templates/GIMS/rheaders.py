"""
    Custom rheaders for GIMS

    License: MIT
"""

from gluon import current

from core import S3ResourceHeader, s3_rheader_resource

# =============================================================================
def profile_rheader(r, tabs=None):
    """ Custom rheader for default/person """

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

            tabs = [(T("Person Details"), None),
                    (T("User Account"), "user_profile"),
                    (T("Contact Information"), "contacts"),
                    ]
            rheader_fields = []

        rheader = S3ResourceHeader(rheader_fields, tabs)(r,
                                                         table = resource.table,
                                                         record = record,
                                                         )
    return rheader

# =============================================================================
def org_rheader(r, tabs=None):
    """ ORG custom resource headers """

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

        if tablename == "org_organisation":

            auth = current.auth
            is_org_group_admin = auth.s3_has_role("ORG_GROUP_ADMIN")

            if not tabs:
                tabs = [(T("Organization"), None),
                        (T("Offices"), "office"),
                        ]
                if is_org_group_admin:
                    tabs.append((T("Invite"), "invite"))
                if auth.s3_has_permission("update", "org_organisation", record_id=record.id):
                    tabs.append((T("Staff"), "human_resource"))

            # Check for active user accounts:
            rheader_fields = []
            if is_org_group_admin:

                from templates.RLPPTM.helpers import get_org_accounts
                active = get_org_accounts(record.id)[0]

                active_accounts = lambda row: len(active)
                rheader_fields.append([(T("Active Accounts"), active_accounts)])

            rheader_title = "name"

        rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
        rheader = rheader(r, table = resource.table, record = record)

    return rheader

# =============================================================================
def cr_rheader(r, tabs=None):
    """ CR custom resource headers """

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
                tabs = [(T("Basic Details"), None),
                        (T("Status History"), "status"),
                        (T("Photos"), "image"),
                        #(T("Documents"), "document"),
                        ]

            rheader_fields = [["organisation_id", "available_capacity"],
                              ["location_id"],
                              ["status"],
                              ]
            rheader_title = "name"

        elif tablename == "cr_reception_center":

            if not tabs:
                tabs = [(T("Basic Details"), None),
                        (T("Status History"), "status"),
                        #(T("Photos"), "image"),
                        #(T("Documents"), "document"),
                        ]

            rheader_fields = [["organisation_id", "capacity"],
                              ["location_id", "population"],
                              ["status", "free_allocable_capacity"],
                              ]
            rheader_title = "name"

        else:
            return rheader

        rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
        rheader = rheader(r, table = resource.table, record = record)

    return rheader

# END =========================================================================
