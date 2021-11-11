"""
    Permission Handling

    Copyright: (c) 2010-2021 Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

__all__ = ("S3Permission",
           )

from collections import OrderedDict

from gluon import current, redirect, HTTP, URL
from gluon.storage import Storage

from s3dal import Field, Row, Table, original_tablename

from ..model import S3MetaFields
from ..errors import S3PermissionError
from ..tools import s3_get_extension

# =============================================================================
class S3Permission:
    """ S3 Class to handle permissions """

    TABLENAME = "s3_permission"

    CREATE = 0x0001     # Permission to create new records
    READ = 0x0002       # Permission to read records
    UPDATE = 0x0004     # Permission to update records
    DELETE = 0x0008     # Permission to delete records
    REVIEW = 0x0010     # Permission to review unapproved records
    APPROVE = 0x0020    # Permission to approve records
    PUBLISH = 0x0040    # Permission to publish records outside of Eden

    ALL = CREATE | READ | UPDATE | DELETE | REVIEW | APPROVE | PUBLISH
    NONE = 0x0000 # must be 0!

    PERMISSION_OPTS = OrderedDict((
        (CREATE, "CREATE"),
        (READ, "READ"),
        (UPDATE, "UPDATE"),
        (DELETE, "DELETE"),
        (REVIEW, "REVIEW"),
        (APPROVE, "APPROVE"),
        #[PUBLISH, "PUBLISH"],   # currently unused
        ))

    # URL Method <-> required permission
    METHODS = {"create": CREATE,
               "read": READ,
               "update": UPDATE,
               "delete": DELETE,
               "list": READ,
               "datatable": READ,
               "datalist": READ,
               "map": READ,
               "report": READ,
               "timeplot": READ,
               "import": CREATE,
               "review": REVIEW,
               "approve": APPROVE,
               "reject": APPROVE,
               "publish": PUBLISH,
               }

    # -------------------------------------------------------------------------
    def __init__(self, auth, tablename=None):
        """
            Constructor, invoked by AuthS3.__init__

            Args:
                auth: the AuthS3 instance
                tablename: the name for the permissions table (override)
        """

        db = current.db

        # Instantiated once per request, but before Auth tables
        # are defined and authentication is checked, thus no use
        # to check permissions in the constructor

        # Store auth reference in self because current.auth is not
        # available at this point yet, but needed in define_table.
        self.auth = auth

        self.error = S3PermissionError

        settings = current.deployment_settings

        # Policy: which level of granularity do we want?
        self.policy = settings.get_security_policy()
        # ACLs to control access per controller:
        self.use_cacls = self.policy in (3, 4, 5, 6, 7)
        # ACLs to control access per function within controllers:
        self.use_facls = self.policy in (4, 5, 6, 7)
        # ACLs to control access per table:
        self.use_tacls = self.policy in (5, 6, 7)
        # Authorization takes realm entity into account:
        self.entity_realm = self.policy in (6, 7)
        # Permissions shared along the hierarchy of entities:
        self.entity_hierarchy = self.policy == 7

        # Permissions table
        self.tablename = tablename or self.TABLENAME
        if self.tablename in db:
            self.table = db[self.tablename]
        else:
            self.table = None

        # Error messages
        T = current.T
        self.INSUFFICIENT_PRIVILEGES = T("Insufficient Privileges")
        self.AUTHENTICATION_REQUIRED = T("Authentication Required")

        # Request information
        request = current.request
        self.controller = request.controller
        self.function = request.function

        # Request format
        self.format = s3_get_extension()

        # Settings
        self.record_approval = settings.get_auth_record_approval()
        self.strict_ownership = settings.get_security_strict_ownership()

        # Initialize cache
        self.permission_cache = {}
        self.query_cache = {}

        # Pages which never require permission:
        # Make sure that any data access via these pages uses
        # accessible_query explicitly!
        self.unrestricted_pages = ("default/index",
                                   "default/user",
                                   "default/contact",
                                   "default/about",
                                   )

        # Default landing pages
        _next = URL(args=request.args, vars=request.get_vars)
        self.homepage = URL(c="default", f="index")
        self.loginpage = URL(c="default", f="user",
                             args = "login",
                             vars = {"_next": _next},
                             )

    # -------------------------------------------------------------------------
    def clear_cache(self):
        """ Clear any cached permissions or accessible-queries """

        self.permission_cache = {}
        self.query_cache = {}

    # -------------------------------------------------------------------------
    def check_settings(self):
        """
            Check whether permission-relevant settings have changed
            during the request, and clear the cache if so.
        """

        clear_cache = False
        settings = current.deployment_settings

        record_approval = settings.get_auth_record_approval()
        if record_approval != self.record_approval:
            clear_cache = True
            self.record_approval = record_approval

        strict_ownership = settings.get_security_strict_ownership()
        if strict_ownership != self.strict_ownership:
            clear_cache = True
            self.strict_ownership = strict_ownership

        if clear_cache:
            self.clear_cache()

    # -------------------------------------------------------------------------
    def define_table(self, migrate=True, fake_migrate=False):
        """
            Define permissions table, invoked by AuthS3.define_tables()
        """

        table_group = self.auth.settings.table_group
        if table_group is None:
            table_group = "integer" # fallback (doesn't work with requires)

        if not self.table:
            db = current.db
            db.define_table(self.tablename,
                            Field("group_id", table_group),
                            Field("controller", length=64),
                            Field("function", length=512),
                            Field("tablename", length=512),
                            Field("record", "integer"),
                            Field("oacl", "integer", default=self.ALL),
                            Field("uacl", "integer", default=self.READ),
                            # apply this ACL only to records owned
                            # by this entity
                            Field("entity", "integer"),
                            # apply this ACL to all records regardless
                            # of the realm entity
                            Field("unrestricted", "boolean",
                                  default = False
                                  ),
                            migrate = migrate,
                            fake_migrate = fake_migrate,
                            *S3MetaFields.sync_meta_fields()
                            )
            self.table = db[self.tablename]

    # -------------------------------------------------------------------------
    def create_indexes(self):
        """
            Create indexes for s3_permission table, for faster rule lookups
        """

        dbtype = current.deployment_settings.get_database_type()

        if dbtype in ("postgres", "sqlite"):
            sql = "CREATE INDEX IF NOT EXISTS %(index)s ON %(table)s (%(field)s);"
        else:
            return

        names = {"table": self.tablename}

        db = current.db
        for fname in ("controller", "function", "tablename"):
            names["field"] = fname
            names["index"] = "%(table)s_%(field)s_idx" % names
            db.executesql(sql % names)

    # -------------------------------------------------------------------------
    # Permission rule handling
    # -------------------------------------------------------------------------
    @classmethod
    def required_acl(cls, methods):

        all_methods, none = cls.METHODS, cls.NONE

        result = none
        for method in methods:
            result |= all_methods.get(method, none)
        return result

    # -------------------------------------------------------------------------
    @classmethod
    def most_permissive(cls, rules):

        result = (cls.NONE, cls.NONE)
        for rule in rules:
            result = result[0] | rule[0], result[1] | rule[1]
        return result

    # -------------------------------------------------------------------------
    @classmethod
    def most_restrictive(cls, rules):

        result = (cls.ALL, cls.ALL)
        for rule in rules:
            result = result[0] & rule[0], result[1] & rule[1]
        return result

    # -------------------------------------------------------------------------
    # ACL Management
    # -------------------------------------------------------------------------
    def update_acl(self, group,
                   c = None,
                   f = None,
                   t = None,
                   record = None,
                   oacl = None,
                   uacl = None,
                   entity = None,
                   delete = False
                   ):
        """
            Update an ACL

            Args:
                group: the ID or UID of the auth_group this ACL applies to
                c: the controller
                f: the function
                t: the tablename
                record: the record (as ID or Row with ID)
                oacl: the ACL for the owners of the specified record(s)
                uacl: the ACL for all other users
                entity: restrict this ACL to the records owned by this
                        entity (pe_id), specify "any" for any entity
                delete: delete the ACL instead of updating it
        """

        ANY = "any"

        unrestricted = entity == ANY
        if unrestricted:
            entity = None

        table = self.table
        if not table:
            # ACLs not relevant to this security policy
            return None

        s3 = current.response.s3
        if "restricted_tables" in s3:
            del s3["restricted_tables"]
        self.clear_cache()

        if c is None and f is None and t is None:
            return None
        if t is not None:
            c = f = None
        else:
            record = None

        if uacl is None:
            uacl = self.NONE
        if oacl is None:
            oacl = uacl

        success = False
        if group:
            group_id = None
            acl = {"group_id": group_id,
                   "deleted": False,
                   "controller": c,
                   "function": f,
                   "tablename": t,
                   "record": record,
                   "oacl": oacl,
                   "uacl": uacl,
                   "unrestricted": unrestricted,
                   "entity": entity,
                   }

            if isinstance(group, str) and not group.isdigit():
                gtable = self.auth.settings.table_group
                query = (gtable.uuid == group) & \
                        (table.group_id == gtable.id)
            else:
                query = (table.group_id == group)
                group_id = group

            query &= ((table.controller == c) & \
                      (table.function == f) & \
                      (table.tablename == t) & \
                      (table.record == record) & \
                      (table.unrestricted == unrestricted) & \
                      (table.entity == entity))
            record = current.db(query).select(table.id,
                                              table.group_id,
                                              limitby = (0, 1)
                                              ).first()
            if record:
                if delete:
                    acl = {"group_id": None,
                           "deleted": True,
                           "deleted_fk": '{"group_id": %d}' % record.group_id
                           }
                else:
                    acl["group_id"] = record.group_id
                record.update_record(**acl)
                success = record.id
            elif group_id:
                acl["group_id"] = group_id
                success = table.insert(**acl)
            else:
                # Lookup the group_id
                record = current.db(gtable.uuid == group).select(gtable.id,
                                                                 limitby = (0, 1)
                                                                 ).first()
                if record:
                    acl["group_id"] = group_id
                    success = table.insert(**acl)

        return success

    # -------------------------------------------------------------------------
    def delete_acl(self, group,
                   c = None,
                   f = None,
                   t = None,
                   record = None,
                   entity = None
                   ):
        """
            Delete an ACL

            Args:
                group: the ID or UID of the auth_group this ACL applies to
                c: the controller
                f: the function
                t: the tablename
                record: the record (as ID or Row with ID)
                entity: restrict this ACL to the records owned by this
                        entity (pe_id), specify "any" for any entity
        """

        return self.update_acl(group,
                               c = c,
                               f = f,
                               t = t,
                               record = record,
                               entity = entity,
                               delete = True
                               )

    # -------------------------------------------------------------------------
    # Record Ownership
    # -------------------------------------------------------------------------
    @staticmethod
    def get_owners(table, record):
        """
            Get the entity/group/user owning a record

            Args:
                table: the table
                record: the record ID (or the Row, if already loaded)

            Returns:
                tuple of (realm_entity, owner_group, owner_user)

            Note:
                If passing a Row, it must contain all available ownership
                fields (id, owned_by_user, owned_by_group, realm_entity),
                otherwise the record will be re-loaded by this function.
        """

        realm_entity = None
        owner_group = None
        owner_user = None

        record_id = None

        DEFAULT = (None, None, None)

        # Load the table, if necessary
        if table and not hasattr(table, "_tablename"):
            table = current.s3db.table(table)
        if not table:
            return DEFAULT

        # Check which ownership fields the table defines
        ownership_fields = ("realm_entity",
                            "owned_by_group",
                            "owned_by_user")
        fields = [f for f in ownership_fields if f in table.fields]
        if not fields:
            # Ownership is not defined for this table
            return DEFAULT

        if isinstance(record, Row):
            # Check if all necessary fields are present
            missing = [f for f in fields if f not in record]
            if missing:
                # Have to reload the record :(
                if table._id.name in record:
                    record_id = record[table._id.name]
                record = None
        else:
            # Record ID given, must load the record anyway
            record_id = record
            record = None

        if not record and record_id:
            # Get the record
            fs = [table[f] for f in fields] + [table.id]
            query = (table._id == record_id)
            record = current.db(query).select(limitby = (0, 1),
                                              *fs
                                              ).first()
        if not record:
            # Record does not exist
            return DEFAULT

        if "realm_entity" in record:
            realm_entity = record["realm_entity"]
        if "owned_by_group" in record:
            owner_group = record["owned_by_group"]
        if "owned_by_user" in record:
            owner_user = record["owned_by_user"]
        return (realm_entity, owner_group, owner_user)

    # -------------------------------------------------------------------------
    def is_owner(self, table, record, owners=None, strict=False):
        """
            Check whether the current user owns the record

            Args:
                table: the table or tablename
                record: the record ID (or the Row if already loaded)
                owners: override the actual record owners by a tuple
                        (realm_entity, owner_group, owner_user)

            Returns:
                True if the current user owns the record, else False
        """

        auth = self.auth
        user_id = None
        sr = auth.get_system_roles()

        if auth.user is not None:
            user_id = auth.user.id

        session = current.session
        roles = [sr.ANONYMOUS]
        if session.s3 is not None:
            roles = session.s3.roles or roles

        if sr.ADMIN in roles:
            # Admin owns all records
            return True
        elif owners is not None:
            realm_entity, owner_group, owner_user = owners
        elif record:
            realm_entity, owner_group, owner_user = \
                    self.get_owners(table, record)
        else:
            # All users own no records
            return True

        # Session ownership?
        if not user_id:
            if isinstance(record, (Row, dict)):
                record_id = record[table._id.name]
            else:
                record_id = record
            return auth.s3_session_owns(table, record_id)

        # Individual record ownership
        if owner_user and owner_user == user_id:
            return True

        # Public record?
        if not any((realm_entity, owner_group, owner_user)) and not strict:
            return True
        elif strict:
            return False

        # OrgAuth: apply only group memberships within the realm
        if self.entity_realm and realm_entity:
            realms = auth.user.realms
            roles = [sr.ANONYMOUS]
            append = roles.append
            for r in realms:
                realm = realms[r]
                if realm is None or realm_entity in realm:
                    append(r)

        # Ownership based on user role
        return bool(owner_group and owner_group in roles)

    # -------------------------------------------------------------------------
    def owner_query(self,
                    table,
                    user,
                    use_realm = True,
                    realm = None,
                    no_realm = None
                    ):
        """
            Returns a query to select the records in table owned by user

            Args:
                table: the table
                user: the current auth.user (None for not authenticated)
                use_realm: use realms
                realm: limit owner access to these realms
                no_realm: don't include these entities in role realms

            Returns:
                a web2py Query instance, or None if no query can be constructed
        """

        OUSR = "owned_by_user"
        OGRP = "owned_by_group"
        OENT = "realm_entity"

        if realm is None:
            realm = []

        no_realm = set() if no_realm is None else set(no_realm)

        query = None
        if user is None:
            # Session ownership?
            if hasattr(table, "_tablename"):
                tablename = original_tablename(table)
            else:
                tablename = table
            session = current.session
            if "owned_records" in session and \
               tablename in session.owned_records:
                query = (table._id.belongs(session.owned_records[tablename]))
        else:
            use_realm = use_realm and \
                        OENT in table.fields and self.entity_realm

            # Individual owner query
            if OUSR in table.fields:
                user_id = user.id
                query = (table[OUSR] == user_id)
                if use_realm:
                    # Limit owner access to permitted realms
                    if realm:
                        realm_query = self.realm_query(table, realm)
                        if realm_query:
                            query &= realm_query
                    else:
                        query = None

            if not self.strict_ownership:
                # Any authenticated user owns all records with no owner
                public = None
                if OUSR in table.fields:
                    public = (table[OUSR] == None)
                if OGRP in table.fields:
                    q = (table[OGRP] == None)
                    if public:
                        public &= q
                    else:
                        public = q
                if use_realm:
                    q = (table[OENT] == None)
                    if public:
                        public &= q
                    else:
                        public = q

                if public is not None:
                    if query is not None:
                        query |= public
                    else:
                        query = public

            # Group ownerships
            if OGRP in table.fields:
                any_entity = set()
                g = None
                user_realms = user.realms
                for group_id in user_realms:

                    role_realm = user_realms[group_id]

                    if role_realm is None or not use_realm:
                        any_entity.add(group_id)
                        continue

                    role_realm = set(role_realm) - no_realm

                    if role_realm:
                        q = (table[OGRP] == group_id) & (table[OENT].belongs(role_realm))
                        if g is None:
                            g = q
                        else:
                            g |= q
                if any_entity:
                    q = (table[OGRP].belongs(any_entity))
                    if g is None:
                        g = q
                    else:
                        g |= q
                if g is not None:
                    if query is None:
                        query = g
                    else:
                        query |= g

        return query

    # -------------------------------------------------------------------------
    @staticmethod
    def realm_query(table, entities):
        """
            Returns a query to select the records owned by one of the entities.

            Args:
                table: the table
                entities: list of entities

            Returns:
                a web2py Query instance, or None if no query can be constructed
        """

        OENT = "realm_entity"

        query = None

        if entities and "ANY" not in entities and OENT in table.fields:
            public = (table[OENT] == None)
            if len(entities) == 1:
                query = (table[OENT] == entities[0]) | public
            else:
                query = (table[OENT].belongs(entities)) | public

        return query

    # -------------------------------------------------------------------------
    def permitted_realms(self, tablename, method="read", c=None, f=None):
        """
            Returns a list of the realm entities which a user can access for
            the given table.

            Args:
                tablename: the tablename
                method: the method
                c: override request.controller to look up for
                   a different controller context
                f: override request.function to look up for
                   a different controller context

            Returns:
                a list of pe_ids or None (for no restriction)
        """

        if not self.entity_realm:
            # Security Policy doesn't use Realms, so unrestricted
            return None

        auth = self.auth
        sr = auth.get_system_roles()
        user = auth.user
        if auth.is_logged_in():
            realms = user.realms
            if sr.ADMIN in realms:
                # ADMIN can see all Realms
                return None
        else:
            realms = Storage({sr.ANONYMOUS:None})

        racl = self.required_acl([method])
        request = current.request
        acls = self.applicable_acls(racl,
                                    realms = realms,
                                    c = c if c else request.controller,
                                    f = f if f else request.function,
                                    t = tablename,
                                    )
        if "ANY" in acls:
            # User is permitted access for all Realms
            return None

        entities = []
        for entity in acls:
            acl = acls[entity]
            if acl[0] & racl == racl:
                entities.append(entity)

        return entities

    # -------------------------------------------------------------------------
    # Record approval
    # -------------------------------------------------------------------------
    def approved(self, table, record, approved=True):
        """
            Check whether a record has been approved or not

            Args:
                table: the table
                record: the record or record ID
                approved: True = check if approved,
                          False = check if unapproved

            Returns:
                boolean result of the check
        """

        if "approved_by" not in table.fields or \
           not self.requires_approval(table):
            return approved

        if isinstance(record, (Row, dict)):
            if "approved_by" not in record:
                record_id = record[table._id]
                record = None
        else:
            record_id = record
            record = None

        if record is None and record_id:
            record = current.db(table._id == record_id).select(table.approved_by,
                                                               limitby = (0, 1)
                                                               ).first()
            if not record:
                return False

        if approved and record["approved_by"] is not None:
            return True
        elif not approved and record["approved_by"] is None:
            return True
        else:
            return False

    # -------------------------------------------------------------------------
    def unapproved(self, table, record):
        """
            Check whether a record has not been approved yet

            Args:
                table: the table
                record: the record or record ID
        """

        return self.approved(table, record, approved=False)

    # -------------------------------------------------------------------------
    @classmethod
    def requires_approval(cls, table):
        """
            Check whether record approval is required for a table

            Args:
                table: the table (or tablename)
        """

        settings = current.deployment_settings

        if settings.get_auth_record_approval():

            if type(table) is Table:
                tablename = original_tablename(table)
            else:
                tablename = table

            tables = settings.get_auth_record_approval_required_for()
            if tables is not None:
                return tablename in tables

            elif current.s3db.get_config(tablename, "requires_approval"):
                return True

            else:
                return False
        else:
            return False

    # -------------------------------------------------------------------------
    @classmethod
    def set_default_approver(cls, table, force=False):
        """
            Set the default approver for new records in table

            Args:
                table: the table
                force: whether to force approval for tables which
                       require manual approval
        """

        APPROVER = "approved_by"
        if APPROVER in table:
            approver = table[APPROVER]
        else:
            return

        settings = current.deployment_settings
        auth = current.auth

        tablename = original_tablename(table)

        if not settings.get_auth_record_approval():
            if auth.s3_logged_in() and auth.user:
                approver.default = auth.user.id
            else:
                approver.default = 0
        elif force or \
             tablename not in settings.get_auth_record_approval_manual():
            if auth.override:
                approver.default = 0
            elif auth.s3_logged_in() and \
                 auth.s3_has_permission("approve", table):
                approver.default = auth.user.id
            else:
                approver.default = None

    # -------------------------------------------------------------------------
    # Authorization
    # -------------------------------------------------------------------------
    def has_permission(self, method, c=None, f=None, t=None, record=None):
        """
            Check permission to access a record with method

            Args:
                method: the access method (string)
                c: the controller name (falls back to current request)
                f: the function name (falls back to current request)
                t: the table or tablename
                record: the record or record ID (None for any record)
        """

        # Auth override, system roles and login
        auth = self.auth
        if auth.override:
            #_debug("==> auth.override")
            #_debug("*** GRANTED ***")
            return True

        # Multiple methods?
        if isinstance(method, (list, tuple)):
            for m in method:
                if self.has_permission(m, c=c, f=f, t=t, record=record):
                    return True
            return False
        else:
            method = [method]

        if record == 0:
            record = None

        #_debug("\nhas_permission('%s', c=%s, f=%s, t=%s, record=%s)",
        #       "|".join(method),
        #       c or current.request.controller,
        #       f or current.request.function,
        #       t,
        #       record,
        #       )

        sr = auth.get_system_roles()
        logged_in = auth.s3_logged_in()
        self.check_settings()

        # Required ACL
        racl = self.required_acl(method)
        #_debug("==> required ACL: %04X", racl)

        # Get realms and delegations
        if not logged_in:
            realms = Storage({sr.ANONYMOUS:None})
        else:
            realms = auth.user.realms

        # Administrators have all permissions
        if sr.ADMIN in realms:
            #_debug("==> user is ADMIN")
            #_debug("*** GRANTED ***")
            return True

        # Fall back to current request
        c = c or self.controller
        f = f or self.function

        if not self.use_cacls:
            #_debug("==> simple authorization")
            # Fall back to simple authorization
            if logged_in:
                #_debug("*** GRANTED ***")
                return True
            else:
                if self.page_restricted(c=c, f=f):
                    permitted = racl == self.READ
                else:
                    #_debug("==> unrestricted page")
                    permitted = True
                #if permitted:
                #    _debug("*** GRANTED ***")
                #else:
                #    _debug("*** DENIED ***")
                return permitted

        # Do we need to check the owner role (i.e. table+record given)?
        if t is not None and record is not None:
            owners = self.get_owners(t, record)
            is_owner = self.is_owner(t, record, owners=owners, strict=self.strict_ownership)
            entity = owners[0]
        else:
            owners = []
            is_owner = True
            entity = None

        permission_cache = self.permission_cache
        if permission_cache is None:
            permission_cache = self.permission_cache = {}
        key = "%s/%s/%s/%s/%s" % (method, c, f, t, record)
        if key in permission_cache:
            #permitted = permission_cache[key]
            #if permitted is None:
            #    pass
            #elif permitted:
            #    _debug("*** GRANTED (cached) ***")
            #else:
            #    _debug("*** DENIED (cached) ***")
            return permission_cache[key]

        # Get the applicable ACLs
        acls = self.applicable_acls(racl,
                                    realms = realms,
                                    c = c,
                                    f = f,
                                    t = t,
                                    entity = entity
                                    )

        permitted = None
        if acls is None:
            #_debug("==> no ACLs defined for this case")
            permitted = True
        elif not acls:
            #_debug("==> no applicable ACLs")
            permitted = False
        else:
            if entity:
                if entity in acls:
                    uacl, oacl = acls[entity]
                elif "ANY" in acls:
                    uacl, oacl = acls["ANY"]
                else:
                    #_debug("==> Owner entity outside realm")
                    permitted = False
            else:
                uacl, oacl = self.most_permissive(acls.values())

            #_debug("==> uacl: %04X, oacl: %04X", uacl, oacl)

            if permitted is None:
                if uacl & racl == racl:
                    permitted = True
                elif oacl & racl == racl:
                    #if is_owner and record:
                    #    _debug("==> User owns the record")
                    #elif record:
                    #    _debug("==> User does not own the record")
                    permitted = is_owner
                else:
                    permitted = False

        if permitted is None:
            raise self.error("Cannot determine permission.")

        elif permitted and \
             t is not None and record is not None and \
             self.requires_approval(t):

            # Approval possible for this table?
            if not hasattr(t, "_tablename"):
                table = current.s3db.table(t)
                if not table:
                    raise AttributeError("undefined table %s" % t)
            else:
                table = t
            if "approved_by" in table.fields:

                approval_methods = ("approve", "review", "reject")
                access_approved = not all([m in approval_methods for m in method])
                access_unapproved = any([m in method for m in approval_methods])

                if access_unapproved:
                    if not access_approved:
                        permitted = self.unapproved(table, record)
                        #if not permitted:
                        #    _debug("==> Record already approved")
                else:
                    permitted = self.approved(table, record) or \
                                self.is_owner(table, record, owners, strict=True) or \
                                self.has_permission("review", t=table, record=record)
                    #if not permitted:
                    #    _debug("==> Record not approved")
                    #    _debug("==> is owner: %s", is_owner)
            else:
                # Approval not possible for this table => no change
                pass

        #if permitted:
        #    _debug("*** GRANTED ***")
        #else:
        #    _debug("*** DENIED ***")

        # Remember the result for subsequent checks
        permission_cache[key] = permitted

        return permitted

    # -------------------------------------------------------------------------
    def accessible_query(self, method, table, c=None, f=None, deny=True):
        """
            Returns a query to select the accessible records for method
            in table.

            Args:
                method: the method as string or a list of methods (AND)
                table: the database table or table name
                c: controller name (falls back to current request)
                f: function name (falls back to current request)
        """

        # Get the table
        if not hasattr(table, "_tablename"):
            tablename = table
            error = AttributeError("undefined table %s" % tablename)
            table = current.s3db.table(tablename,
                                       db_only = True,
                                       default = error,
                                       )

        if not isinstance(method, (list, tuple)):
            method = [method]

        #_debug("\naccessible_query(%s, '%s')", table, ",".join(method))

        # Defaults
        ALL_RECORDS = (table._id > 0)
        NO_RECORDS = (table._id == 0) if deny else None

        # Record approval required?
        if self.requires_approval(table) and \
           "approved_by" in table.fields:
            requires_approval = True
            APPROVED = (table.approved_by != None)
            UNAPPROVED = (table.approved_by == None)
        else:
            requires_approval = False
            APPROVED = ALL_RECORDS
            UNAPPROVED = NO_RECORDS

        # Approval method?
        approval_methods = ("review", "approve", "reject")
        unapproved = any([m in method for m in approval_methods])
        approved = not all([m in approval_methods for m in method])

        # What does ALL RECORDS mean?
        ALL_RECORDS = ALL_RECORDS if approved and unapproved \
                                  else UNAPPROVED if unapproved \
                                  else APPROVED

        # Auth override, system roles and login
        auth = self.auth
        if auth.override:
            #_debug("==> auth.override")
            #_debug("*** ALL RECORDS ***")
            return ALL_RECORDS

        sr = auth.get_system_roles()
        logged_in = auth.s3_logged_in()
        self.check_settings()

        # Get realms and delegations
        user = auth.user
        if not logged_in:
            realms = Storage({sr.ANONYMOUS:None})
        else:
            realms = user.realms

        # Don't filter out unapproved records owned by the user
        if requires_approval and not unapproved and \
           "owned_by_user" in table.fields:
            ALL_RECORDS = (table.approved_by != None)
            if user:
                owner_query = (table.owned_by_user == user.id)
            else:
                owner_query = self.owner_query(table, None)
            if owner_query is not None:
                ALL_RECORDS |= owner_query

        # Administrators have all permissions
        if sr.ADMIN in realms:
            #_debug("==> user is ADMIN")
            #_debug("*** ALL RECORDS ***")
            return ALL_RECORDS

        # Multiple methods?
        if len(method) > 1:
            query = None
            for m in method:
                q = self.accessible_query(m, table, c=c, f=f, deny=False)
                if q is not None:
                    if query is None:
                        query = q
                    else:
                        query |= q
            if query is None:
                query = NO_RECORDS
            return query

        key = "%s/%s/%s/%s/%s" % (method, table, c, f, deny)
        query_cache = self.query_cache
        if key in query_cache:
            query = query_cache[key]
            return query

        # Required ACL
        racl = self.required_acl(method)
        #_debug("==> required permissions: %04X", racl)

        # Use ACLs?
        if not self.use_cacls:
            #_debug("==> simple authorization")
            # Fall back to simple authorization
            if logged_in:
                #_debug("*** ALL RECORDS ***")
                return ALL_RECORDS
            else:
                permitted = racl == self.READ
                if permitted:
                    #_debug("*** ALL RECORDS ***")
                    return ALL_RECORDS
                else:
                    #_debug("*** ACCESS DENIED ***")
                    return NO_RECORDS

        # Fall back to current request
        c = c or self.controller
        f = f or self.function

        # Get the applicable ACLs
        acls = self.applicable_acls(racl,
                                    realms = realms,
                                    c = c,
                                    f = f,
                                    t = table
                                    )

        if acls is None:
            #_debug("==> no ACLs defined for this case")
            #_debug("*** ALL RECORDS ***")
            query = query_cache[key] = ALL_RECORDS
            return query
        elif not acls:
            #_debug("==> no applicable ACLs")
            #_debug("*** ACCESS DENIED ***")
            query = query_cache[key] = NO_RECORDS
            return query

        oacls = []
        uacls = []
        for entity in acls:
            acl = acls[entity]
            if acl[0] & racl == racl:
                uacls.append(entity)
            elif acl[1] & racl == racl and entity not in uacls:
                oacls.append(entity)

        query = None
        no_realm = []
        check_owner_acls = True

        if "ANY" in uacls:
            #_debug("==> permitted for any records")
            query = ALL_RECORDS
            check_owner_acls = False

        elif uacls:
            query = self.realm_query(table, uacls)
            if query is None:
                #_debug("==> permitted for any records")
                query = ALL_RECORDS
                check_owner_acls = False
            else:
                #_debug("==> permitted for records owned by entities %s", str(uacls))
                no_realm = uacls

        if check_owner_acls:

            use_realm = "ANY" not in oacls
            owner_query = self.owner_query(table,
                                           user,
                                           use_realm = use_realm,
                                           realm = oacls,
                                           no_realm = no_realm,
                                           )

            if owner_query is not None:
                #_debug("==> permitted for owned records (limit to realms=%s)", use_realm)
                if query is not None:
                    query |= owner_query
                else:
                    query = owner_query
            elif use_realm:
                #_debug("==> permitted for any records owned by entities %s", str(uacls+oacls))
                query = self.realm_query(table, uacls+oacls)

            if query is not None and requires_approval:
                base_filter = None if approved and unapproved else \
                              UNAPPROVED if unapproved else APPROVED
                if base_filter is not None:
                    query = base_filter & query

        # Fallback
        if query is None:
            query = NO_RECORDS

        #_debug("*** Accessible Query ***")
        #_debug(str(query))
        query_cache[key] = query
        return query

    # -------------------------------------------------------------------------
    def accessible_url(self,
                       c = None,
                       f = None,
                       p = None,
                       t = None,
                       a = None,
                       args = None,
                       vars = None,
                       anchor = "",
                       extension = None,
                       env = None
                       ):
        """
            Return a URL only if accessible by the user, otherwise False
                - used for Navigation Items

            Args:
                c: the controller
                f: the function
                p: the permission (defaults to READ)
                t: the tablename (defaults to <c>_<f>)
                a: the application name
                args: the URL arguments
                vars: the URL variables
                anchor: the anchor (#) of the URL
                extension: the request format extension
                env: the environment
        """

        if args is None:
            args = []
        if vars is None:
            vars = {}

        if c != "static":
            # Hide disabled modules
            settings = current.deployment_settings
            if not settings.has_module(c):
                return False

        if t is None:
            t = "%s_%s" % (c, f)
            table = current.s3db.table(t)
            if not table:
                t = None
        if not p:
            p = "read"

        permitted = self.has_permission(p, c=c, f=f, t=t)
        if permitted:
            return URL(a = a,
                       c = c,
                       f = f,
                       args = args,
                       vars = vars,
                       anchor = anchor,
                       extension = extension,
                       env = env
                       )
        else:
            return False

    # -------------------------------------------------------------------------
    def fail(self):
        """ Action upon insufficient permissions """

        if self.format == "html":
            # HTML interactive request => flash message + redirect
            if self.auth.s3_logged_in():
                current.session.error = self.INSUFFICIENT_PRIVILEGES
                redirect(self.homepage)
            else:
                current.session.error = self.AUTHENTICATION_REQUIRED
                redirect(self.loginpage)
        else:
            # Non-HTML request => raise HTTP status
            if self.auth.s3_logged_in():
                raise HTTP(403, body=self.INSUFFICIENT_PRIVILEGES)

            # RFC1945/2617 compliance:
            # Must raise an HTTP Auth challenge with status 401
            headers = {"WWW-Authenticate":
                       "Basic realm=\"%s\"" % current.request.application,
                       }

            # Add Master Key Auth token if enabled + requested
            if current.deployment_settings.get_auth_masterkey():
                from .masterkey import S3MasterKey
                S3MasterKey.challenge(headers)

            raise HTTP(401, body=self.AUTHENTICATION_REQUIRED, **headers)

    # -------------------------------------------------------------------------
    # ACL Lookup
    # -------------------------------------------------------------------------
    def applicable_acls(self, racl,
                        realms = None,
                        c = None,
                        f = None,
                        t = None,
                        entity = None
                        ):
        """
            Find all applicable ACLs for the specified situation for
            the specified realms and delegations

            Args:
                racl: the required ACL
                realms: the realms
                delegations: the delegations
                c: the controller name, falls back to current request
                f: the function name, falls back to current request
                t: the tablename
                entity: the realm entity

            Returns:
                - None for no ACLs defined (allow), or
                - [] for no ACLs applicable (deny), or
                - list of applicable ACLs
        """

        if not self.use_cacls:
            # We do not use ACLs at all (allow all)
            return None
        else:
            acls = {}

        # Get all roles
        if realms:
            roles = set(realms.keys())
        else:
            # No roles available (deny all)
            return acls

        db = current.db
        table = self.table

        c = c or self.controller
        f = f or self.function
        page_restricted = self.page_restricted(c=c, f=f)

        # Base query
        query = (table.group_id.belongs(roles)) & \
                (table.deleted == False)

        # Page ACLs
        if page_restricted:
            q = (table.function == None)
            if f and self.use_facls:
                q |= (table.function == f)
            q = (table.controller == c) & q
        else:
            q = None

        # Table ACLs
        if t and self.use_tacls:
            # Be sure to use the original table name
            if hasattr(t, "_tablename"):
                t = original_tablename(t)
            tq = (table.tablename == t) & \
                 (table.controller == None) & \
                 (table.function == None)
            q = tq if q is None else q | tq
            table_restricted = self.table_restricted(t)
        else:
            table_restricted = False

        # Retrieve the ACLs
        if q is not None:
            query = q & query
            rows = db(query).select(table.group_id,
                                    table.controller,
                                    table.function,
                                    table.tablename,
                                    table.unrestricted,
                                    table.entity,
                                    table.uacl,
                                    table.oacl,
                                    cacheable = True,
                                    )
        else:
            rows = []

        # Cascade ACLs
        ANY = "ANY"

        ALL = (self.ALL, self.ALL)
        NONE = (self.NONE, self.NONE)

        use_facls = self.use_facls
        def rule_type(r):
            if r.controller is not None:
                if r.function is None:
                    return "c"
                elif use_facls:
                    return "f"
            elif r.tablename is not None:
                return "t"
            return None

        most_permissive = lambda x, y: (x[0] | y[0], x[1] | y[1])
        most_restrictive = lambda x, y: (x[0] & y[0], x[1] & y[1])

        # Realms
        use_realms = self.entity_realm
        for row in rows:

            # Get the assigning entities
            group_id = row.group_id
            if group_id not in realms:
                continue
            rtype = rule_type(row)
            if rtype is None:
                continue

            if use_realms:
                if row.unrestricted:
                    entities = [ANY]
                elif row.entity is not None:
                    entities = [row.entity]
                else:
                    entities = realms[group_id]
                if entities is None:
                    entities = [ANY]
            else:
                entities = [ANY]

            # Merge the ACL
            acl = (row["uacl"], row["oacl"])
            for e in entities:
                if e in acls:
                    eacls = acls[e]
                    if rtype in eacls:
                        eacls[rtype] = most_permissive(eacls[rtype], acl)
                    else:
                        eacls[rtype] = acl
                else:
                    acls[e] = {rtype: acl}

        acl = acls.get(ANY, {})

        # Default page ACL
        if "c" in acl:
            default_page_acl = acl["f"] if "f" in acl else acl["c"]
        elif page_restricted:
            default_page_acl = NONE
        else:
            default_page_acl = ALL

        # Default table ACL
        if "t" in acl:
            # If we have a table rule, apply it
            default_table_acl = acl["t"]
        elif self.use_tacls and table_restricted:
            # A restricted table is not accessible on any page without an
            # explicit table rule (once explicit => always explicit!)
            default_table_acl = NONE
        else:
            # An unrestricted table is accessible under the page rule
            default_table_acl = default_page_acl if page_restricted else ALL

        # No ACLs inevitably causes a "no applicable ACLs" permission failure,
        # so for unrestricted pages or tables, we must create a default ACL
        # here in order to have the default apply:
        if not acls:
            if t and self.use_tacls:
                if not table_restricted:
                    acls[ANY] = {"t": default_table_acl}
            elif not page_restricted:
                acls[ANY] = {"c": default_page_acl}

        # Order by precedence
        s3db = current.s3db
        ancestors = set()
        if entity and self.entity_hierarchy and \
           s3db.pr_instance_type(entity) == "pr_person":
            # If the realm entity is a person, then we apply the ACLs
            # for the immediate OU ancestors, for two reasons:
            # a) it is not possible to assign roles for personal realms anyway
            # b) looking up OU ancestors of a person (=a few) is much more
            #    efficient than looking up pr_person OU descendants of the
            #    role realm (=could be tens or hundreds of thousands)
            ancestors = set(s3db.pr_default_realms(entity))

        result = {}
        for e in acls:
            # Skip irrelevant ACLs
            if entity and e != entity and e != ANY:
                if e in ancestors:
                    key = entity
                else:
                    continue
            else:
                key = e

            acl = acls[e]

            # Get the page ACL
            if "f" in acl:
                page_acl = most_permissive(default_page_acl, acl["f"])
            elif "c" in acl:
                page_acl = most_permissive(default_page_acl, acl["c"])
            elif page_restricted:
                page_acl = default_page_acl
            else:
                page_acl = ALL

            # Get the table ACL
            if "t" in acl:
                table_acl = most_permissive(default_table_acl, acl["t"])
            elif table_restricted:
                table_acl = default_table_acl
            else:
                table_acl = ALL

            # Merge
            acl = most_restrictive(page_acl, table_acl)

            # Include ACL if relevant
            if acl[0] & racl == racl or acl[1] & racl == racl:
                result[key] = acl

        #for pe in result:
        #    import sys
        #    sys.stderr.write("ACL for PE %s: %04X %04X\n" %
        #                        (pe, result[pe][0], result[pe][1]))

        return result

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------
    def page_restricted(self, c=None, f=None):
        """
            Checks whether a page is restricted (=whether ACLs
            are to be applied)

            Args:
                c: controller name
                f: function name
        """


        page = "%s/%s" % (c, f)
        if page in self.unrestricted_pages:
            restricted = False
        elif c != "default" or f not in ("tables", "table"):
            modules = current.deployment_settings.modules
            restricted = c in modules and modules[c].get("restricted", True)
        else:
            restricted = True

        return restricted

    # -------------------------------------------------------------------------
    def table_restricted(self, t=None):
        """
            Check whether access to a table is restricted

            Args:
                t: the table name or Table
        """

        s3 = current.response.s3

        if not "restricted_tables" in s3:
            table = self.table
            query = (table.controller == None) & \
                    (table.function == None) & \
                    (table.deleted == False)
            rows = current.db(query).select(table.tablename,
                                            groupby = table.tablename,
                                            )
            s3.restricted_tables = [row.tablename for row in rows]

        return str(t) in s3.restricted_tables

    # -------------------------------------------------------------------------
    def hidden_modules(self):
        """ List of modules to hide from the main menu """

        hidden_modules = []
        if self.use_cacls:
            sr = self.auth.get_system_roles()
            modules = current.deployment_settings.modules
            restricted_modules = [m for m in modules
                                    if modules[m].get("restricted", True)]
            roles = []
            if current.session.s3 is not None:
                roles = current.session.s3.roles or []
            if sr.ADMIN in roles:   # or sr.EDITOR in roles:
                return []
            if not roles:
                hidden_modules = restricted_modules
            else:
                t = self.table
                query = (t.deleted == False) & \
                        (t.controller.belongs(restricted_modules)) & \
                        (t.tablename == None)
                if roles:
                    query = query & (t.group_id.belongs(roles))
                else:
                    query = query & (t.group_id == None)
                rows = current.db(query).select()
                acls = {}
                for acl in rows:
                    if acl.controller not in acls:
                        acls[acl.controller] = self.NONE
                    acls[acl.controller] |= acl.oacl | acl.uacl
                hidden_modules = [m for m in restricted_modules
                                    if m not in acls or not acls[m]]
        return hidden_modules

    # -------------------------------------------------------------------------
    def ownership_required(self, method, table, c=None, f=None):
        """
            Checks whether ownership can be required to access records in
            this table (this may not apply to every record in this table).

            Args:
                method: the method as string or a list of methods (AND)
                table: the database table or table name
                c: controller name (falls back to current request)
                f: function name (falls back to current request)
        """

        if not self.use_cacls:
            if self.policy in (1, 2):
                return False
            else:
                return True

        if not hasattr(table, "_tablename"):
            tablename = table
            table = current.s3db.table(tablename)
            if not table:
                raise AttributeError("undefined table %s" % tablename)

        # If the table doesn't have any ownership fields, then no
        if "owned_by_user" not in table.fields and \
           "owned_by_group" not in table.fields and \
           "realm_entity" not in table.fields:
            return False

        if not isinstance(method, (list, tuple)):
            method = [method]

        # Auth override, system roles and login
        auth = self.auth
        if self.auth.override or not self.use_cacls:
            return False
        sr = auth.get_system_roles()
        logged_in = auth.s3_logged_in()

        # Required ACL
        racl = self.required_acl(method)

        # Get realms and delegations
        user = auth.user
        if not logged_in:
            realms = Storage({sr.ANONYMOUS: None})
        else:
            realms = user.realms

        # Admin always owns all records
        if sr.ADMIN in realms:
            return False

        # Fall back to current request
        c = c or self.controller
        f = f or self.function

        # Get the applicable ACLs
        acls = self.applicable_acls(racl,
                                    realms = realms,
                                    c = c,
                                    f = f,
                                    t = table)
        acls = [entity for entity in acls if acls[entity][0] & racl == racl]

        # If we have a UACL and it is not limited to any realm, then no
        if "ANY" in acls or acls and "realm_entity" not in table.fields:
            return False

        # In all other cases: yes
        return True

    # -------------------------------------------------------------------------
    def forget(self, table=None, record_id=None):
        """
            Remove any cached permissions for a record. This can be
            necessary in methods which change the status of the record
            (e.g. approval).

            Args:
                table: the table
                record_id: the record ID
        """

        if table is None:
            self.permission_cache = {}
            return
        permissions = self.permission_cache
        if not permissions:
            return

        if hasattr(table, "_tablename"):
            tablename = original_tablename(table)
        else:
            tablename = table

        for key in list(permissions.keys()):
            r = key.split("/")
            if len(r) > 1 and r[-2] == tablename:
                if record_id is None or \
                   record_id is not None and r[-1] == str(record_id):
                    del permissions[key]

# END =========================================================================
