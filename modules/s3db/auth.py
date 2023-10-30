"""
    Auth Model

    Copyright: 2009-2022 (c) Sahana Software Foundation

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

__all__ = ("AuthDomainApproverModel",
           "AuthUserOptionsModel",
           "AuthConsentModel",
           "AuthMasterKeyModel",
           "AuthUserTempModel",
           "auth_user_options_get_osm",
           "auth_UserRepresent",
           )

from gluon import *
from gluon.storage import Storage

from ..core import *
from ..s3dal import original_tablename
from ..s3layouts import S3PopupLink

# =============================================================================
class AuthDomainApproverModel(DataModel):

    names = ("auth_organisation",)

    def model(self):

        T = current.T

        # ---------------------------------------------------------------------
        # Domain table:
        # When users register their email address is checked against this list.
        #   - If the Domain matches, then they are automatically assigned to the
        #     Organization.
        #   - If there is no Approvals email then the user is automatically approved.
        #   - If there is an Approvals email then the approval request goes to this
        #     address
        #   - If a user registers for an Organization & the domain doesn't match (or
        #     isn't listed) then the approver gets the request
        #
        if current.deployment_settings.get_auth_registration_requests_organisation():
            ORG_HELP = T("If this field is populated then a user who specifies this Organization when signing up will be assigned as a Staff of this Organization unless their domain doesn't match the domain field.")
        else:
            ORG_HELP = T("If this field is populated then a user with the Domain specified will automatically be assigned as a Staff of this Organization")

        DOMAIN_HELP = T("If a user verifies that they own an Email Address with this domain, the Approver field is used to determine whether & by whom further approval is required.")
        APPROVER_HELP = T("The Email Address to which approval requests are sent (normally this would be a Group mail rather than an individual). If the field is blank then requests are approved automatically if the domain matches.")

        tablename = "auth_organisation"
        self.define_table(tablename,
                          self.org_organisation_id(
                                comment=DIV(_class = "tooltip",
                                            _title = "%s|%s" % (current.messages.ORGANISATION,
                                                                ORG_HELP,
                                                                ),
                                            ),
                                ),
                          Field("domain",
                                label = T("Domain"),
                                comment=DIV(_class = "tooltip",
                                            _title = "%s|%s" % (T("Domain"),
                                                                DOMAIN_HELP,
                                                                ),
                                            ),
                                ),
                          Field("approver",
                                label = T("Approver"),
                                requires = IS_EMPTY_OR(IS_EMAIL()),
                                comment=DIV(_class = "tooltip",
                                            _title = "%s|%s" % (T("Approver"),
                                                                APPROVER_HELP,
                                                                ),
                                            ),
                                ),
                          CommentsField(),
                          )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None


# =============================================================================
class AuthUserOptionsModel(DataModel):
    """ Model to store per-user configuration options """

    names = ("auth_user_options",)

    def model(self):

        T = current.T

        # ---------------------------------------------------------------------
        # User Options
        #
        OAUTH_KEY_HELP = "%s|%s|%s" % (T("OpenStreetMap OAuth Consumer Key"),
                                       T("In order to be able to edit OpenStreetMap data from within %(name_short)s, you need to register for an account on the OpenStreetMap server.") % \
                                            {"name_short": current.deployment_settings.get_system_name_short()},
                                       T("Go to %(url)s, sign up & then register your application. You can put any URL in & you only need to select the 'modify the map' permission.") % \
                                            {"url": A("http://www.openstreetmap.org",
                                                      _href="http://www.openstreetmap.org",
                                                      _target="blank",
                                                      ),
                                             },
                                       )

        self.define_table("auth_user_options",
                          self.super_link("pe_id", "pr_pentity"),
                          Field("user_id", current.auth.settings.table_user),
                          Field("osm_oauth_consumer_key",
                                label = T("OpenStreetMap OAuth Consumer Key"),
                                comment = DIV(_class="stickytip",
                                              _title=OAUTH_KEY_HELP,
                                              ),
                                ),
                          Field("osm_oauth_consumer_secret",
                                label = T("OpenStreetMap OAuth Consumer Secret"),
                                ),
                          )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

# =============================================================================
class AuthConsentModel(DataModel):
    """
        Model to track consent, e.g. to legitimise processing of personal
        data under GDPR rules.
    """

    names = ("auth_processing_type",
             "auth_consent_option",
             "auth_consent_option_hash_fields",
             "auth_consent",
             "auth_consent_assertion",
             )

    def model(self):

        T = current.T

        db = current.db
        s3 = current.response.s3

        define_table = self.define_table
        crud_strings = s3.crud_strings

        # ---------------------------------------------------------------------
        # Processing Types
        # - types of data processing consent is required for
        #
        tablename = "auth_processing_type"
        define_table(tablename,
                     Field("code", length=16, notnull=True, unique=True,
                           label = T("Code"),
                           requires = [IS_NOT_EMPTY(),
                                       IS_LENGTH(16),
                                       IS_NOT_ONE_OF(db, "%s.code" % tablename),
                                       ],
                           comment = DIV(_class = "tooltip",
                                         _title = "%s|%s" % (T("Type Code"),
                                                             T("A unique code to identify the type"),
                                                             ),
                                         ),
                           ),
                     Field("name",
                           label = T("Description"),
                           requires = IS_NOT_EMPTY(),
                           ),
                     CommentsField(),
                     )

        # Table configuration
        self.configure(tablename,
                       deduplicate = S3Duplicate(primary = ("code",),
                                                 secondary = ("name",),
                                                 ),
                       )

        # Representation
        type_represent = S3Represent(lookup=tablename)

        # CRUD Strings
        ADD_TYPE = T("Create Processing Type")
        crud_strings[tablename] = Storage(
            label_create = ADD_TYPE,
            title_display = T("Processing Type Details"),
            title_list = T("Processing Types"),
            title_update = T("Edit Processing Type"),
            label_list_button = T("List Processing Types"),
            label_delete_button = T("Delete Processing Type"),
            msg_record_created = T("Processing Type created"),
            msg_record_modified = T("Processing Type updated"),
            msg_record_deleted = T("Processing Type deleted"),
            msg_list_empty = T("No Processing Types currently defined"),
            )

        # ---------------------------------------------------------------------
        # Consent Option
        # - a description of the data processing consent is requested for
        # - multiple consecutive versions of a description for the same
        #   type of data processing can exist, but once a user has consented
        #   to a particular version of the description, it becomes a legal
        #   document that must not be changed or deleted
        #
        tablename = "auth_consent_option"
        define_table(tablename,
                     Field("type_id", "reference auth_processing_type",
                           label = T("Processing Type"),
                           represent = type_represent,
                           requires = IS_ONE_OF(db, "auth_processing_type.id",
                                                type_represent,
                                                ),
                           ondelete = "RESTRICT",
                           comment = S3PopupLink(c = "admin",
                                                 f = "processing_type",
                                                 title = ADD_TYPE,
                                                 tooltip = T("Choose a type from the drop-down, or click the link to create a new type"),
                                                 vars = {"parent": "consent_option",
                                                         "child": "type_id",
                                                         },
                                                 ),
                           ),
                     Field("name",
                           label = T("Short Description"),
                           requires = IS_NOT_EMPTY(),
                           writable = False,
                           ),
                     Field("description", "text",
                           label = T("Explanations"),
                           represent = s3_text_represent,
                           writable = False,
                           ),
                     DateField("valid_from",
                               label = T("Valid From"),
                               default = "now",
                               ),
                     DateField("valid_until",
                               # Automatically set onaccept
                               readable = False,
                               writable = False,
                               ),
                     Field("opt_out", "boolean",
                           default = False,
                           label = T("Preselected"),
                           comment = DIV(_class = "tooltip",
                                         _title = "%s|%s" % (T("Preselected"),
                                                             T("This option is preselected in consent question (explicit opt-out)"),
                                                             ),
                                         ),
                           ),
                     Field("mandatory", "boolean",
                           default = False,
                           label = T("Mandatory"),
                           comment = DIV(_class = "tooltip",
                                         _title = "%s|%s" % (T("Mandatory"),
                                                             T("This option is required for the consent question to succeed"),
                                                             ),
                                         ),
                           ),
                     Field("validity_period", "integer",
                           default = None,
                           label = T("Consent valid for (days)"),
                           requires = IS_EMPTY_OR(IS_INT_IN_RANGE(1, None)),
                           comment = DIV(_class = "tooltip",
                                         _title = "%s|%s" % (T("Period of Validity"),
                                                             T("Consent to this option expires after this many days"),
                                                             ),
                                         ),
                           ),
                     Field("obsolete", "boolean",
                           default = False,
                           label = T("Obsolete"),
                           represent = s3_yes_no_represent,
                           comment = DIV(_class = "tooltip",
                                         _title = "%s|%s" % (T("Obsolete"),
                                                             T("This description of the data processing is obsolete"),
                                                             ),
                                         ),
                           ),
                     CommentsField(),
                     )

        # Read-only hash fields (enabled in controller if permissible)
        # NB order matters! (for verification hashes)
        hash_fields = ("name", "description")

        # List fields
        list_fields = ["id",
                       "type_id",
                       "name",
                       "valid_from",
                       "obsolete",
                       ]

        # Table Configuration
        self.configure(tablename,
                       # NB must not deduplicate! (invalid operation + breaks vhash chain)
                       list_fields = list_fields,
                       onaccept = self.consent_option_onaccept,
                       )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Create Consent Option"),
            title_display = T("Consent Option Details"),
            title_list = T("Consent Options"),
            title_update = T("Edit Consent Option"),
            label_list_button = T("List Consent Options"),
            label_delete_button = T("Delete Consent Option"),
            msg_record_created = T("Consent Option created"),
            msg_record_modified = T("Consent Option updated"),
            msg_record_deleted = T("Consent Option deleted"),
            msg_list_empty = T("No Consent Options currently defined"),
            )

        # ---------------------------------------------------------------------
        # Consent Question Responses
        #
        tablename = "auth_consent"
        define_table(tablename,
                     self.pr_person_id(),
                     Field("vsign"),
                     Field("vhash", "text"),
                     Field("option_id", "reference auth_consent_option",
                           label = T("Consent Question"),
                           ondelete = "RESTRICT",
                           represent = S3Represent(lookup="auth_consent_option",
                                                   fields = ["name", "valid_from"],
                                                   labels = "%(name)s (%(valid_from)s)",
                                                   ),
                           ),
                     Field("consenting", "boolean",
                           label = T("Consenting"),
                           default = False,
                           represent = s3_yes_no_represent,
                           ),
                     DateField(default = "now",
                               ),
                     DateField("expires_on",
                               label = T("Expires on"),
                               ),
                     )

        # List Fields
        list_fields = ["person_id",
                       "option_id",
                       "consenting",
                       "date",
                       "expires_on",
                       ]

        # Table Configuration
        self.configure(tablename,
                       list_fields = list_fields,
                       onaccept = self.consent_onaccept,
                       insertable = False,
                       editable = False,
                       deletable = False,
                       )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            title_display = T("Consent Details"),
            title_list = T("Consent##plural"),
            label_list_button = T("List Consent"),
            msg_list_empty = T("No Consent currently registered"),
            )

        # ---------------------------------------------------------------------
        # Consent Assertion
        # - when a local user asserts that a non-local entity has consented
        #   to a transaction (e.g. a person who is not registered locally)
        # - differs from auth_consent in that it assigns liability to obtain
        #   consent rather than being proof of consent itself
        # - the respective consent option should therefore be worded as
        #   testimony - not as declaration - of consent
        #
        tablename = "auth_consent_assertion"
        define_table(tablename,
                     self.pr_person_id(), # the person asserting consent
                     Field("context", "text"),
                     Field("option_id", "reference auth_consent_option",
                           ondelete = "RESTRICT",
                           represent = S3Represent(lookup="auth_consent_option"),
                           ),
                     Field("consented", "boolean",
                           default = False,
                           ),
                     DateTimeField(default = "now",
                                   ),
                     Field("vhash", "text"),
                     )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return {"auth_consent_option_hash_fields": hash_fields,
                }

    # -------------------------------------------------------------------------
    @staticmethod
    def consent_option_onaccept(form):
        """
            Onaccept-routine for consent options:
                - set valid_until date when obsolete (or otherwise remove it)
        """

        db = current.db
        s3db = current.s3db

        # Get record ID
        form_vars = form.vars
        if "id" in form_vars:
            record_id = form_vars.id
        elif hasattr(form, "record_id"):
            record_id = form.record_id
        else:
            return

        # Retrieve record (id and obsolete)
        table = s3db.auth_consent_option
        query = (table.id == record_id)
        row = db(query).select(table.id,
                               table.obsolete,
                               table.valid_until,
                               limitby = (0, 1),
                               ).first()
        if not row:
            return

        if row.obsolete:
            if not row.valid_until:
                row.update_record(valid_until = current.request.utcnow.date())
        else:
            row.update_record(valid_until = None)

    # -------------------------------------------------------------------------
    @staticmethod
    def consent_onaccept(form):
        """
            Onaccept-routine for consent:
                - automatically expire all previous consent to the same
                  processing type
        """

        db = current.db
        s3db = current.s3db

        # Get record ID
        form_vars = form.vars
        if "id" in form_vars:
            record_id = form_vars.id
        elif hasattr(form, "record_id"):
            record_id = form.record_id
        else:
            return

        # Retrieve record
        ctable = s3db.auth_consent
        otable = s3db.auth_consent_option
        ttable = s3db.auth_processing_type

        join = [otable.on(otable.id == ctable.option_id),
                ttable.on(ttable.id == otable.type_id),
                ]
        query = (ctable.id == record_id)
        row = db(query).select(ctable.id,
                               ctable.person_id,
                               ttable.id,
                               join = join,
                               limitby = (0, 1),
                               ).first()
        if not row:
            return

        # Expire all previous consent records for the same
        # processing type and person
        today = current.request.utcnow.date()

        consent = row.auth_consent
        processing_type_id = row.auth_processing_type.id

        query = (ctable.person_id == consent.person_id) & \
                ((ctable.expires_on == None) | (ctable.expires_on > today)) & \
                (otable.id == ctable.option_id) & \
                (otable.type_id == processing_type_id) & \
                (ctable.id != consent.id) & \
                (ctable.deleted == False)
        rows = db(query).select(ctable.id)

        query = ctable.id.belongs(set(row.id for row in rows))
        db(query).update(expires_on = today)

# =============================================================================
class AuthMasterKeyModel(DataModel):
    """
        Model to store Master Keys
        - used for Authentication from Mobile App to e.g. Surveys
    """

    names = ("auth_masterkey",
             "auth_masterkey_id",
             "auth_masterkey_token",
             )

    def model(self):

        #T = current.T
        define_table = self.define_table

        # ---------------------------------------------------------------------
        # Master Keys
        #
        tablename = "auth_masterkey"
        define_table(tablename,
                     Field("name", length=254, unique=True,
                           #label = T("Master Key"),
                           requires = IS_LENGTH(254),
                           ),
                     # Which 'dummy' user this master key links to:
                     Field("user_id", current.auth.settings.table_user),
                     )

        represent = S3Represent(lookup=tablename)

        masterkey_id = FieldTemplate("masterkey_id", "reference %s" % tablename,
                                     #label = T("Master Key"),
                                     ondelete = "CASCADE",
                                     represent = represent,
                                     requires = IS_EMPTY_OR(
                                                    IS_ONE_OF(current.db, "auth_masterkey.id",
                                                              represent,
                                                              )),
                                     )

        # ---------------------------------------------------------------------
        # Single-use tokens for master key authentication
        #
        tablename = "auth_masterkey_token"
        define_table(tablename,
                     Field("token", length=64, unique=True),
                     DateTimeField("expires_on"),
                     meta = False,
                     )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return {"auth_masterkey_id": masterkey_id,
                }

# =============================================================================
class AuthUserTempModel(DataModel):
    """
        Model to store complementary data for pending user accounts
        after self-registration
    """

    names = ("auth_user_temp",
             )

    def model(self):

        utable = current.auth.settings.table_user

        # ---------------------------------------------------------------------
        # Temporary User Table
        # - interim storage of registration data that can be used to
        #   create complementary records about a user once their account
        #   is approved
        #
        self.define_table("auth_user_temp",
                          Field("user_id", utable),
                          Field("home"),
                          Field("mobile"),
                          Field("image", "upload",
                                length = current.MAX_FILENAME_LENGTH,
                                ),
                          Field("consent"),
                          Field("custom", "json",
                                requires = IS_EMPTY_OR(IS_JSONS3()),
                                ),
                          MetaFields.uuid(),
                          MetaFields.created_on(),
                          MetaFields.modified_on(),
                          meta = False,
                          )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

# =============================================================================
def auth_user_options_get_osm(pe_id):
    """
        Gets the OSM-related options for a pe_id
    """

    db = current.db
    table = current.s3db.auth_user_options
    query = (table.pe_id == pe_id)
    record = db(query).select(limitby=(0, 1)).first()
    if record:
        return record.osm_oauth_consumer_key, record.osm_oauth_consumer_secret
    else:
        return None

# =============================================================================
class auth_UserRepresent(S3Represent):
    """
        Representation of User IDs to include 1 or more of
            * Name
            * Phone Number
            * Email address
        using the highest-priority contact info available (and permitted)
    """

    def __init__(self,
                 labels = None,
                 linkto = None,
                 show_name = True,
                 show_email = True,
                 show_phone = False,
                 show_org = False,
                 access = None,
                 show_link = True,
                 ):
        """
            Args:
                labels: callable to render the name part
                        (defaults to s3_fullname)
                linkto: a URL (as string) to link representations to,
                        with "[id]" as placeholder for the key
                        (defaults see pr_PersonRepresent)
                show_name: include name in representation
                show_email: include email address in representation
                show_phone: include phone number in representation
                access: access level for contact details,
                            None = ignore access level
                            1 = show private only
                            2 = show public only
                show_link: render as HTML hyperlink
        """

        if labels is None:
            labels = s3_fullname

        super().__init__(lookup = "auth_user",
                         fields = ["id"],
                         labels = labels,
                         linkto = linkto,
                         show_link = show_link,
                         )

        self.show_name = show_name
        self.show_email = show_email
        self.show_phone = show_phone
        self.show_org = show_org
        self.access = access

        self._phone = {}

    # -------------------------------------------------------------------------
    def represent_row(self, row):
        """
            Represent a row

            Args:
                row: the Row
        """

        if self.show_name:
            repr_str = self.labels(row.get("pr_person"))
            if repr_str == "":
                # Fallback to using auth_user name
                # (Need to extra elements as otherwise the pr_person LazySet in the row is queried by s3_fullname)
                user_row = row.get("auth_user")
                repr_str = self.labels(Storage(first_name = user_row.first_name,
                                               last_name = user_row.last_name,
                                               ))
        else:
            repr_str = ""

        if self.show_org:
            organisation_id = row.get("auth_user.organisation_id")
            if organisation_id:
                org = current.s3db.org_OrganisationRepresent()(organisation_id)
                repr_str = "%s (%s)" % (repr_str, org)

        if self.show_email:
            email = row.get("auth_user.email")
            if email:
                if repr_str:
                    repr_str = "%s <%s>" % (repr_str, email)
                else:
                    repr_str = email

        if self.show_phone:
            phone = self._phone.get(row.get("pr_person.pe_id"))
            if phone:
                repr_str = "%s %s" % (repr_str, s3_phone_represent(phone))

        return repr_str

    # -------------------------------------------------------------------------
    def lookup_rows(self, key, values, fields=None):
        """
            Custom rows lookup

            Args:
                key: the key Field
                values: the values
                fields: unused (retained for API compatibility)
        """

        # Lookup pe_ids and name fields
        db = current.db
        s3db = current.s3db

        table = self.table

        show_name = self.show_name
        show_phone = self.show_phone

        count = len(values)
        if count == 1:
            query = (key == values[0])
        else:
            query = key.belongs(values)

        if show_name or show_phone:
            ptable = s3db.pr_person
            ltable = s3db.pr_person_user
            left = [ltable.on(table.id == ltable.user_id),
                    ptable.on(ltable.pe_id == ptable.pe_id),
                    ]
        else:
            left = None

        fields = [table.id]
        if self.show_email:
            fields.append(table.email)
        if show_phone:
            fields.append(ptable.pe_id)
        if self.show_org:
            fields.append(table.organisation_id)
        if show_name:
            fields += [table.first_name,
                       table.last_name,
                       ptable.first_name,
                       ptable.middle_name,
                       ptable.last_name,
                       ]

        rows = db(query).select(*fields,
                                left = left,
                                limitby = (0, count)
                                )
        self.queries += 1

        if show_phone:
            lookup_phone = set()
            phone = self._phone
            for row in rows:
                pe_id = row["pr_person.pe_id"]
                if pe_id not in phone:
                    lookup_phone.add(pe_id)

            if lookup_phone:
                ctable = s3db.pr_contact
                base = current.auth.s3_accessible_query("read", ctable)
                query = base & \
                          (ctable.pe_id.belongs(lookup_phone)) & \
                          (ctable.contact_method == "SMS") & \
                          (ctable.deleted == False)
                access = self.access
                if access:
                    query &= (ctable.access == access)
                contacts = db(query).select(ctable.pe_id,
                                            ctable.value,
                                            orderby = ctable.priority,
                                            )
                self.queries += 1
                for contact in contacts:
                    pe_id = contact.pe_id
                    if not phone.get(pe_id):
                        phone[pe_id] = contact.value

        return rows

# END =========================================================================
