"""
    Consent Tracking

    Copyright: (c) 2018-2022 Sahana Software Foundation

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

__all__ = ("ConsentTracking",
           )

import datetime
import json

from gluon import current, CRYPT, \
                  DIV, INPUT, LABEL, P, SPAN, XML

from s3dal import original_tablename

from ..tools import JSONERRORS, JSONSEPARATORS
from ..resource import FS

# =============================================================================
class ConsentTracking:
    """ Helper class to track consent """

    def __init__(self, processing_types=None):
        """
            Args:
                processing_types: the processing types (default: all types
                                  for which there is a valid consent option)
        """

        self.processing_types = processing_types

    # -------------------------------------------------------------------------
    def widget(self, field, value, **attributes):
        """
            Produce a form widget to request consent, for embedding of consent
            questions in other forms

            Args:
                field: the Field (to hold the response)
                value: the current or default value
                attributes: HTML attributes for the widget
        """

        T = current.T
        fieldname = field.name

        # Consent options to ask
        opts = self.extract()

        # Current consent status (from form)
        selected = self.parse(value)
        value = {}

        # Widget ID
        widget_id = attributes.get("_id")
        if not widget_id:
            widget_id = "%s-consent" % fieldname

        # The widget
        widget = DIV(_id = widget_id,
                     _class = "consent-widget",
                     )

        # Construct the consent options
        has_mandatory_opts = False
        if self.processing_types:
            # Preserve order
            items = ((k, opts[k]) for k in self.processing_types if k in opts)
        else:
            items = opts.items()
        for code, spec in items:

            # Title
            title = spec.get("name")
            if not title:
                continue

            # Current selected-status of this option
            status = selected.get(code)
            v = status[1] if status is not None else spec.get("default", False)

            # The question for this option
            question = LABEL(INPUT(_type="checkbox",
                                   _class = "consent-checkbox",
                                   value = v,
                                   data = {"code": code},
                                   ),
                             SPAN(title,
                                 _class = "consent-title",
                                 ),
                             _class = "consent-question",
                             )

            if spec.get("mandatory"):
                has_mandatory_opts = True
                question.append(SPAN("*", _class="req"))

            # The option
            option = DIV(question, _class="consent-option")

            # Optional explanation
            description = spec.get("description")
            if description:
                option.append(P(XML(description), _class="consent-explanation"))

            # Append to widget
            widget.append(option)

            # Add selected-status to hidden input
            # JSON format: {"code": [id, consenting]}
            value[code] = [spec.get("id"), v]

        # Mandatory options advice
        if has_mandatory_opts:
            widget.append(P("* %s" % T("Consent required"), _class="req_key"))

        # The hidden input
        hidden_input = INPUT(_type = "hidden",
                             _name = attributes.get("_name", fieldname),
                             _id = "%s-response" % widget_id,
                             _value = json.dumps(value),
                             requires = self.validate,
                             )
        widget.append(hidden_input)

        # Inject client-side script and instantiate UI widget
        self.inject_script(widget_id, {})

        return widget

    # -------------------------------------------------------------------------
    def extract(self):
        """ Extract the current consent options """

        db = current.db
        s3db = current.s3db

        ttable = s3db.auth_processing_type
        otable = s3db.auth_consent_option

        left = ttable.on((ttable.id == otable.type_id) & \
                         (ttable.deleted == False))
        today = current.request.utcnow.date()
        query = (otable.valid_from <= today) & \
                (otable.obsolete == False) & \
                (otable.deleted == False)
        if self.processing_types:
            query = (ttable.code.belongs(self.processing_types)) & query

        rows = db(query).select(otable.id,
                                ttable.code,
                                otable.name,
                                otable.description,
                                otable.opt_out,
                                otable.mandatory,
                                left = left,
                                orderby = (~otable.valid_from, ~otable.created_on),
                                )
        options = {}
        for row in rows:
            code = row.auth_processing_type.code
            if code in options:
                continue
            option = row.auth_consent_option
            options[code] = {"id": option.id,
                             "name": option.name,
                             "description": option.description,
                             "default": True if option.opt_out else False,
                             "mandatory": option.mandatory,
                             }
        return options

    # -------------------------------------------------------------------------
    @classmethod
    def parse(cls, value):
        """
            Parse the JSON string returned by the widget

            Args:
                value: the JSON string

            Returns:
                dict with consent question responses,
                format {code: [id, consenting], ...}
        """

        parsed = {}
        if value is not None:
            try:
                parsed = json.loads(value)
            except JSONERRORS:
                pass
        return parsed

    # -------------------------------------------------------------------------
    @classmethod
    def validate(cls, value, record_id=None):
        """
            Validate a consent response (for use with Field.requires)

            Args:
                value: the value returned from the widget
        """

        T = current.T
        invalid = T("Invalid value")

        error = None
        parsed = cls.parse(value)
        if not parsed or not isinstance(parsed, dict):
            error = invalid
        else:
            try:
                option_ids = {v[0] for v in parsed.values()}
            except (TypeError, IndexError):
                error = invalid
            else:
                # Retrieve the relevant consent options
                s3db = current.s3db
                ttable = s3db.auth_processing_type
                otable = s3db.auth_consent_option
                join = ttable.on(ttable.id == otable.type_id)
                query = otable.id.belongs(option_ids)
                rows = current.db(query).select(otable.id,
                                                otable.obsolete,
                                                otable.mandatory,
                                                ttable.code,
                                                join = join,
                                                )
                options = {}
                for row in rows:
                    processing = row.auth_processing_type
                    option = row.auth_consent_option
                    options[option.id] = (processing.code, option.obsolete, option.mandatory)

                # Validate each response
                for code, spec in parsed.items():
                    option_id, consenting = spec
                    option = options.get(option_id)

                    if not option or option[0] != code:
                        # Option does not exist or does not match the code
                        error = invalid
                        break
                    if option[1]:
                        # Option is obsolete
                        error = T("Obsolete option: %(code)s") % {"code": code}
                        break
                    if option[2] and not consenting:
                        # Required consent has not been given
                        error = T("Required consent not given")
                        break

        return (None, error) if error else (value, None)

    # -------------------------------------------------------------------------
    @staticmethod
    def inject_script(widget_id, options):
        """
            Inject static JS and instantiate client-side UI widget

            Args:
                widget_id: the widget ID
                options: JSON-serializable dict with UI widget options
        """

        request = current.request
        s3 = current.response.s3

        # Static script
        if s3.debug:
            script = "/%s/static/scripts/S3/s3.ui.consent.js" % \
                     request.application
        else:
            script = "/%s/static/scripts/S3/s3.ui.consent.min.js" % \
                     request.application
        scripts = s3.scripts
        if script not in scripts:
            scripts.append(script)

        # Widget options
        opts = {}
        if options:
            opts.update(options)

        # Widget instantiation
        script = '''$('#%(widget_id)s').consentQuestion(%(options)s)''' % \
                 {"widget_id": widget_id,
                  "options": json.dumps(opts),
                  }
        jquery_ready = s3.jquery_ready
        if script not in jquery_ready:
            jquery_ready.append(script)

    # -------------------------------------------------------------------------
    @classmethod
    def track(cls, person_id, value, timestmp=None, allow_obsolete=True):
        """
            Record response to consent question

            Args:
                person_id: the person consenting
                value: the value returned from the widget
                timestmp: the date/time when the consent was given
                allow_obsolete: allow tracking of obsolete consent options
        """

        db = current.db
        s3db = current.s3db
        request = current.request

        today = timestmp.date() if timestmp else request.utcnow.date()
        vsign = request.env.remote_addr

        # Consent option hash fields
        hash_fields = s3db.auth_consent_option_hash_fields

        # Parse the value
        parsed = cls.parse(value)

        # Get all current+valid options matching the codes
        ttable = s3db.auth_processing_type
        otable = s3db.auth_consent_option

        option_fields = {"id", "validity_period"} | set(hash_fields)
        fields = [ttable.code] + [otable[fn] for fn in option_fields]

        join = ttable.on(ttable.id == otable.type_id)
        query = (ttable.code.belongs(set(parsed.keys()))) & \
                (otable.deleted == False)
        if not allow_obsolete:
            query &= (otable.obsolete == False)
        rows = db(query).select(join=join, *fields)

        valid_options = {}
        for row in rows:
            option = row.auth_consent_option
            context = [(fn, option[fn]) for fn in hash_fields]
            valid_options[option.id] = {"code": row.auth_processing_type.code,
                                        "hash": cls.get_hash(context),
                                        "valid_for": option.validity_period,
                                        }

        ctable = s3db.auth_consent
        record_ids = []
        for code, response in parsed.items():

            option_id, consenting = response

            # Verify option_id
            option = valid_options.get(option_id)
            if not option or option["code"] != code:
                raise ValueError("Invalid consent option: %s#%s" % (code, option_id))

            consent = (("date", today.isoformat()),
                       ("option_id", option_id),
                       ("person_id", person_id),
                       ("vsign", vsign),
                       ("consenting", consenting),
                       ("ohash", option["hash"]),
                       )

            # Store the hash for future verification
            consent = dict(consent[:5])
            consent["vhash"] = cls.get_hash(consent)

            # Update data
            consent["date"] = today
            valid_for = option["valid_for"]
            if valid_for:
                consent["expires_on"] = today + datetime.timedelta(days=valid_for)

            # Create new consent record
            record_id = ctable.insert(**consent)
            if record_id:
                consent["id"] = record_id
                s3db.onaccept(ctable, consent)
                record_ids.append(record_id)

        return record_ids

    # -------------------------------------------------------------------------
    @classmethod
    def register_consent(cls, user_id):
        """
            Track consent responses given during user self-registration

            Args:
                user_id: the auth_user ID
        """

        db = current.db
        s3db = current.s3db

        ltable = s3db.pr_person_user
        ptable = s3db.pr_person

        # Look up the person ID
        join = ptable.on(ptable.pe_id == ltable.pe_id)
        person = db(ltable.user_id == user_id).select(ptable.id,
                                                      join = join,
                                                      limitby = (0, 1),
                                                      ).first()
        if person:
            person_id = person.id

            # Look up the consent response from temp user record
            ttable = s3db.auth_user_temp
            row = db(ttable.user_id == user_id).select(ttable.id,
                                                       ttable.consent,
                                                       ttable.created_on,
                                                       limitby = (0, 1),
                                                       ).first()
            if row and row.consent:
                # Track consent
                cls.track(person_id, row.consent,
                          timestmp = row.created_on,
                          )

                # Reset consent response in temp user record
                row.update_record(consent=None)

    # -------------------------------------------------------------------------
    @classmethod
    def assert_consent(cls,
                       context,
                       code,
                       value,
                       person_id = None,
                       timestmp = None,
                       allow_obsolete = False,
                       ):
        """
            Assert consent of a non-local entity

            Args:
                context: string specifying the transaction to which
                         consent was to be obtained
                code: the processing type code
                value: the value returned from the consent widget
                person_id: the person asserting consent (defaults to
                           the current user)
                timestmp: datetime when consent was obtained (defaults
                          to current time)
                allow_obsolete: allow recording assertions for obsolete
                                consent options

            Returns:
                the consent assertion record ID

            Raises:
                TypeError: for invalid parameter types
                ValueError: for invalid input data

        """

        if not context:
            raise ValueError("Context is required")
        context = str(context)

        now = current.request.utcnow
        if not timestmp:
            timestmp = now
        elif not isinstance(timestmp, datetime.datetime):
            raise TypeError("Invalid timestmp type, expected datetime but got %s" % type(timestmp))
        elif timestmp > now:
            raise ValueError("Future timestmp not permitted")
        timestmp = timestmp.replace(microsecond=0)

        if not person_id:
            person_id = current.auth.s3_logged_in_person()
        if not person_id:
            raise ValueError("Must be logged in or specify a person_id")

        # Parse the value and extract the option_id
        parsed = cls.parse(value)
        consent = parsed.get(code)
        if not consent:
            raise ValueError("Invalid JSON, or no response for processing type found")
        option_id, response = consent

        # Get all current+valid options matching the codes
        db = current.db
        s3db = current.s3db

        ttable = s3db.auth_processing_type
        otable = s3db.auth_consent_option

        hash_fields = s3db.auth_consent_option_hash_fields
        option_fields = {"id"} | set(hash_fields)
        fields = [otable[fn] for fn in option_fields]

        join = ttable.on((ttable.id == otable.type_id) & \
                         (ttable.code == code))
        query = (otable.id == option_id) & \
                (otable.deleted == False)
        if not allow_obsolete:
            query &= (otable.obsolete == False)
        option = db(query).select(*fields,
                                  join = join,
                                  limitby = (0, 1),
                                  ).first()
        if not option:
            raise ValueError("Invalid consent option for processing type")

        ohash = cls.get_hash([(fn, option[fn]) for fn in hash_fields])
        consent = (("person_id", person_id),
                   ("context", context),
                   ("date", timestmp.isoformat()),
                   ("option_id", option.id),
                   ("consented", bool(response)),
                   ("ohash", ohash),
                   )
        # Generate verification hash
        vhash = cls.get_hash(consent)

        consent = dict(consent[:5])
        consent["vhash"] = vhash
        consent["date"] = timestmp

        atable = s3db.auth_consent_assertion
        record_id = atable.insert(**consent)
        if record_id:
            consent["id"] = record_id
            s3db.onaccept(atable, consent)

        return record_id

    # -------------------------------------------------------------------------
    @classmethod
    def verify(cls, record_id):
        """
            Verify a consent record (checks the hash, not expiry)

            Args:
                record_id: the consent record ID
        """

        db = current.db
        s3db = current.s3db

        # Consent option hash fields
        hash_fields = s3db.auth_consent_option_hash_fields

        # Load consent record and referenced option
        otable = s3db.auth_consent_option
        ctable = s3db.auth_consent

        join = otable.on(otable.id == ctable.option_id)
        query = (ctable.id == record_id) & (ctable.deleted == False)

        fields = [otable.id,
                  ctable.date,
                  ctable.person_id,
                  ctable.option_id,
                  ctable.vsign,
                  ctable.vhash,
                  ctable.consenting,
                  ] + [otable[fn] for fn in hash_fields]

        row = db(query).select(join=join, limitby=(0, 1), *fields).first()
        if not row:
            return False

        option = row.auth_consent_option
        context = [(fn, option[fn]) for fn in hash_fields]

        consent = row.auth_consent
        verify = (("date", consent.date.isoformat()),
                  ("option_id", consent.option_id),
                  ("person_id", consent.person_id),
                  ("vsign", consent.vsign),
                  ("consenting", consent.consenting),
                  ("ohash", cls.get_hash(context)),
                  )

        return consent.vhash == cls.get_hash(verify)

    # -------------------------------------------------------------------------
    @staticmethod
    def get_hash(data):
        """
            Produce a hash for JSON-serializable data

            Args:
                data: the JSON-serializable data (normally a dict)

            Returns:
                the hash as string
        """

        inp = json.dumps(data, separators=JSONSEPARATORS)

        crypt = CRYPT(key = current.deployment_settings.hmac_key,
                      digest_alg = "sha512",
                      salt = False,
                      )
        return str(crypt(inp)[0])

    # -------------------------------------------------------------------------
    @staticmethod
    def get_consent_options(code):
        """
            Get all currently valid consent options for a processing type

            Args:
                code: the processing type code

            Returns:
                set of record IDs
        """

        s3db = current.s3db

        today = current.request.utcnow.date()

        ttable = s3db.auth_processing_type
        otable = s3db.auth_consent_option
        join = ttable.on((ttable.id == otable.type_id) & \
                         (ttable.deleted == False))
        query = (ttable.code == code) & \
                (otable.valid_from <= today) & \
                (otable.obsolete == False) & \
                (otable.deleted == False)
        rows = current.db(query).select(otable.id, join=join)

        return set(row.id for row in rows)

    # -------------------------------------------------------------------------
    @classmethod
    def has_consented(cls, person_id, code):
        """
            Check valid+current consent for a particular processing type

            Args:
                person_id: the person to check consent for
                code: the data processing type code

            Returns:
                True|False whether or not the person has consented
                to this type of data processing and consent has not
                expired

            Example:
                consent = ConsentTracking()
                if consent.has_consented(auth.s3_logged_in_person(), "PIDSHARE"):
                    # perform PIDSHARE...
        """

        # Get all current consent options for the code
        option_ids = cls.get_consent_options(code)
        if not option_ids:
            return False

        # Check if there is a positive consent record for this person
        # for any of these consent options that has not expired
        today = current.request.utcnow.date()

        ctable = current.s3db.auth_consent
        query = (ctable.person_id == person_id) & \
                (ctable.option_id.belongs(option_ids)) & \
                ((ctable.expires_on == None) | (ctable.expires_on > today)) & \
                (ctable.consenting == True) & \
                (ctable.deleted == False)
        row = current.db(query).select(ctable.id, limitby = (0, 1)).first()

        return row is not None

    # -------------------------------------------------------------------------
    def pending_responses(self, person_id):
        """
            Identify all processing types for which a person has not
            responded to the updated consent questions, or where their
            previously given consent has expired

            Args:
                person_id: the person ID

            Returns:
                list of processing type codes
        """

        # Get all current consent options for the given processing types
        options = self.extract()
        option_ids = {spec["id"] for spec in options.values()}

        # Find all responses of this person to these options
        today = current.request.utcnow.date()
        ctable = current.s3db.auth_consent
        query = (ctable.person_id == person_id) & \
                (ctable.option_id.belongs(option_ids)) & \
                ((ctable.consenting == False) | \
                 (ctable.expires_on == None) | \
                 (ctable.expires_on > today)) & \
                (ctable.deleted == False)
        rows = current.db(query).select(ctable.option_id)

        # Identify any pending responses
        responded = {row.option_id for row in rows}
        pending = []
        for code, spec in options.items():
            if spec["id"] not in responded:
                pending.append(code)

        return pending

    # -------------------------------------------------------------------------
    @classmethod
    def consent_query(cls, table, code, field=None):
        """
            Get a query for table for records where the person identified
            by field has consented to a certain type of data processing.

            - useful to limit background processing that requires consent

            Args:
                table: the table to query
                code: the processing type code to check
                field: the field in the table referencing pr_person.id

            Returns:
                Query

            Example:
                consent = ConsentTracking()
                query = consent.consent_query(table, "PIDSHARE") & (table.deleted == False)
                # Perform PIDSHARE with query result...
                rows = db(query).select(*fields)
        """

        if field is None:
            if original_tablename(table) == "pr_person":
                field = table.id
            else:
                field = table.person_id
        elif isinstance(field, str):
            field = table[field]

        option_ids = cls.get_consent_options(code)
        today = current.request.utcnow.date()

        ctable = current.s3db.auth_consent
        query = (ctable.person_id == field) & \
                (ctable.option_id.belongs(option_ids)) & \
                ((ctable.expires_on == None) | (ctable.expires_on > today)) & \
                (ctable.consenting == True) & \
                (ctable.deleted == False)

        return query

    # -------------------------------------------------------------------------
    @classmethod
    def consent_filter(cls, code, selector=None):
        """
            Filter resource for records where the person identified by
            selector has consented to a certain type of data processing.

            - useful to limit REST methods that require consent

            Args:
                code: the processing type code to check
                selector: a field selector (string) that references
                          pr_person.id; if not specified pr_person is
                          assumed to be the master resource

            Returns:
                S3ResourceQuery

            Example:
                consent = ConsentTracking
                resource.add_filter(consent.consent_filter("PIDSHARE", "~.person_id"))

            Note:
                only one consent filter can be used for the same resource;
                if multiple consent options must be checked and/or multiple
                person_id references apply independently, then either aliased
                auth_consent components can be used to construct a filter, or
                the query must be split (the latter typically performs better).
                Ideally, however, the consent decision for a single operation
                should not be complex or second-guessing.
        """

        option_ids = cls.get_consent_options(code)
        today = current.request.utcnow.date()

        # Construct sub-selectors
        if selector and selector not in ("id", "~.id"):
            consent = "%s$person_id:auth_consent" % selector
        else:
            # Assume pr_person is master
            consent = "person_id:auth_consent"
        option_id = FS("%s.option_id" % consent)
        expires_on = FS("%s.expires_on" % consent)
        consenting = FS("%s.consenting" % consent)

        query = (option_id.belongs(option_ids)) & \
                ((expires_on == None) | (expires_on > today)) & \
                (consenting == True)

        return query

# END =========================================================================
