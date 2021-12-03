"""
    Record Anonymizing

    Copyright: 2018-2021 (c) Sahana Software Foundation

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

import json
from uuid import uuid4

from gluon import current, redirect, A, BUTTON, DIV, FORM, INPUT, LABEL, P

from s3dal import original_tablename

from ..tools import JSONERRORS, s3_str

from .base import CRUDMethod

__all__ = ("S3Anonymize",
           "S3AnonymizeWidget",
           "S3AnonymizeBulk",
           "S3AnonymizeBulkWidget",
           )

# =============================================================================
class S3Anonymize(CRUDMethod):
    """
        REST Method to Anonymize a Record
        - usually pr_person
    """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point for REST API

            Args:
                r: the CRUDRequest instance
                attr: controller parameters

            Returns:
                output data (JSON)
        """

        output = {}

        table, record_id = self.get_target_id()
        if not table:
            r.error(405, "Anonymizing not configured for resource")
        if not record_id:
            r.error(400, "No target record specified")
        if not self.permitted(table, record_id):
            r.unauthorised()

        if r.representation == "json":
            if r.http == "POST":
                output = self.anonymize(r, table, record_id)
            else:
                r.error(405, current.ERROR.BAD_METHOD)
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        # Set Content Type
        current.response.headers["Content-Type"] = "application/json"

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def anonymize(cls, r, table, record_id):
        """
            Handle POST (anonymize-request), i.e. anonymize the target record

            Args:
                r: the CRUDRequest
                table: the target Table
                record_id: the target record ID

            Returns:
                JSON message
        """

        # Read+parse body JSON
        s = r.body
        s.seek(0)
        try:
            options = json.load(s)
        except JSONERRORS:
            options = None
        if not isinstance(options, dict):
            r.error(400, "Invalid request options")

        # Verify submitted action key against session (CSRF protection)
        widget_id = "%s-%s-anonymize" % (table, record_id)
        session_s3 = current.session.s3
        keys = session_s3.anonymize
        if keys is None or \
           widget_id not in keys or \
           options.get("key") != keys[widget_id]:
            r.error(400, "Invalid action key (form reopened in another tab?)")

        # Get the available rules from settings
        rules = current.s3db.get_config(table, "anonymize")
        if isinstance(rules, (tuple, list)):
            names = set(rule.get("name") for rule in rules)
            names.discard(None)
        else:
            # Single rule
            rules["name"] = "default"
            names = (rules["name"],)
            rules = [rules]

        # Get selected rules from options
        selected = options.get("apply")
        if not isinstance(selected, list):
            r.error(400, "Invalid request options")

        # Validate selected rules
        for name in selected:
            if name not in names:
                r.error(400, "Invalid rule: %s" % name)

        # Merge selected rules
        cleanup = {}
        cascade = []
        for rule in rules:
            name = rule.get("name")
            if not name or name not in selected:
                continue
            field_rules = rule.get("fields")
            if field_rules:
                cleanup.update(field_rules)
            cascade_rules = rule.get("cascade")
            if cascade_rules:
                cascade.extend(cascade_rules)

        # Apply selected rules
        if cleanup or cascade:
            rules = {"fields": cleanup,
                     "cascade": cascade,
                     }

            # NB will raise (+roll back) if configuration is invalid
            cls.cascade(table, (record_id,), rules)

            # Audit anonymize
            prefix, name = original_tablename(table).split("_", 1)
            current.audit("anonymize", prefix, name,
                          record = record_id,
                          representation = "html",
                          )

            output = current.xml.json_message(updated=record_id)
        else:
            output = current.xml.json_message(msg="No applicable rules found")

        return output

    # -------------------------------------------------------------------------
    def get_target_id(self):
        """
            Determine the target table and record ID

            Returns:
                tuple (table, record_id)
        """

        resource = self.resource

        rules = resource.get_config("anonymize")
        if not rules:
            return None, None

        return resource.table, self.record_id

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted(table, record_id):
        """
            Check permissions to anonymize the target record

            Args:
                table: the target Table
                record_id: the target record ID

            Returns:
                True|False
        """

        has_permission = current.auth.s3_has_permission

        return has_permission("update", table, record_id=record_id) and \
               has_permission("delete", table, record_id=record_id)

    # -------------------------------------------------------------------------
    @classmethod
    def cascade(cls, table, record_ids, rules):
        """
            Apply cascade of rules to anonymize records

            Args:
                table: the Table
                record_ids: a set of record IDs
                rules: the rules for this Table

            Raises:
                Exception: if the cascade failed due to DB constraints
                           or invalid rules; callers should roll back
                           the transaction if an exception is raised
        """

        from ..resource import FS, S3Joins

        s3db = current.s3db

        pkey = table._id.name

        cascade = rules.get("cascade")
        if cascade:

            fieldnames = set(rule.get("match", pkey) for _, rule in cascade)
            if pkey not in fieldnames:
                fieldnames.add(pkey)
            fields = [table[fn] for fn in fieldnames]

            db = current.db
            rows = db(table._id.belongs(record_ids)).select(*fields)

            for tablename, rule in cascade:

                lookup = rule.get("lookup")
                if lookup:
                    # Explicit look-up function, call with master table+rows,
                    # as well as the name of the related table; should return
                    # a set/tuple/list of record ids in the related table
                    ids = lookup(table, rows, tablename)
                else:
                    key = rule.get("key")
                    if not key:
                        continue

                    field = rule.get("match", pkey)
                    match = set(row[field] for row in rows)

                    # Resolve key and construct query
                    resource = s3db.resource(tablename, components=[])
                    rq = FS(key).belongs(match)
                    query = rq.query(resource)

                    # Construct necessary joins
                    joins = S3Joins(tablename)
                    joins.extend(rq._joins(resource)[0])
                    joins = joins.as_list()

                    # Extract the target table IDs
                    target_rows = db(query).select(resource._id,
                                                   join = joins,
                                                   )
                    ids = set(row[resource._id.name] for row in target_rows)

                # Recurse into related table
                if ids:
                    cls.cascade(resource.table, ids, rule)

        # Apply field rules
        field_rules = rules.get("fields")
        if field_rules:
            cls.apply_field_rules(table, record_ids, field_rules)

        # Apply deletion rules
        if rules.get("delete"):
            resource = s3db.resource(table, id=list(record_ids))
            resource.delete(cascade=True)

    # -------------------------------------------------------------------------
    @staticmethod
    def apply_field_rules(table, record_ids, rules):
        """
            Apply field rules on a set of records in a table

            Args:
                table: the Table
                record_ids: the record IDs
                rules: the rules

            Raises:
                Exception: if the field rules could not be applied
                           due to DB constraints or invalid rules;
                           callers should roll back the transaction
                           if an exception is raised
        """

        fields = [table[fn] for fn in rules if fn in table.fields]
        if table._id.name not in rules:
            fields.insert(0, table._id)

        # Select the records
        query = table._id.belongs(record_ids)
        rows = current.db(query).select(*fields)

        pkey = table._id.name

        s3db = current.s3db
        update_super = s3db.update_super
        onaccept = s3db.onaccept

        for row in rows:
            data = {}
            for fieldname, rule in rules.items():

                if fieldname in table.fields:
                    field = table[fieldname]
                else:
                    continue

                if rule == "remove":
                    # Set to None
                    if field.notnull:
                        raise ValueError("Cannot remove %s - must not be NULL" % field)
                    else:
                        data[fieldname] = None

                elif rule == "reset":
                    # Reset to the field's default value
                    default = field.default
                    if default is None and field.notnull:
                        raise ValueError("Cannot reset %s - default value None violates notnull-constraint")
                    data[fieldname] = default

                elif callable(rule):
                    # Callable rule to procude a new value
                    new_value = rule(row[pkey], field, row[field])
                    if fieldname != table._id.name:
                        data[fieldname] = new_value

                elif type(rule) is tuple:
                    method, value = rule
                    if method == "set":
                        # Set a fixed value
                        data[fieldname] = value

            if data:
                success = row.update_record(**data)
                if not success:
                    raise ValueError("Could not clean %s record" % table)

                update_super(table, row)

                data[pkey] = row[pkey]
                onaccept(table, data, method="update")

# =============================================================================
class S3AnonymizeWidget:
    """
        GUI widget for S3Anonymize
        - popup
        - acts via AJAX
    """

    # -------------------------------------------------------------------------
    @classmethod
    def widget(cls,
               r,
               label = "Anonymize",
               ajaxURL = None,
               _class = "action-lnk",
               ):
        """
            Render an action item (link or button) to anonymize the
            target record of an CRUDRequest, which can be embedded in
            the record view

            Args:
                r: the CRUDRequest
                label: The label for the action item
                ajaxURL: The URL for the AJAX request
                _class: HTML class for the action item

            Returns:
                the action item (a HTML helper instance), or an empty
                string if no anonymize-rules are configured for the
                target table, no target record was specified or the
                user is not permitted to anonymize it
        """

        T = current.T

        default = ""

        # Determine target table
        if r.component:
            resource = r.component
            if resource.link and not r.actuate_link():
                resource = resource.link
        else:
            resource = r.resource
        table = resource.table

        # Determine target record
        record_id = S3Anonymize._record_id(r)
        if not record_id:
            return default

        # Check if target is configured for anonymize
        rules = resource.get_config("anonymize")
        if not rules:
            return default
        if not isinstance(rules, (tuple, list)):
            # Single rule
            rules["name"] = "default"
            rules = [rules]

        # Check permissions to anonymize
        if not S3Anonymize.permitted(table, record_id):
            return default

        # Determine widget ID
        widget_id = "%s-%s-anonymize" % (table, record_id)

        # Inject script
        if ajaxURL is None:
            ajaxURL = r.url(method = "anonymize",
                            representation = "json",
                            )
        script_options = {"ajaxURL": ajaxURL,
                          }
        next_url = resource.get_config("anonymize_next")
        if next_url:
            script_options["nextURL"] = next_url
        cls.inject_script(widget_id, script_options)

        # Action button
        translated_label = T(label)
        action_button = A(translated_label, _class="anonymize-btn")
        if _class:
            action_button.add_class(_class)

        # Dialog and Form
        INFO = T("The following information will be deleted from the record")
        CONFIRM = T("Are you sure you want to delete the selected details?")
        SUCCESS = T("Action successful - please wait...")

        form = FORM(P("%s:" % INFO),
                    cls.selector(rules),
                    P(CONFIRM),
                    DIV(INPUT(value = "anonymize_confirm",
                              _name = "anonymize_confirm",
                              _type = "checkbox",
                              ),
                    LABEL(T("Yes, delete the selected details")),
                          _class = "anonymize-confirm",
                          ),
                    cls.buttons(),
                    _class = "anonymize-form",
                    # Store action key in form
                    hidden = {"action-key": cls.action_key(widget_id)},
                    )

        dialog = DIV(form,
                     DIV(P(SUCCESS),
                         _class = "hide anonymize-success",
                         ),
                     _class = "anonymize-dialog hide",
                     _title = translated_label,
                     )

        # Assemble widget
        widget = DIV(action_button,
                     dialog,
                     _class = "s3-anonymize",
                     _id = widget_id,
                     )

        return widget

    # -------------------------------------------------------------------------
    @staticmethod
    def action_key(widget_id):
        """
            Generate a unique STP token for the widget (CSRF protection) and
            store it in session

            Args:
                widget_id: the widget ID (which includes the target
                           table name and record ID)

            Returns:
                a unique identifier (as string)
        """

        session_s3 = current.session.s3

        keys = session_s3.anonymize
        if keys is None:
            session_s3.anonymize = keys = {}
        key = keys[widget_id] = str(uuid4())

        return key

    # -------------------------------------------------------------------------
    @staticmethod
    def selector(rules):
        """
            Generate the rule selector for anonymize-form

            Args:
                rules: the list of configured rules

            Returns:
                the selector (DIV)
        """

        T = current.T

        selector = DIV(_class = "anonymize-select",
                       )

        for rule in rules:

            name = rule.get("name")
            if not name:
                continue

            title = T(rule.get("title", name))

            selector.append(DIV(INPUT(value = "on",
                                      _name = s3_str(name),
                                      _type = "checkbox",
                                      _class = "anonymize-rule",
                                      ),
                                LABEL(title),
                                _class = "anonymize-option",
                                ))

        return selector

    # -------------------------------------------------------------------------
    @staticmethod
    def buttons():
        """
            Generate the submit/cancel buttons for the anonymize-form

            Returns:
                the buttons row (DIV)
        """

        T = current.T

        return DIV(BUTTON(T("Submit"),
                          _class = "small alert button anonymize-submit",
                          _disabled = "disabled",
                          _type = "button",
                          ),
                   A(T("Cancel"),
                     _class = "cancel-form-btn action-lnk anonymize-cancel",
                     _href = "javascript:void(0)",
                     ),
                   _class = "anonymize-buttons",
                   )

    # -------------------------------------------------------------------------
    @staticmethod
    def inject_script(widget_id, options):
        """
            Inject the necessary JavaScript for the UI dialog

            Args:
                widget_id: the widget ID
                options: JSON-serializable dict of widget options
        """

        request = current.request
        s3 = current.response.s3

        # Static script
        if s3.debug:
            script = "/%s/static/scripts/S3/s3.ui.anonymize.js" % \
                     request.application
        else:
            script = "/%s/static/scripts/S3/s3.ui.anonymize.min.js" % \
                     request.application
        scripts = s3.scripts
        if script not in scripts:
            scripts.append(script)

        # Widget options
        opts = {}
        if options:
            opts.update(options)

        # Widget instantiation
        script = '''$('#%(widget_id)s').anonymize(%(options)s)''' % \
                 {"widget_id": widget_id,
                  "options": json.dumps(opts),
                  }
        jquery_ready = s3.jquery_ready
        if script not in jquery_ready:
            jquery_ready.append(script)

# =============================================================================
class S3AnonymizeBulk(S3Anonymize):
    """
        REST Method to Anonymize Records
        - usually auth_user
    """

    def apply_method(self, r, **attr):
        """
            Entry point for REST API

            Args:
                r: the CRUDRequest instance
                attr: controller parameters

            Returns:
                output data (JSON)
        """

        resource = self.resource
        rules = resource.get_config("anonymize")
        if not rules:
            r.error(405, "Anonymizing not configured for resource")

        record_ids = current.session.s3.get("anonymize_record_ids")
        if not record_ids:
            r.error(400, "No target record(s) specified")

        table = resource.table

        # Check permission for each record
        has_permission = current.auth.s3_has_permission
        for record_id in record_ids:
            if not has_permission("update", table, record_id=record_id) or \
               not has_permission("delete", table, record_id=record_id):
                r.unauthorised()

        output = {}

        if r.representation == "html":
            if r.http == "GET":
                # Show form
                anonymise_btn = S3AnonymizeBulkWidget.widget(r,
                                                             record_ids = record_ids,
                                                             _class = "action-btn anonymize-btn",
                                                             )
                current.response.view = "simple.html"
                output = {"item": anonymise_btn,
                          "title": current.T("Anonymize Records"),
                          }
            elif r.http == "POST":
                # Process form
                output = self.anonymize(r, table, record_ids)
                del current.session.s3["anonymize_record_ids"]
                next_url = resource.get_config("anonymize_next")
                if next_url:
                    redirect(next_url)
            else:
                r.error(405, current.ERROR.BAD_METHOD)
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def anonymize(cls, r, table, record_ids):
        """
            Handle POST (anonymize-request), i.e. anonymize the target record

            Args:
                r: the CRUDRequest
                table: the target Table
                record_ids: the target record IDs

            Returns:
                JSON message
        """

        post_vars_get = r.post_vars.get

        # Verify submitted action key against session (CSRF protection)
        widget_id = "%s-anonymize" % table
        session_s3 = current.session.s3
        keys = session_s3.anonymize
        if keys is None or \
           widget_id not in keys or \
           post_vars_get("action-key") != keys[widget_id]:
            r.error(400, "Invalid action key (form reopened in another tab?)")

        # Get the available rules from settings
        rules = current.s3db.get_config(table, "anonymize")
        if isinstance(rules, (tuple, list)):
            names = set(rule.get("name") for rule in rules)
            names.discard(None)
        else:
            # Single rule
            rules["name"] = "default"
            names = (rules["name"],)
            rules = [rules]

        # Get selected rules from form
        selected = []
        for rule in rules:
            rule_name = rule.get("name")
            if not rule_name:
                continue
            if post_vars_get(rule_name) == "on":
                selected.append(rule)

        # Merge selected rules
        cleanup = {}
        cascade = []
        for rule in selected:
            field_rules = rule.get("fields")
            if field_rules:
                cleanup.update(field_rules)
            cascade_rules = rule.get("cascade")
            if cascade_rules:
                cascade.extend(cascade_rules)

        # Apply selected rules
        if cleanup or cascade:
            rules = {"fields": cleanup,
                     "cascade": cascade,
                     }

            for record_id in record_ids:
                # NB will raise (+roll back) if configuration is invalid
                cls.cascade(table, (record_id,), rules)

                # Audit anonymize
                prefix, name = original_tablename(table).split("_", 1)
                current.audit("anonymize", prefix, name,
                              record = record_id,
                              representation = "html",
                              )

            output = current.xml.json_message(updated=record_ids)
        else:
            output = current.xml.json_message(msg="No applicable rules found")

        return output

# =============================================================================
class S3AnonymizeBulkWidget(S3AnonymizeWidget):
    """
        GUI widget for S3AnonymizeBulk
        - normal page (not popup)
        - acts via POST (not AJAX)
    """

    # -------------------------------------------------------------------------
    @classmethod
    def widget(cls,
               r,
               record_ids = None,
               _class = "action-lnk",
               ):
        """
            Render an action item (link or button) to anonymize the
            provided records

            Args:
                r: the CRUDRequest
                record_ids: The list of record_ids to act on
                _class: HTML class for the action item

            Returns:
                the action item (a HTML helper instance), or an empty
                string if no anonymize-rules are configured for the
                target table, no target record was specified or the
                user is not permitted to anonymize it
        """

        T = current.T

        default = ""

        # Determine target table
        if r.component:
            resource = r.component
            if resource.link and not r.actuate_link():
                resource = resource.link
        else:
            resource = r.resource
        table = resource.table

        # Determine target record
        if not record_ids:
            return default

        # Check if target is configured for anonymize
        rules = resource.get_config("anonymize")
        if not rules:
            return default
        if not isinstance(rules, (tuple, list)):
            # Single rule
            rules["name"] = "default"
            rules = [rules]

        # Determine widget ID
        widget_id = "%s-anonymize" % table

        # Dialog and Form
        INFO = T("The following information will be deleted from all the selected records")
        CONFIRM = T("Are you sure you want to delete the selected details?")
        #SUCCESS = T("Action successful - please wait...")

        form = FORM(P("%s:" % INFO),
                    cls.selector(rules),
                    P(CONFIRM),
                    DIV(INPUT(value = "anonymize_confirm",
                              _name = "anonymize_confirm",
                              _type = "checkbox",
                              ),
                    LABEL(T("Yes, delete the selected details")),
                          _class = "anonymize-confirm",
                          ),
                    DIV(INPUT(_class = "small alert button anonymize-submit",
                              _disabled = "disabled",
                              _type = "submit",
                              _value = T("Anonymize"),
                              ),
                        _class = "anonymize-buttons",
                        ),
                    _class = "anonymize-form",
                    # Store action key in form
                    hidden = {"action-key": cls.action_key(widget_id)},
                    )

        script = '''var submitButton=$('.anonymize-submit');
$('input[name="anonymize_confirm"]').off('.anonymize').on('click.anonymize',function(){if ($(this).prop('checked')){submitButton.prop('disabled',false);}else{submitButton.prop('disabled',true);}});'''
        current.response.s3.jquery_ready.append(script)

        return form

# END =========================================================================
