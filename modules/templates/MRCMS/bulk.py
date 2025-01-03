"""
    Bulk Methods for MRCMS

    License: MIT
"""

import datetime
import json

from gluon import current, redirect, \
                  A, BUTTON, DIV, FORM, INPUT, LABEL, P, SCRIPT, TAG
from gluon.storage import Storage

from s3dal import Field

from core import CRUDMethod, FS, FormKey, \
                 IS_UTC_DATETIME, S3CalendarWidget, S3DateTime

# =============================================================================
class CompleteAppointments(CRUDMethod):
    """ Method to complete appointments in-bulk """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point for CRUD controller

            Args:
                r: the CRUDRequest instance
                attr: controller parameters

            Returns:
                output data (JSON)
        """

        output = {}

        if r.http == "POST":
            if r.ajax or r.representation in ("json", "html"):
                output = self.complete(r, **attr)
            else:
                r.error(415, current.ERROR.BAD_FORMAT)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def complete(self, r, **attr):
        """
            Provide a dialog to enter the actual date interval and confirm
            completion, and upon submission, mark the appointments as completed

            Args:
                r: the CRUDRequest
                table: the target table

            Returns:
                a JSON object with the dialog HTML as string

            Note:
                redirects to /select upon completion
        """

        s3 = current.response.s3

        resource = self.resource
        table = resource.table

        get_vars = r.get_vars

        # Select-URL for redirections
        select_vars = {"$search": "session"}
        select_url = r.url(method="select", representation="", vars=select_vars)

        if any(key not in r.post_vars for key in ("selected", "mode")):
            r.error(400, "Missing selection parameters", next=select_url)

        # Save ready-scripts
        jquery_ready = s3.jquery_ready
        s3.jquery_ready = []

        # Form to choose dates of completion
        form_name = "%s-complete" % table
        form = self.form(form_name)
        form["_action"] = r.url(representation="", vars=get_vars)

        # Capture injected JS, and restore ready-scripts
        injected = s3.jquery_ready
        s3.jquery_ready = jquery_ready

        output = None
        if r.ajax or r.representation == "json":
            # Dialog request
            # => generate a JSON object with form and control script

            # Form control script
            script = '''
(function() {
    $(function() {
        %s
    });
    const s = $('input[name="start_date"]'),
          e = $('input[name="end_date"]'),
          c = $('input[name="complete_confirm"]'),
          b = $('.complete-submit'),
          toggle = function() {
              b.prop('disabled', !c.prop('checked') || !s.val() || !e.val());
          };
    s.add(e).off('.complete').on('change.complete', toggle);
    c.off('.complete').on('click.complete', toggle);
    }
)();''' % ("\n".join(injected))

            dialog = TAG[""](form, SCRIPT(script, _type='text/javascript'))

            current.response.headers["Content-Type"] = "application/json"
            output = json.dumps({"dialog": dialog.xml().decode("utf-8")})

        elif form.accepts(r.vars, current.session, formname=form_name):
            # Dialog submission
            # => process the form, set up, authorize and perform the action

            T = current.T
            pkey = table._id.name
            post_vars = r.post_vars

            try:
                record_ids = self.selected_set(resource, post_vars)
            except SyntaxError:
                r.error(400, "Invalid select mode", next=select_url)
            total_selected = len(record_ids)

            # Verify permission for all selected record
            query = (table._id.belongs(record_ids)) & \
                    (table._id.belongs(self.permitted_set(table, record_ids)))
            permitted = current.db(query).select(table._id)
            denied = len(record_ids) - len(permitted)
            if denied > 0:
                record_ids = {row[pkey] for row in permitted}

            # Read the selected date/time interval
            form_vars = form.vars
            start = form_vars.get("start_date")
            end = form_vars.get("end_date")

            # Mark the appointments as completed
            completed, failed = self.mark_completed(record_ids, start, end)
            success = bool(completed)

            # Build confirmation/error message
            msg = T("%(number)s appointments closed") % {"number": completed}
            failures = []

            failed += denied
            already_closed = total_selected - completed - failed
            if already_closed:
                failures.append(T("%(number)s already closed") % {"number": already_closed})
            if failed:
                failures.append(T("%(number)s failed") % {"number": failed})
            if failures:
                failures = "(%s)" % (", ".join(str(f) for f in failures))
                msg = "%s %s" % (msg, failures)

            if success:
                current.session.confirmation = msg
            else:
                current.session.warning = msg
            redirect(select_url)
        else:
            r.error(400, current.ERROR.BAD_REQUEST, next=select_url)

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def form(cls, form_name):
        """
            Produces the form to select closure status and confirm the action

            Args:
                form_name: the form name (for CSRF protection)

            Returns:
                the FORM
        """

        T = current.T
        tablename = "dvr_case_appointment"

        # Info text and confirmation question
        INFO = T("The selected appointments will be marked as completed.")
        CONFIRM = T("Are you sure you want to mark these appointments as completed?")

        # Default values for date/time interval
        now = current.request.utcnow.replace(microsecond=0)
        start = S3DateTime.datetime_represent(now - datetime.timedelta(hours=1), utc=True)
        end = S3DateTime.datetime_represent(now, utc=True)

        # Date/time interval inputs
        field = Field("start_date", "datetime",
                      requires = IS_UTC_DATETIME(maximum=now),
                      )
        field.tablename = tablename
        widget = S3CalendarWidget(set_min = "#dvr_case_appointment_end_date",
                                  timepicker = True,
                                  future = 0,
                                  )
        start_date_input = widget(field, start)

        field = Field("end_date", "datetime",
                      requires = IS_UTC_DATETIME(maximum=now),
                      )
        field.tablename = tablename
        widget = S3CalendarWidget(set_max = "#dvr_case_appointment_start_date",
                                  timepicker = True,
                                  future = 0,
                                  )
        end_date_input = widget(field, end)

        # Build the form
        components = [P(INFO, _class="checkout-info"),
                      LABEL("%s:" % T("Start Date"),
                            DIV(start_date_input,
                                _class="controls",
                                ),
                            _class="label-above",
                            ),
                      LABEL("%s:" % T("End Date"),
                            DIV(end_date_input,
                                _class="controls",
                                ),
                            _class="label-above",
                            ),
                      P(CONFIRM, _class="complete-question"),
                      LABEL(INPUT(value = "complete_confirm",
                                  _name = "complete_confirm",
                                  _type = "checkbox",
                                  ),
                            T("Yes, mark the selected appointments as completed"),
                            _class = "complete-confirm label-inline",
                            ),
                      DIV(BUTTON(T("Submit"),
                                 _class = "small alert button complete-submit",
                                 _disabled = "disabled",
                                 _type = "submit",
                                 ),
                          A(T("Cancel"),
                            _class = "cancel-form-btn action-lnk complete-cancel",
                            _href = "javascript:void(0)",
                            ),
                          _class = "checkout-buttons",
                          ),
                      ]

        form = FORM(*[DIV(c, _class="form-row row") for c in components],
                    hidden = {"_formkey": FormKey(form_name).generate(),
                              "_formname": form_name,
                              },
                    _class = "bulk-complete-form",
                    )

        return form

    # -------------------------------------------------------------------------
    @staticmethod
    def selected_set(resource, post_vars):
        """
            Determine the selected persons from select-parameters

            Args:
                resource: the pre-filtered CRUDResource (dvr_case_appointment)
                post_vars: the POST vars containing the select-parameters

            Returns:
                set of dvr_case_appointment.id
        """

        pkey = resource.table._id.name

        # Selected records
        selected_ids = post_vars.get("selected", [])
        if isinstance(selected_ids, str):
            selected_ids = {item for item in selected_ids.split(",") if item.strip()}
        query = FS(pkey).belongs(selected_ids)

        # Selection mode
        mode = post_vars.get("mode")
        if mode == "Exclusive":
            query = ~query if selected_ids else None
        elif mode != "Inclusive":
            raise SyntaxError

        # Get all matching record IDs
        if query is not None:
            resource.add_filter(query)
        rows = resource.select([pkey], as_rows=True)

        return {row[pkey] for row in rows}

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted_set(table, selected_set):
        """
            Produces a sub-query of appointments the user is permitted to
            mark as completed.

            Args:
                table: the target table (dvr_case_appointment)
                selected_set: sub-query for permitted appointments

            Returns:
                SQL
        """

        db = current.db

        # All records in the selected set the user can update
        query = (table._id.belongs(selected_set)) & \
                current.auth.s3_accessible_query("update", table)

        return db(query)._select(table.id)

    # -------------------------------------------------------------------------
    @staticmethod
    def mark_completed(appointment_ids, start_date, end_date):
        """
            Sets the status of the selected appointments to completed (4)

            Args:
                appointment_ids: the record IDs of the selected appointments
                start_date: the start date/time of the appointments
                end_date: the end date/time of the appointments

            Returns:
                tuple (number_completed, number_failed)
        """

        db = current.db
        s3db = current.s3db

        table = s3db.dvr_case_appointment

        if end_date < start_date:
            start_date, end_date = end_date, start_date

        # Determine which appointments should be marked completed
        # - only one appointment per type and client
        query = (table.id.belongs(appointment_ids)) & \
                (table.status.belongs((1, 2, 3))) & \
                (table.deleted == False)
        last = table.id.max()
        rows = db(query).select(last,
                                table.person_id,
                                table.type_id,
                                groupby = (table.person_id, table.type_id),
                                )
        actionable = {row[last] for row in rows}

        # Look up duplicates within the selected set
        # - those will be marked as "not required" and undated instead
        query = (table.id.belongs(appointment_ids)) & \
                (~(table.id.belongs(actionable))) & \
                (table.status.belongs((1, 2, 3))) & \
                (table.deleted == False)
        rows = db(query).select(table.id)
        duplicates = {row.id for row in rows}

        audit = current.audit
        onaccept = s3db.onaccept

        # Mark duplicates as "not required"
        data = Storage(start_date=None, end_date=None, status=7)
        completed = db(table.id.belongs(duplicates)).update(**data)
        for appointment_id in duplicates:
            audit("update", "dvr", "case_appointment",
                  form = Storage(vars=data),
                  record = appointment_id,
                  representation = "html",
                  )

        # Mark actionables as "completed"
        data = Storage(start_date=start_date, end_date=end_date, status=4)
        completed += db(table.id.belongs(actionable)).update(**data)
        for appointment_id in actionable:
            # Onaccept to update last_seen_on
            record = Storage(data)
            record["id"] = appointment_id
            onaccept(table, record, method="update")
            audit("update", "dvr", "case_appointment",
                  form = Storage(vars=data),
                  record = appointment_id,
                  representation = "html",
                  )

        # Calculate failed (should be 0)
        failed = len(actionable) + len(duplicates) - completed

        return completed, failed

# END =========================================================================
