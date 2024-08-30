"""
    MRCMS Presence List

    License: MIT
"""

import json
import datetime

from dateutil.relativedelta import relativedelta

from gluon import current, SQLFORM, BUTTON, DIV
from gluon.contenttype import contenttype
from gluon.serializers import json as jsons
from gluon.storage import Storage
from gluon.streamer import DEFAULT_CHUNK_SIZE

from s3dal import Field

from core import CRUDMethod, CustomController, DateField, FormKey, \
                 ICON, IS_ONE_OF, IS_ONE_OF_EMPTY, JSONERRORS, JSONSEPARATORS, \
                 S3DateTime, XLSXWriter, s3_decode_iso_datetime, s3_str

# =============================================================================
class BaseReport(CRUDMethod):

    report_type = "base"

    def apply_method(self, r, **attr):
        """
            Entry point for the CRUDController

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        output = None

        if r.http == "GET":
            output = self.form(r, **attr)

        elif r.http == "POST":
            fmt = r.representation
            if r.ajax or fmt == "json":
                output = self.json(r, **attr)
            elif fmt == "xlsx":
                output = self.xlsx(r, **attr)
            else:
                r.error(415, current.ERROR.BAD_FORMAT)

        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    @property
    def report_title(self):
        """ A title for this report """

        return current.T("Report")

    # -------------------------------------------------------------------------
    def form(self, r, **attr):
        """
            Renders the dialog for the presence report

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                HTML page
        """

        T = current.T

        report_name = "%s_report" % self.report_type

        formfields = self.formfields(r, report_name)

        # Hidden inputs
        hidden = {"formkey": FormKey(report_name).generate(),
                  }

        # Form buttons
        buttons = [BUTTON(T("Show Report"),
                          _type = "button",
                          _class = "small primary button update-report-btn",
                          ),
                   BUTTON(ICON("file-xls"), T("Download Report"),
                          _type = "button",
                          _class = "small activity button download-report-btn",
                          ),
                   ]

        # IDs for form and table container
        widget_id = "%s-report" % self.report_type
        container_id = "%s-data" % widget_id

        # Form
        formstyle = current.deployment_settings.get_ui_filter_formstyle()
        form = SQLFORM.factory(*formfields,
                               record = None,
                               showid = False,
                               formstyle = formstyle,
                               table_name = report_name,
                               hidden = hidden,
                               buttons = buttons,
                               _id = widget_id,
                               _class = "table-report-form",
                               )
        output = {"title": self.report_title,
                  "form": form,
                  "items": DIV(_class = "table-report-data",
                               _id = "%s-data" % widget_id,
                               ),
                  }

        # Inject client-side script to retrieve and render the data
        script_opts = {"ajaxURL": r.url(representation="json"),
                       "xlsxURL": r.url(representation="xlsx"),
                       "tableContainer": container_id,
                       }
        self.inject_script(widget_id, script_opts)

        # Set view template
        CustomController._view("MRCMS", "tblreport.html")

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def formfields(cls, r, report_name):
        """
            Returns the fields for the report form

            Args:
                r: the CRUDRequest
                report_name: the report name (pseudo-table name) for the XSRF token

            Returns:
                list of Fields
        """

        T = current.T

        db = current.db
        s3db = current.s3db

        # Determine selectable organisations
        org_ids = cls.permitted_orgs()
        if not org_ids:
            r.unauthorised()
        default_org = org_ids[0] if len(org_ids) == 1 else None

        # Date interval
        today = current.request.utcnow.date()
        one_month_ago = today - relativedelta(months=1)

        # Create form to select organisation and date
        ctable = s3db.dvr_case
        rtable = s3db.cr_shelter_registration
        otable = s3db.org_organisation
        dbset = db(otable.id.belongs(org_ids))
        formfields = [Field("organisation_id",
                            label = T("Organization"),
                            requires = IS_ONE_OF(dbset, "org_organisation.id",
                                                 ctable.organisation_id.represent,
                                                 ),
                            default = default_org,
                            ),
                      Field("shelter_id",
                            label = T("Shelter"),
                            requires = IS_ONE_OF_EMPTY(db, "cr_shelter.id",
                                                       rtable.shelter_id.represent,
                                                       ),
                            ),
                      DateField("start_date",
                                label = T("From Date"),
                                default = one_month_ago,
                                future = 0,
                                set_min = "#%s_end_date" % report_name,
                                ),
                      DateField("end_date",
                                label = T("Until Date"),
                                default = today,
                                future = 0,
                                set_max = "#%s_start_date" % report_name,
                                ),
                      ]

        # Filter shelter list to just those for organisation
        options = {"trigger": "organisation_id",
                   "target": "shelter_id",
                   "lookupPrefix": "cr",
                   "lookupResource": "shelter",
                   "showEmptyField": False,
                   "optional": True,
                   }
        jquery_ready = current.response.s3.jquery_ready
        jquery_ready.append('''$.filterOptionsS3(%s)''' % \
                            json.dumps(options, separators=JSONSEPARATORS))

        return formfields

    # -------------------------------------------------------------------------
    @staticmethod
    def parameters(r, report_name):
        """
            Extracts the report parameters from the request body and verifies
            the XSRF token

            Args:
                r: the CRUDRequest
                report_name: the report name (pseudo-table name) for the XSRF token

            Returns:
                a tuple (organisation_id, shelter_id, start_date, end_date)

            Raises:
                HTTP400 for invalid request parameters
        """

        # Read+parse body JSON
        s = r.body
        s.seek(0)
        try:
            options = json.load(s)
        except JSONERRORS:
            options = None
        if not isinstance(options, dict):
            r.error(400, "Invalid request parameters")

        # Verify submitted form key against session (CSRF protection)
        formkey = FormKey(report_name)
        if not formkey.verify(options, invalidate=False):
            r.error(400, "Invalid action key (form reopened in another tab?)")

        # Extract organisation ID
        organisation = options.get("organisation")
        try:
            organisation_id = int(organisation)
        except (ValueError, TypeError):
            r.error(400, "Invalid or missing organisation parameter")

        # Extract report reference date
        dtstr = options.get("start_date")
        start_date = s3_decode_iso_datetime(dtstr)
        if not start_date:
            r.error(400, "Invalid or missing start date parameter")

        dtstr = options.get("end_date")
        end_date = s3_decode_iso_datetime(dtstr)
        if not end_date:
            r.error(400, "Invalid or missing end_date parameter")

        # Extract shelter ID
        shelter = options.get("shelter")
        if shelter:
            try:
                shelter_id = int(shelter)
            except (ValueError, TypeError):
                r.error(400, "Invalid shelter parameter")
        else:
            shelter_id = None

        return organisation_id, shelter_id, start_date, end_date

    # -------------------------------------------------------------------------
    def json(self, r, **attr):
        """
            Returns the report as JSON object; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a JSON object to construct a table like:
                    {"labels": [label, label, ...],
                     "rows": [[value, value, ...], ...]
                     "results": number
                     }
        """

        raise NotImplementedError()

    # -------------------------------------------------------------------------
    def xlsx(self, r, **attr):
        """
            Returns the report as XLSX file; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a XLSX file
        """

        raise NotImplementedError()

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted_orgs():
        """
            Returns the organisations the user is permitted to generate
            the report for; to be adapted by subclass

            Returns:
                List of organisation IDs
        """

        from .helpers import permitted_orgs
        return permitted_orgs("read", "dvr_case")

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
        script = "/%s/static/themes/JUH/js/tblreport.js" % request.application
        scripts = s3.scripts
        if script not in scripts:
            scripts.append(script)

        # Widget options
        opts = {"labelNoData": s3_str(current.T("No records found"))}
        if options:
            opts.update(options)

        # Widget instantiation
        script = '''$('#%(widget_id)s').tableReport(%(options)s)''' % \
                 {"widget_id": widget_id, "options": json.dumps(opts)}
        jquery_ready = s3.jquery_ready
        if script not in jquery_ready:
            jquery_ready.append(script)

# =============================================================================
class PresenceReport(BaseReport):
    """ Report over the last sightings of clients """

    report_type = "presence"

    # -------------------------------------------------------------------------
    @property
    def report_title(self):
        """ A title for this report """

        return current.T("Presence Report")

    # -------------------------------------------------------------------------
    def json(self, r, **attr):
        """
            Returns the report as JSON object; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a JSON object to construct a table like:
                    {"labels": [label, label, ...],
                     "rows": [[value, value, ...], ...]
                     "results": number
                     }
        """

        T = current.T

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, shelter_id, start_date, end_date)

        extension = [("presence.event_type", T("Last Presence Registration")),
                     ("presence.shelter_name", T("Place")),
                     ("presence.date", T("Date")),
                     ("checkpoint.event_type", T("Last Checkpoint")),
                     ("checkpoint.date", T("Date")),
                     ]

        # Produce a JSON {results: 0, labels: [], records: [[]]}
        rfields = data.rfields
        labels = [rfield.label for rfield in rfields if rfield.ftype != "id"] + \
                 [item[1] for item in extension]

        records = []
        for row in data.rows:
            record = [row[rfield.colname] for rfield in rfields if rfield.ftype != "id"] + \
                     [row[item[0]] for item in extension]
            records.append(record)

        output = {"labels": labels,
                  "records": records,
                  "results": max(0, len(records)),
                  }

        # Set Content Type
        current.response.headers["Content-Type"] = "application/json"

        return jsons(output)

    # -------------------------------------------------------------------------
    def xlsx(self, r, **attr):
        """
            Returns the report as XLSX file; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a XLSX file
        """

        T = current.T

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, shelter_id, start_date, end_date)

        # Prepare the input for XLSXWriter
        table_data = {"columns": [],
                      "headers": {},
                      "types": {},
                      "rows": data.rows,
                      }

        rfields = data.rfields
        for rfield in rfields:
            table_data["columns"].append(rfield.colname)
            table_data["headers"][rfield.colname] = rfield.label
            table_data["types"][rfield.colname] = str(rfield.ftype)

        extension = [("presence.event_type", T("Last Presence Registration"), "string"),
                     ("presence.shelter_name", T("Place"), "string"),
                     ("presence.date", T("Date"), "datetime"),
                     ("checkpoint.event_type", T("Last Checkpoint"), "string"),
                     ("checkpoint.date", T("Date"), "datetime"),
                     ]
        for colname, label, ftype in extension:
            table_data["columns"].append(colname)
            table_data["headers"][colname] = label
            table_data["types"][colname] = str(ftype)

        # Use a title row (also includes exported-date)
        current.deployment_settings.base.xls_title_row = True
        title = "%s %s -- %s" % (self.report_title,
                                 S3DateTime.date_represent(start_date, utc=True),
                                 S3DateTime.date_represent(end_date, utc=True),
                                 )

        # Generate XLSX byte stream
        output = XLSXWriter.encode(table_data,
                                   title = title,
                                   sheet_title = self.report_title,
                                   as_stream = True,
                                   )

        # Set response headers
        filename = "presence_report_%s_%s" % (start_date.strftime("%Y%m%d"),
                                              end_date.strftime("%Y%m%d"),
                                              )
        disposition = "attachment; filename=\"%s\"" % filename
        response = current.response
        response.headers["Content-Type"] = contenttype(".xlsx")
        response.headers["Content-disposition"] = disposition

        # Return stream response
        return response.stream(output,
                               chunk_size = DEFAULT_CHUNK_SIZE,
                               request = current.request
                               )

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted(organisation_id=None):
        """
            Checks if the user is permitted to access relevant case
            and event data of the organisation

            Args:
                organisation_id: the organisation record ID

            Returns:
                boolean
        """

        # Determine the target realm
        pe_id = current.s3db.pr_get_pe_id("org_organisation", organisation_id) \
                if organisation_id else None

        permitted = True
        permitted_realms = current.auth.permission.permitted_realms

        # Check permissions for this realm
        realms = permitted_realms("dvr_case")
        if realms is not None:
            permitted = permitted and (pe_id is None or pe_id in realms)

        realms = permitted_realms("org_site_presence_event")
        if realms is not None:
            permitted = permitted and (pe_id is None or pe_id in realms)

        realms = permitted_realms("dvr_case_event")
        if realms is not None:
            permitted = permitted and (pe_id is None or pe_id in realms)

        return permitted

    # -------------------------------------------------------------------------
    @classmethod
    def extract(cls, organisation_id, shelter_id, start_date, end_date):
        """
            Looks up the case data and last sighting events for all residents
            of a shelter that were checked-in there during the date interval;
            if no shelter is specified, all cases of the organisation during
            that interval are looked at instead

            Args:
                organisation_id: the organisation record ID
                shelter_id: the shelter record ID (optional)
                start_date: start date of the interval
                end_date: end date of the interval

            Returns:
                ResourceData instance (pr_person) with extended rows
        """

        T = current.T

        db = current.db
        s3db = current.s3db

        # Fields to extract for cases
        list_fields = ["id",
                       (T("ID"), "pe_label"),
                       (T("Principal Ref.No."), "dvr_case.reference"),
                       "last_name",
                       "first_name",
                       "date_of_birth",
                       "person_details.nationality",
                       ]

        stable = s3db.cr_shelter
        if shelter_id:
            # Limit to residents of this shelter
            person_ids = cls.residents(shelter_id, start_date, end_date)

            # Include only presence events at this shelter
            squery = (stable.id == shelter_id)
        else:
            # All persons with open cases with this organisation
            table = s3db.dvr_case
            query = (table.organisation_id == organisation_id) & \
                    ((table.date == None) | (table.date <= end_date)) & \
                    ((table.closed_on == None) | (table.closed_on >= start_date)) & \
                    (table.archived == False) & \
                    (table.deleted == False)
            person_ids = db(query)._select(table.person_id)

            # Include presence events at any shelter of the organisation
            squery = (stable.organisation_id == organisation_id) & \
                     (stable.deleted == False)

        # Retrieve the case data
        resource = s3db.resource("pr_person")
        resource.add_filter(resource.table._id.belongs(person_ids))
        persons = resource.select(list_fields,
                                  represent = True,
                                  raw_data = True,
                                  orderby = "pr_person.last_name,pr_person.first_name",
                                  )

        # All relevant shelters
        site_ids = db(squery)._select(stable.site_id)

        # Last presence events for the persons at any of these shelters
        presence_events = cls.last_site_presence_event(person_ids,
                                                       site_ids,
                                                       start_date,
                                                       end_date,
                                                       )

        # Last checkpoint events for the persons with this organisation
        case_events = cls.last_case_event(person_ids,
                                          organisation_id,
                                          start_date,
                                          end_date,
                                          )

        # Representation functions
        ptable = s3db.org_site_presence_event
        date_represent = ptable.date.represent
        move_represent = ptable.event_type.represent

        # Add last events to the person rows
        for row in persons.rows:

            person_id = row._row["pr_person.id"]

            # Presence events
            event = presence_events.get(person_id)
            if event:
                details = {"presence.event_type": move_represent(event["event_type"]),
                           "presence.shelter_name": event["shelter_name"],
                           "presence.date": date_represent(event["date"]),
                           }
            else:
                details = {"presence.event_type": "-",
                           "presence.shelter_name": "-",
                           "presence.date": "-",
                           }
            row.update(details)

            # Checkpoint events
            event = case_events.get(person_id)
            if event:
                details = {"checkpoint.event_type": event["type_name"],
                           "checkpoint.date": date_represent(event["date"]),
                           }
            else:
                details = {"checkpoint.event_type": "-",
                           "checkpoint.date": "-",
                           }
            row.update(details)

        return persons

    # -------------------------------------------------------------------------
    @staticmethod
    def residents(shelter_id, start_date, end_date):
        """
            Looks up the person_ids of all residents who were checked-in
            at the specified shelter at any time during the date interval

            Args:
                shelter_id: the shelter record ID
                start_date: start date of the interval
                end_date: end date of the interval

            Returns:
                a set of person IDs
        """

        db = current.db
        s3db = current.s3db

        table = s3db.cr_shelter_registration_history

        # All residents checked-in to or checked-out from this shelter
        # during the interval
        query = (table.shelter_id == shelter_id) & \
                (table.status.belongs(2, 3)) & \
                (table.date >= start_date) & \
                (table.date <= end_date) & \
                (table.deleted == False)
        rows = db(query).select(table.person_id, distinct=True)
        person_ids = {row.person_id for row in rows}

        # Other residents that have been checked-in to this shelter
        # before the interval
        query = (table.shelter_id == shelter_id) & \
                (table.status == 2) & \
                (table.date < start_date) & \
                (~(table.person_id.belongs(person_ids))) & \
                (table.deleted == False)
        previous = db(query)._select(table.person_id, distinct=True)

        # Dates of the last status change for each of those previous residents
        query = (table.person_id.belongs(previous)) & \
                (table.date < start_date) & \
                (table.deleted == False)
        last_event = db(query).nested_select(table.person_id,
                                             table.date.max().with_alias("max_date"),
                                             groupby = table.person_id,
                                             ).with_alias("last_event")

        # Any history entries matching these dates that represent check-ins
        # to this shelter (i.e. all residents that were last checked-in to this
        # shelter before start_date)
        join = last_event.on((last_event.person_id == table.person_id) & \
                             (last_event.max_date == table.date))
        query = (table.shelter_id == shelter_id) & \
                (table.status == 2)
        rows = db(table.id > 0).select(table.person_id, join=join, distinct=True)
        person_ids |= {row.person_id for row in rows}

        return person_ids

    # -------------------------------------------------------------------------
    @staticmethod
    def last_case_event(person_ids, organisation_id, start_date, end_date):
        """
            Retrieves the last case events (checkpoint events) during the date
            interval, where personal presence was required

            Args:
                person_ids: the person record IDs of the relevant clients
                organisation_id: the organisation defining the event types
                start_date: start date of the interval
                end_date: end_date of the interval

            Returns:
                a dict {person_id: dvr_case_event Row}, with the
                event Row extended with type_name
        """

        db = current.db
        s3db = current.s3db

        # Relevant event types (those which require presence)
        ttable = s3db.dvr_case_event_type
        query = (ttable.organisation_id == organisation_id) & \
                (ttable.presence_required == True) & \
                (ttable.deleted == False)
        event_types = db(query)._select(ttable.id)

        # The dates of the last relevant event for each person on or before
        # the given date (subselect)
        table = s3db.dvr_case_event
        query = (table.person_id.belongs(person_ids)) & \
                (table.type_id.belongs(event_types)) & \
                (table.date >= start_date) & \
                (table.date <= end_date) & \
                (table.deleted == False)
        last_event = db(query).nested_select(table.person_id,
                                             table.date.max().with_alias("max_date"),
                                             groupby = table.person_id,
                                             ).with_alias("last_event")

        # The events corresponding to these dates
        join = last_event.on((last_event.person_id == table.person_id) & \
                             (last_event.max_date == table.date))
        events = db(table.id > 0).select(table.person_id,
                                         table.type_id,
                                         table.date,
                                         join = join,
                                         )

        # Event type names (for representation)
        type_ids = {event.type_id for event in events}
        types = db(ttable.id.belongs(type_ids)).select(ttable.id,
                                                       ttable.name,
                                                       ).as_dict()

        # Build the result dict
        result = {}
        for event in events:
            event_type = types.get(event.type_id)
            event.type_name = event_type["name"] if event_type else None
            result[event.person_id] = event

        return result

    # -------------------------------------------------------------------------
    @staticmethod
    def last_site_presence_event(person_ids, site_ids, start_date, end_date):
        """
            Retrieves the last site presence events (IN|OUT) at any of the
            specified sites during the date interval

            Args:
                person_ids: the person record IDs of the relevant clients
                site_ids: the site_id of the relevant shelters
                start_date: start date of the interval
                end_date: end date of the interval

            Returns:
                a dict {person_id: org_site_presence_event Row}, with
                the event Row extended with shelter_id and shelter_name
        """

        db = current.db
        s3db = current.s3db

        # The dates of the last IN|OUT events for each person on or before
        # the given date (subselect)
        table = s3db.org_site_presence_event
        query = (table.person_id.belongs(person_ids)) & \
                (table.site_id.belongs(site_ids)) & \
                (table.event_type.belongs(("IN", "OUT"))) & \
                (table.date >= start_date) & \
                (table.date <= end_date) & \
                (table.deleted == False)
        last_event = db(query).nested_select(table.person_id,
                                             table.date.max().with_alias("max_date"),
                                             groupby = table.person_id,
                                             ).with_alias("last_event")

        # The actual presence events
        join = last_event.on((last_event.person_id == table.person_id) & \
                             (last_event.max_date == table.date))
        events = db(table.id > 0).select(table.person_id,
                                         table.site_id,
                                         table.event_type,
                                         table.date,
                                         join=join,
                                         )

        # Site names (for representation)
        stable = s3db.cr_shelter
        shelter_site_ids = {event.site_id for event in events}
        shelters = db(stable.site_id.belongs(shelter_site_ids)).select(stable.id,
                                                                       stable.site_id,
                                                                       stable.name,
                                                                       ).as_dict(key="site_id")

        # Build the result
        result = {}
        for event in events:
            shelter = shelters.get(event.site_id)
            if shelter:
                event.shelter_id = shelter["id"]
                event.shelter_name = shelter["name"]
            else:
                event.shelter_id = event.shelter_name = None
            result[event.person_id] = event

        return result

# =============================================================================
class MealsReport(BaseReport):
    """ Report over distributed meals """

    report_type = "meals"

    # -------------------------------------------------------------------------
    @property
    def report_title(self):
        """ A title for this report """

        return current.T("Food Distribution")

    # -------------------------------------------------------------------------
    def json(self, r, **attr):
        """
            Returns the report as JSON object; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a JSON object to construct a table like:
                    {"labels": [label, label, ...],
                     "rows": [[value, value, ...], ...]
                     "results": number
                     }
        """

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, shelter_id, start_date, end_date)

        columns = data["columns"]
        headers = data["headers"]
        labels = [headers[colname] for colname in columns]

        records, rows = [], data["rows"]
        for row in rows:
            records.append([s3_str(row[colname]) if row[colname] is not None else "" for colname in columns])


        output = {"labels": labels,
                  "records": records,
                  "results": max(0, len(records) - 1),
                  }

        # Set Content Type
        current.response.headers["Content-Type"] = "application/json"

        return jsons(output)

    # -------------------------------------------------------------------------
    def xlsx(self, r, **attr):
        """
            Returns the report as XLSX file; to be implemented by subclass

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a XLSX file
        """

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, shelter_id, start_date, end_date)

        # Use a title row (also includes exported-date)
        current.deployment_settings.base.xls_title_row = True
        title = "%s %s -- %s" % (self.report_title,
                                 S3DateTime.date_represent(start_date, utc=True),
                                 S3DateTime.date_represent(end_date, utc=True),
                                 )

        # Generate XLSX byte stream
        output = XLSXWriter.encode(data,
                                   title = title,
                                   sheet_title = self.report_title,
                                   as_stream = True,
                                   )

        # Set response headers
        filename = "meals_report_%s_%s" % (start_date.strftime("%Y%m%d"),
                                           end_date.strftime("%Y%m%d"),
                                           )
        disposition = "attachment; filename=\"%s\"" % filename
        response = current.response
        response.headers["Content-Type"] = contenttype(".xlsx")
        response.headers["Content-disposition"] = disposition

        # Return stream response
        return response.stream(output,
                               chunk_size = DEFAULT_CHUNK_SIZE,
                               request = current.request
                               )

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted_orgs():
        """
            Returns the organisations the user is permitted to generate
            the report for

            Returns:
                List of organisation IDs
        """

        from .helpers import permitted_orgs
        return permitted_orgs("read", "dvr_case_event")

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted(organisation_id=None):
        """
            Checks if the user is permitted to access relevant case
            and event data of the organisation

            Args:
                organisation_id: the organisation record ID

            Returns:
                boolean
        """

        # Determine the target realm
        pe_id = current.s3db.pr_get_pe_id("org_organisation", organisation_id) \
                if organisation_id else None

        permitted = True
        permitted_realms = current.auth.permission.permitted_realms

        # Check permissions for this realm
        realms = permitted_realms("dvr_case_event")
        if realms is not None:
            permitted = permitted and (pe_id is None or pe_id in realms)

        return permitted

    # -------------------------------------------------------------------------
    @classmethod
    def extract(cls, organisation_id, shelter_id, start_date, end_date):
        """
            Looks up the number of distributed meals per day and type,
            including totals per day and per type

            Args:
                organisation_id: the organisation record ID
                shelter_id: the shelter record ID (optional)
                start_date: start date of the interval
                end_date: end date of the interval

            Returns:
                a dict of results like:
                {"columns": [colname, ...],
                 "headers": {colname: label, ...},
                 "types": {colname: datatype, ...},
                 "rows": [{colname: value, ...}, ...]
                 }
        """

        T = current.T

        db = current.db
        s3db = current.s3db

        # Relevant report types
        ttable = s3db.dvr_case_event_type
        query = (ttable.organisation_id == organisation_id) & \
                (ttable.event_class == "F") & \
                (ttable.deleted == False)
        rows = db(query).select(ttable.id, ttable.name, orderby=ttable.id)
        event_types = {row.id: row.name for row in rows}

        # Initialize table model
        columns = ["dow", "date", "total"]
        headers = {"dow": T("Day"), "date": T("Date"), "total": T("Total")}
        types = {"dow": "string", "date": "date", "total": "integer"}
        totals = {"dow": T("Total"), "date": ""}
        grand_total = 0

        # Add event type columns and totals
        for row in rows:
            key = "meal%s" % row.id
            columns.insert(-1, key)
            headers[key] = T(row.name)
            types[key] = "integer"
            totals[key] = 0

        # Local dates for selected interval
        local_date = lambda dt: S3DateTime.to_local(dt).date()
        start, end = local_date(start_date), local_date(end_date)
        if start > end:
            start, end = end, start

        # Days of week representation
        # TODO move into tools/represent
        days_of_week = {1: T("Mon##weekday"),
                        2: T("Tue##weekday"),
                        3: T("Wed##weekday"),
                        4: T("Thu##weekday"),
                        5: T("Fri##weekday"),
                        6: T("Sat##weekday"),
                        7: T("Sun##weekday"),
                        }

        # Initialize results
        dates, results = [], {}
        date = start
        while date <= end:

            dates.append(date.isoformat())

            record = {"meal%s" % type_id: 0 for type_id in event_types}
            record["dow"] = s3_str(days_of_week.get(date.isoweekday()))
            record["date"] = S3DateTime.date_represent(date, utc=False)
            record["total"] = 0

            results[date.isoformat()] = record

            date += relativedelta(days=1)

        # Expression for local date of event
        table = s3db.dvr_case_event
        offset = S3DateTime.get_utc_offset()
        day = (table.date + (offset + ":00")).cast("date") if offset else table.date.cast("date")

        # Expression for total number of events
        number = table.id.count(distinct=True)

        # Extract data
        query = (table.type_id.belongs(set(event_types))) & \
                (table.date >= start_date) & \
                (table.date < end_date) & \
                (table.deleted == False)
        if shelter_id:
            # Include only events where the client was a checked-in resident
            # of that shelter at the time of the event
            # Note: the join could result in multiple rows per event, so
            #       must count distinct events in number-expression
            registration = cls.registrations(shelter_id, start_date, end_date)
            join = registration.on((registration.person_id == table.person_id) & \
                                   (registration.date <= table.date) & \
                                   ((registration.end_date == None) | \
                                    (registration.end_date >= table.date)))
        else:
            # Include all events
            join = None

        rows = db(query).select(day,
                                table.type_id,
                                number,
                                join = join,
                                groupby = (day, table.type_id),
                                )
        # Process the data
        for row in rows:

            date = row[day].isoformat()

            # Get the result record
            record = results.get(date)
            if not record:
                continue

            type_id = row[table.type_id]
            value = row[number]

            key = "meal%s" % type_id
            if key in record:
                # Update the record
                record[key] += value
                record["total"] += value
                # Update the totals
                totals[key] += value
                grand_total += value

        totals["total"] = grand_total
        rows = [results[date] for date in dates]
        rows.append(totals)

        return {"columns": columns,
                "headers": headers,
                "types": types,
                "rows": rows,
                }

    # -------------------------------------------------------------------------
    @staticmethod
    def registrations(shelter_id, start_date, end_date):
        """
            Constructs a nested select of shelter registration history entries
            at the shelter with end dates

            Args:
                shelter_id: the cr_shelter record ID
                start_date: the start of the date interval
                end_date: the end of of the date interval

            Returns:
                a subselect with alias "registration", with fields
                person_id, date and end_date
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history
        ntable = rtable.with_alias("next_event")

        # Select all check-in events at this shelter before end_date
        query = (rtable.shelter_id == shelter_id) & \
                (rtable.status == 2) & \
                (rtable.date <= end_date) & \
                (rtable.deleted == False)

        # Left join any subsequent events for the same person
        # with change of shelter or status before end_date
        left = ntable.on((ntable.person_id == rtable.person_id) & \
                         (ntable.date > rtable.date) & \
                         (ntable.date <= end_date) & \
                         ((ntable.status.belongs((1,3))  | \
                          (ntable.shelter_id != rtable.shelter_id))) & \
                         (ntable.deleted == False))

        # Select the earliest next-event date as end_date (could be None)
        registrations = db(query).nested_select(rtable.person_id,
                                                rtable.date,
                                                ntable.date.min().with_alias("end_date"),
                                                left = left,
                                                groupby = (rtable.person_id, rtable.date),
                                                orderby = (rtable.person_id, rtable.date),
                                                ).with_alias("registration")

        return registrations

# =============================================================================
class ArrivalsDeparturesReport(BaseReport):
    """ Report over newly arrived/departed shelter residents """

    report_type = "aandd"

    # -------------------------------------------------------------------------
    @property
    def report_title(self):
        """ A title for this report """

        return current.T("Arrivals and Departures##shelter")

    # -------------------------------------------------------------------------
    def json(self, r, **attr):
        """
            Returns the report as JSON object

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                an array of JSON objects to construct a table like:
                    [{"labels": [label, label, ...],
                      "rows": [[value, value, ...], ...]
                      "results": number
                      }, ...]
        """

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, shelter_id, start_date, end_date)

        output = []
        for item in data:

            columns = item["columns"]
            headers = item["headers"]
            labels = [headers[colname] for colname in columns]

            records, rows = [], item["rows"]
            for row in rows:
                records.append([s3_str(row[colname])
                                if row[colname] is not None else ""
                                for colname in columns
                                ])

            table = {"labels": labels,
                     "records": records,
                     "results": max(0, len(records)),
                     "title": item.get("title"),
                     }
            output.append(table)

        # Set Content Type
        current.response.headers["Content-Type"] = "application/json"

        return jsons(output)

    # -------------------------------------------------------------------------
    def xlsx(self, r, **attr):
        """
            Returns the report as XLSX file

            Args:
                r - the CRUDRequest
                attr - controller parameters

            Returns:
                a XLSX file
        """

        report_name = "%s_report" % self.report_type

        # Read request parameters
        organisation_id, shelter_id, start_date, end_date = self.parameters(r, report_name)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        datasets = self.extract(organisation_id, shelter_id, start_date, end_date)

        output = None
        for dataset in datasets:
            # Use a title row (also includes exported-date)
            current.deployment_settings.base.xls_title_row = True

            subtitle = dataset.get("title")
            if not subtitle:
                subtitle = self.report_title
            title = "%s %s -- %s" % (subtitle,
                                     S3DateTime.date_represent(start_date, utc=True),
                                     S3DateTime.date_represent(end_date, utc=True),
                                     )

            # Generate XLSX byte stream
            output = XLSXWriter.encode(dataset,
                                       title = title,
                                       sheet_title = subtitle,
                                       as_stream = True,
                                       append_to = output,
                                       )

        # Set response headers
        filename = "aandd_report_%s_%s" % (start_date.strftime("%Y%m%d"),
                                           end_date.strftime("%Y%m%d"),
                                           )
        disposition = "attachment; filename=\"%s\"" % filename
        response = current.response
        response.headers["Content-Type"] = contenttype(".xlsx")
        response.headers["Content-disposition"] = disposition

        # Return stream response
        return response.stream(output,
                               chunk_size = DEFAULT_CHUNK_SIZE,
                               request = current.request
                               )

    # -------------------------------------------------------------------------
    @staticmethod
    def permitted(organisation_id=None):
        """
            Checks if the user is permitted to access relevant case
            and event data of the organisation

            Args:
                organisation_id: the organisation record ID

            Returns:
                boolean
        """

        # Determine the target realm
        pe_id = current.s3db.pr_get_pe_id("org_organisation", organisation_id) \
                if organisation_id else None

        permitted = True
        permitted_realms = current.auth.permission.permitted_realms

        # Check permissions for this realm
        realms = permitted_realms("cr_shelter_registration_history")
        if realms is not None:
            permitted = permitted and (pe_id is None or pe_id in realms)

        return permitted

    # -------------------------------------------------------------------------
    @classmethod
    def extract(cls, organisation_id, shelter_id, start_date, end_date):
        """
            Extracts the data for the report

            Args:
                organisation_id: limit to cases of this organisation
                shelter_id: limit to arrivals at/departures fromthis shelter
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)

            Returns:
                a list of two dicts [arrivals, departures], like:
                {"columns": [colname, ...],
                 "headers": {colname: label, ...},
                 "types": {colname: datatype, ...},
                 "rows": [{colname: value, ...}, ...]
                 }
        """

        T = current.T
        s3db = current.s3db

        # Determine which residents were already checked-in at the start
        # of the interval
        clients = cls.clients(start_date,
                              end_date,
                              organisation_id = organisation_id,
                              )
        rows = cls.prior_check_ins(clients, start_date, shelter_id)
        checked_in_before = {row.person_id for row in rows}

        # Determine which residents were checked-in during the interval
        # and when (date of last check-in)
        clients = cls.clients(start_date,
                              end_date,
                              organisation_id = organisation_id,
                              exclude = checked_in_before,
                              )
        rows = cls.check_ins(clients, start_date, end_date, shelter_id)
        arrivals = {}
        for row in rows:
            person_id = row.person_id
            if person_id not in arrivals:
                arrivals[person_id] = row.date

        # Determine which of these residents were no longer checked-in by
        # the end of the interval, and when they departed (date of last check-out
        # after the last check-in)
        rows = cls.final_events(checked_in_before | set(arrivals.keys()),
                                start_date,
                                end_date,
                                shelter_id,
                                )
        departures = cls.check_out_dates({row.person_id for row in rows},
                                         start_date,
                                         end_date,
                                         shelter_id = shelter_id,
                                         check_in_dates = arrivals,
                                         )

        # Extract the person data for the clients
        resource = s3db.resource("pr_person",
                                 id = list(arrivals.keys()) + list(departures.keys()),
                                 )
        list_fields = ["id",
                       (T("ID"), "pe_label"),
                       (T("Principal Ref.No."), "dvr_case.reference"),
                       "last_name",
                       "first_name",
                       "date_of_birth",
                       "gender",
                       "person_details.nationality",
                       (T("BAMF Ref.No."), "bamf.value"),
                       "dvr_case.status_id",
                       "dvr_case.last_seen_on",
                       ]
        person_data = resource.select(list_fields,
                                      represent = True,
                                      raw_data = True,
                                      )
        persons = {row._row["pr_person.id"]: row for row in person_data.rows}

        # Columns for the tables
        date_col = "cr_shelter_registration_history.date"
        columns, headers, types = [date_col], {date_col: T("Date")}, {date_col: "datetime"}
        for rfield in person_data.rfields:
            if rfield.ftype == "id":
                continue
            columns.append(rfield.colname)
            headers[rfield.colname] = rfield.label
            types[rfield.colname] = str(rfield.ftype)

        # Build result
        rtable = current.s3db.cr_shelter_registration_history
        date_represent = rtable.date.represent
        sort_by_date = lambda item: item[1] if item[1] else datetime.datetime.max
        group_title = [T("Arrivals##shelter"), T("Departures##shelter")]

        output = []
        for i, group in enumerate((arrivals, departures)):
            rows = []
            for person_id, date in sorted(group.items(), key=sort_by_date):

                # Get the original person Row
                person = persons.get(person_id)
                if not person:
                    continue

                # Make a shallow copy and add the date column
                row = Storage(person)
                row._row = Storage(person._row)
                row._row[date_col], row[date_col] = date, date_represent(date)
                rows.append(row)

            result = {"title": group_title[i],
                      "columns": columns,
                      "headers": headers,
                      "types": types,
                      "rows": rows,
                      }
            output.append(result)

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def clients(start_date, end_date, organisation_id=None, exclude=None):
        """
            Returns a subquery for all relevant client person IDs, i.e. those
            who have a registration history entry (=status change) within the
            given time interval (without status change, they can neither have
            moved in nor moved out, and therefore are irrelevant for this report)

            Args:
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)
                organisation_id: limit to cases of this organisation
                exclude: exclude these person IDs (set|list|tuple)

            Returns:
                The subquery as SQL string
        """

        db = current.db
        s3db = current.s3db

        ctable = s3db.dvr_case
        rtable = s3db.cr_shelter_registration_history

        if organisation_id:
            accessible_cases = (ctable.organisation_id == organisation_id)
        else:
            accessible_cases = current.auth.s3_accessible_query("read", ctable)

        join = ctable.on(accessible_cases & \
                         (ctable.person_id == rtable.person_id) & \
                         (ctable.deleted == False) & \
                         (ctable.archived == False))

        query = (rtable.date >= start_date) & \
                (rtable.date < end_date)
        if exclude:
            query &= (~(rtable.person_id.belongs(exclude)))
        query &= (rtable.deleted == False)

        return db(query)._select(rtable.person_id,
                                 distinct = True,
                                 join = join,
                                 )

    # -------------------------------------------------------------------------
    @classmethod
    def prior_check_ins(cls, person_ids, start_date, shelter_id=None):
        """
            Returns the last check-in events before start_date for all
            persons who were checked in (at a shelter) at that date

            Args:
                person_ids: the person IDs
                start_date: the start of the interval (datetime.datetime)
                shelter_id: only consider checked-in at this shelter

            Returns:
                Rows (cr_shelter_registration_history)
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history

        latest = cls.last_status_change(person_ids,
                                        end_date = start_date,
                                        )
        join = latest.on((latest.person_id == rtable.person_id) & \
                         (latest.date == rtable.date))

        # If the last event before start_date is a check-in status
        # (at the shelter), then the person was already there and
        # therefore cannot be a new arrival
        query = (rtable.person_id.belongs(person_ids)) & \
                (rtable.status == 2) & \
                (rtable.deleted == False)
        if shelter_id:
            query = (rtable.shelter_id == shelter_id) & query

        rows = db(query).select(rtable.id,
                                rtable.person_id,
                                rtable.shelter_id,
                                rtable.date,
                                rtable.status,
                                join = join,
                                )
        return rows

    # -------------------------------------------------------------------------
    @classmethod
    def check_ins(cls, person_ids, start_date, end_date, shelter_id=None):
        """
            Returns all check-in events (at a shelter) during the period

            Args:
                person_ids: the person IDs
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)
                shelter_id: only consider check-ins at this shelter

            Returns:
                Rows (cr_shelter_registration_history)
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history

        query = (rtable.status == 2) & \
                (rtable.date >= start_date) & \
                (rtable.date < end_date) & \
                (rtable.person_id.belongs(person_ids)) & \
                (rtable.deleted == False)
        if shelter_id:
            query = (rtable.shelter_id == shelter_id) & query

        rows = db(query).select(rtable.id,
                                rtable.person_id,
                                rtable.shelter_id,
                                rtable.date,
                                rtable.status,
                                orderby = ~rtable.date,
                                )
        return rows

    # -------------------------------------------------------------------------
    @classmethod
    def final_events(cls, person_ids, start_date, end_date, shelter_id=None):
        """
            Returns the final status events of the period for all persons
            who were no longer checked-in (at a shelter) by the end of the
            period

            Args:
                person_ids: the person IDs
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)
                shelter_id: only consider checked-in at this shelter

            Returns:
                Rows (cr_shelter_registration_history)
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history

        latest = cls.last_status_change(person_ids,
                                        start_date = start_date,
                                        end_date = end_date,
                                        )
        join = latest.on((latest.person_id == rtable.person_id) & \
                         (latest.date == rtable.date))

        # Any final event during the period that is either not checked-in
        # status or not at the shelter in question indicates that the person
        # was no longer checked-in at the end of the interval
        query = (rtable.status != 2)
        if shelter_id:
            query |= (rtable.shelter_id != shelter_id)
        query = (rtable.person_id.belongs(person_ids)) & query & \
                (rtable.deleted == False)

        rows = db(query).select(rtable.id,
                                rtable.person_id,
                                rtable.shelter_id,
                                rtable.date,
                                rtable.status,
                                join = join,
                                )
        return rows

    # -------------------------------------------------------------------------
    @staticmethod
    def check_out_dates(person_ids, start_date=None, end_date=None, shelter_id=None,
                        check_in_dates=None):
        """
            Finds the last check-out date of persons during the interval

            Args:
                person_id: the person IDs
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)
                shelter_id: limit to check-outs from this shelter
                check_in_dates: a dict {person_id: check_in_dates} with known
                                check-in dates

            Returns:
                a dict {person_id: check_out_date}, where check_out_date
                can be None if no explicit checkout (from this shelter) was
                registered
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history

        query = (rtable.person_id.belongs(person_ids)) & \
                (rtable.status == 3)
        if shelter_id:
            query &= (rtable.shelter_id == shelter_id)
        if start_date:
            query &= (rtable.date >= start_date)
        if end_date:
            query &= (rtable.date < end_date)
        query &= (rtable.deleted == False)

        rows = db(query).select(rtable.person_id.with_alias("person_id"),
                                rtable.date.max().with_alias("date"),
                                groupby = rtable.person_id,
                                )

        dates = {person_id: None for person_id in person_ids}
        if not check_in_dates:
            check_in_dates = {}

        for row in rows:
            person_id = row.person_id
            # Disregard any check-out events that predate the check-in date
            check_in_date = check_in_dates.get(person_id)
            if check_in_date and row.date < check_in_date:
                continue
            dates[person_id] = row.date

        return dates

    # -------------------------------------------------------------------------
    @staticmethod
    def last_status_change(person_ids, start_date=None, end_date=None):
        """
            Returns a joinable sub-select with the dates of the last
            registration status change of persons during an interval

            Args:
                person_ids: the person IDs
                start_date: the start of the interval (datetime.datetime)
                end_date: the end of the interval (datetime.datetime)

            Returns:
                A joinable subquery with alias "last_status" and fields
                "person_id" and "date"

            Notes:
                - either end of the interval can be ommitted, thereby
                  including all registered status changes before/after
                  the other end date (if given)
                - all dates in UTC (tz-naive)
        """

        db = current.db
        s3db = current.s3db

        rtable = s3db.cr_shelter_registration_history

        query = (rtable.person_id.belongs(person_ids))
        if start_date:
            query &= (rtable.date >= start_date)
        if end_date:
            query &= (rtable.date < end_date)
        query &= (rtable.deleted == False)

        latest = db(query).nested_select(rtable.person_id.with_alias("person_id"),
                                         rtable.date.max().with_alias("date"),
                                         groupby = rtable.person_id,
                                         ).with_alias("last_status")
        return latest

# END =========================================================================
