"""
    MRCMS Presence List

    License: MIT
"""

import datetime
import json

from dateutil.relativedelta import relativedelta

from gluon import current, SQLFORM
from gluon.contenttype import contenttype
from gluon.serializers import json as jsons
from gluon.streamer import DEFAULT_CHUNK_SIZE

from s3dal import Field

from core import CRUDMethod, CustomController, DateField, FormKey, JSONERRORS, S3DateTime, XLSXWriter, s3_str

# =============================================================================
class PresenceList(CRUDMethod):
    """
        A list of people currently reported present at a site
    """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point for the CRUDController

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        if r.http == "GET":
            output = self.presence_list(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def presence_list(self, r, **attr):
        """
            Generate the presence list

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        record = r.record
        if not record or "site_id" not in record:
            r.error(400, current.ERROR.BAD_RECORD)

        shelter_name = record.name
        data = self.lookup(record)

        fmt = r.representation
        if fmt == "xlsx":
            output = self.xlsx(shelter_name, data)
        # TODO support other formats?
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    @classmethod
    def xlsx(cls, shelter_name, data):
        """
            Serialize the presence data as Excel file

            Args:
                shelter_name: the shelter name
                data: the presence data as extracted with lookup()
            Returns:
                the Excel file as byte stream
        """

        # Prepare the input for XLSXWriter
        table_data = {"columns": [],
                      "headers": {},
                      "types": {},
                      "rows": data,
                      }
        for fname, label, ftype in cls.columns():
            table_data["columns"].append(fname)
            table_data["headers"][fname] = label
            table_data["types"][fname] = ftype

        # Use a title row (also includes exported-date)
        current.deployment_settings.base.xls_title_row = True
        title = current.T("Presence List")
        if shelter_name:
            title = "%s - %s" % (shelter_name, title)

        # Generate XLSX byte stream
        output = XLSXWriter.encode(table_data, title=title, as_stream=True)

        # Set response headers
        disposition = "attachment; filename=\"presence_list.xlsx\""
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
    def columns():
        """
            The columns of the presence list, in order of appearance

            Returns:
                tuple of tuples ((fieldname, label, datatype), ...)
        """

        T = current.T

        return (("type", "", "string"),
                ("pe_label", T("ID"), "string"),
                ("last_name", T("Last Name"), "string"),
                ("first_name", T("First Name"), "string"),
                ("gender", T("Gender"), "string"),
                ("age", T("Age"), "string"),
                ("unit", T("Housing Unit"), "string"),
                ("since", T("Reported present since"), "datetime"),
                )

    # -------------------------------------------------------------------------
    @classmethod
    def lookup(cls, shelter):
        """
            Looks up people currently reported present at the shelter

            Args:
                shelter: the cr_shelter Row
            Returns:
                the entries for the presence list as a list of dicts,
                dict structure see columns()

            Note:
                entries pre-ordered by name (last name, then first name)
        """

        T = current.T
        STAFF = T("Staff")
        RESIDENT = T("Resident")

        db = current.db
        s3db = current.s3db

        shelter_id = shelter.id
        site_id = shelter.site_id
        organisation_id = shelter.organisation_id

        ptable = s3db.pr_person
        ctable = s3db.dvr_case
        htable = s3db.hrm_human_resource
        rtable = s3db.cr_shelter_registration
        left = [ctable.on((ctable.person_id == ptable.id) & \
                          (ctable.organisation_id == organisation_id) & \
                          (ctable.deleted == False)),
                htable.on((htable.person_id == ptable.id) & \
                          (htable.organisation_id == organisation_id) & \
                          (htable.deleted == False)),
                rtable.on((rtable.person_id == ptable.id) & \
                          (rtable.shelter_id == shelter_id) & \
                          (rtable.deleted == False)),
                ]

        sptable = s3db.org_site_presence

        join = sptable.on((sptable.person_id == ptable.id) & \
                          (sptable.site_id == site_id) & \
                          (sptable.status == "IN") & \
                          (sptable.deleted == False))

        query = (ptable.deleted == False)
        rows = db(query).select(ptable.id,
                                ptable.pe_label,
                                ptable.first_name,
                                ptable.last_name,
                                ptable.gender,
                                ptable.date_of_birth,
                                sptable.date,
                                ctable.id,
                                htable.id,
                                rtable.id,
                                rtable.shelter_unit_id,
                                left = left,
                                join = join,
                                orderby = (ptable.last_name,
                                           ptable.first_name,
                                           ),
                                )
        staff, residents = [], []
        units = {}
        seen = set()
        for row in rows:
            person = row.pr_person
            if person.id in seen:
                # Duplicate (e.g. multiple case records):
                # - this is a serious DB inconsistency, but an emergency
                #   tool is the wrong place to raise the issue, so we just
                #   skip it
                continue
            seen.add(person.id)

            details = {}

            # Differentiate staff/residents
            if row.dvr_case.id:
                items = residents
                details["type"] = RESIDENT
            elif row.hrm_human_resource.id:
                items = staff
                details["type"] = STAFF
            else:
                items = staff
                details["type"] = "-"

            # Basic person data
            for fn in ("pe_label", "first_name", "last_name", "gender"):
                value = person[fn]
                represent = ptable[fn].represent
                details[fn] = represent(value) if represent else s3_str(value)

            # Age
            now = datetime.datetime.utcnow().date()
            dob = person.date_of_birth
            age = str(relativedelta(now, dob).years) if dob else "-"
            details["age"] = age

            # Housing unit
            registration = row.cr_shelter_registration
            if registration.id:
                unit_id = registration.shelter_unit_id
                persons = units.get(unit_id)
                if not persons:
                    persons = units[unit_id] = []
                persons.append(details)
            details["unit"] = "-"

            # Presence date
            presence = row.org_site_presence
            since = sptable.date.represent(presence.date)
            details["since"] = since

            items.append(details)

        # Look up housing unit names
        if units:
            utable = s3db.cr_shelter_unit
            query = utable.id.belongs(list(units.keys()))
            rows = db(query).select(utable.id, utable.name)
            for row in rows:
                persons = units[row.id]
                for details in persons:
                    details["unit"] = row.name

        return staff + residents

# =============================================================================
class RegistrationHistory(CRUDMethod):
    """
        Ajax method to read the shelter registration history of a client
    """

    def apply_method(self, r, **attr):
        """
            Entry point for the CRUDController

            Args:
                r: the CRUDRequest
                attr: controller parameters
        """

        output = {}

        resource = r.resource
        if resource.tablename != "pr_person":
            r.error(400, current.ERROR.BAD_RESOURCE)
        if not r.record:
            r.error(400, current.ERROR.BAD_RECORD)

        has_permission = current.auth.s3_has_permission
        person_id = r.record.id
        if not has_permission("read", "pr_person", record_id=person_id) or \
           not has_permission("read", "cr_shelter_registration_history"):
            r.unauthorised()

        if r.http == "GET":
            if r.representation == "json":
                history = self.extract(person_id)
                response = current.response
                if response:
                    response.headers["Content-Type"] = "application/json"
                output = self.serialize(history)

            else:
                r.error(415, current.ERROR.BAD_FORMAT)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def extract(person_id):
        """
            Extracts the registration registry of a person

            Args:
                person_id: the person record ID

            Returns:
                a list of dicts like:
                    [{s: ShelterID,
                      p: PlannedDateTime,
                      i: CheckedInDateTime,
                      o: CheckedOutDateTime,
                      }, ...]
        """

        db = current.db
        s3db = current.s3db

        # Get all shelter registration history entries for person_id
        # ordered by date
        table = s3db.cr_shelter_registration_history

        query = (table.person_id == person_id) & \
                (table.deleted == False)
        rows = db(query).select(table.shelter_id,
                                table.date,
                                table.status,
                                table.previous_status,
                                orderby = table.date,
                                )

        history = []
        new_item = lambda s: {"s": s, "p": None, "i": None, "o": None, "c": False}

        item = None
        shelter_id = None
        current_status = None
        for row in rows:
            date = row.date
            status = row.status

            if not date or status not in (1, 2, 3):
                continue
            if shelter_id != row.shelter_id:
                if item:
                    history.append(item)
                shelter_id, item = row.shelter_id, None
            elif not shelter_id:
                continue

            if not item:
                item = new_item(shelter_id)

            if status == 1:
                if current_status == 2:
                    history.append(item)
                    item = new_item(shelter_id)
                if not item.get("p"):
                    item["p"] = date
            elif status == 2:
                if not item.get("i"):
                    item["i"] = date
            elif status == 3:
                item["o"] = date
                history.append(item)
                item = None
            else:
                continue

            current_status = status

        if item:
            history.append(item)
        return history

    # -------------------------------------------------------------------------
    @staticmethod
    def serialize(history):
        """
            Serializes the registration history as JSON

            Args:
                history: the history as returned by extract()

            Returns:
                a JSON array of objects like:
                    [{n: ShelterName,
                      p: PlannedDate,
                      i: CheckedInDate,
                      o: CheckedOutDate,
                      }, ...]
        """

        # Lookup all shelter names
        table = current.s3db.cr_shelter
        shelter_ids = {item.get("s") for item in history}
        rows = current.db(table.id.belongs(shelter_ids)).select(table.id,
                                                                table.name,
                                                                )
        shelters = {row.id: row.name for row in rows}

        # Format all items
        dtfmt = lambda dt: S3DateTime.date_represent(dt, utc=True)
        formatted = []
        for item in history:
            name = shelters.get(item.get("s"))
            if not name:
                name = "?"

            planned = item.get("p")
            planned = dtfmt(planned) if planned else None
            checked_in = item.get("i")
            checked_in = dtfmt(checked_in) if checked_in else None
            checked_out = item.get("o")
            checked_out = dtfmt(checked_out) if checked_out else None

            formatted.append({"n": name, "p": planned, "i": checked_in, "o": checked_out})

        # Mark last item as current if not checked-out
        if formatted and formatted[-1]["o"] is None:
            formatted[-1]["c"] = True

        # Serialize as JSON
        return json.dumps(formatted)

# =============================================================================
class PresenceReport(CRUDMethod):
    """ Report over the last sightings of clients """

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

        # Standard form fields and data
        formfields = [Field("organisation_id",
                            label = T("Organization"),
                            # TODO implement selector
                            #requires = IS_IN_SET,
                            ),
                      DateField("date",
                                label = T("Date"),
                                default = "now",
                                ),
                      ]

        # Hidden inputs
        hidden = {"formkey": FormKey("last_seen").generate(),
                  }

        formstyle = current.deployment_settings.get_ui_formstyle()
        widget_id = "last-seen"
        form = SQLFORM.factory(*formfields,
                               record = None,
                               showid = False,
                               formstyle = formstyle,
                               hidden = hidden,
                               buttons = [],
                               _id = widget_id,
                               _class = "last-seen-form",
                               )
        output = {"title": T("Last Presence"),
                  "form": form,
                  }

        # TODO add XLSX export button
        CustomController._view("MRCMS", "last_seen.html")

        # TODO inject client-side script to retrieve and render the data

        return output

    # -------------------------------------------------------------------------
    def parameters(self, r):
        """
            Parses the request parameters (from POST/json)

            Args:
                r: the CRUDRequest

            Returns:
                tuple (organisation_id, date)

            Raises:
                HTTP400 for invalid parameters or formkey mismatch
        """

        INVALID = "Invalid request parameters"

        # Read+parse body JSON
        s = r.body
        s.seek(0)
        try:
            options = json.load(s)
        except JSONERRORS:
            options = None
        if not isinstance(options, dict):
            r.error(400, INVALID)

        # Verify submitted form key against session (CSRF protection)
        formkey = FormKey("last_seen")
        if not formkey.verify(options):
            r.error(400, "Invalid action key (form reopened in another tab?)")

        # Extract organisation ID
        organisation = options.get("organisation")
        try:
            organisation_id = int(organisation)
        except (ValueError, TypeError):
            r.error(400, INVALID)

        # Extract report reference date
        dtstr = options.get("date")
        date = current.calendar.parse_date(dtstr)
        if not date:
            r.error(400, INVALID)

        return organisation_id, date

    # -------------------------------------------------------------------------
    def json(self, r, **attr):
        """
            Renders the report as JSON object

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                JSON {results: n, labels: [], records: [[]]} as string
        """

        T = current.T

        # Read request parameters
        organisation_id, date = self.parameters(r)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, date)

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
                  "results": len(records),
                  }

        # Set Content Type
        current.response.headers["Content-Type"] = "application/json"

        return jsons(output)


    # -------------------------------------------------------------------------
    def xlsx(self, r, **attr):
        """
            Renders the report as XLSX spreadsheet

            Args:
                r: the CRUDRequest
                attr: controller parameters

            Returns:
                XLSX binary stream
        """

        T = current.T

        # Read request parameters
        organisation_id, date = self.parameters(r)

        # Check permissions
        if not self.permitted(organisation_id=organisation_id):
            r.unauthorised()

        # Extract the data
        data = self.extract(organisation_id, date)

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
        title = "%s - %s" % (T("Presence Report"),
                             S3DateTime.date_represent(date, utc=True),
                             )

        # Generate XLSX byte stream
        output = XLSXWriter.encode(table_data, title=title, as_stream=True)

        # Set response headers
        # TODO add date to file name
        disposition = "attachment; filename=\"presence_report.xlsx\""
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
    def extract(cls, organisation_id, date):
        """
            Extracts the case data and last sighting events for all open cases
            of the organisation on the given date

            Args:
                organisation_id: the organisation record ID
                date: the cutoff date

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

        # All persons with open cases with this organisation on that date
        table = s3db.dvr_case
        query = (table.organisation_id == organisation_id) & \
                ((table.date == None) | (table.date <= date)) & \
                ((table.closed_on == None) | (table.closed_on >= date)) & \
                (table.archived == False) & \
                (table.deleted == False)
        person_ids = db(query)._select(table.person_id)

        # Retrieve the case data
        resource = s3db.resource("pr_person")
        resource.add_filter(resource.table._id.belongs(person_ids))
        persons = resource.select(list_fields,
                                  represent = True,
                                  raw_data = True,
                                  orderby = "pr_person.last_name,pr_person.first_name",
                                  )

        # All relevant shelters
        stable = s3db.cr_shelter
        query = (stable.organisation_id == organisation_id) & \
                (stable.deleted == False)
        site_ids = db(query)._select(stable.site_id)

        # Last presence events for the persons at any of these shelters
        presence_events = cls.last_site_presence_event(person_ids, site_ids, date)

        # Last checkpoint events for the persons with this organisation
        case_events = cls.last_case_event(person_ids, organisation_id, date)

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
    def last_case_event(person_ids, organisation_id, date):
        """
            Retrieves the last case events (checkpoint events) before or
            on date, where personal presence was required

            Args:
                person_ids: the person record IDs of the relevant clients
                organisation_id: the organisation defining the event types
                date: the cutoff date

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
                (table.date <= date) & \
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
    def last_site_presence_event(person_ids, site_ids, date):
        """
            Retrieves the last site presence events (IN|OUT) before or on
            date

            Args:
                person_ids: the person record IDs of the relevant clients
                site_ids: the site_id of the relevant shelters
                date: the cutoff date

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
                (table.date <= date) & \
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

# END =========================================================================
