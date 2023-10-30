"""
    MRCMS Presence List

    License: MIT
"""

import datetime

from dateutil.relativedelta import relativedelta

from gluon import current
from gluon.contenttype import contenttype
from gluon.streamer import DEFAULT_CHUNK_SIZE

from core import CRUDMethod, XLSXWriter, s3_str

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
        data = self.lookup(record.site_id)

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

# END =========================================================================
