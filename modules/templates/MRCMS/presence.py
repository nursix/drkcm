"""
    Presence List

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
        # TODO docstring

        if r.http == "GET":
            output = self.presence_list(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def presence_list(self, r, **attr):
        # TODO docstring

        record = r.record
        if not record or "site_id" not in record:
            r.error(400, current.ERROR.BAD_RECORD)

        shelter_name = record.name
        data = self.lookup(record.site_id)

        fmt = r.representation
        if fmt == "html":
            output = self.html(shelter_name, data)
        elif fmt == "xlsx":
            output = self.xlsx(shelter_name, data)
        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def columns():
        # TODO docstring

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
    def html(cls, shelter_name, data):
        # TODO docstring

        # TODO implement
        formatted = None

        return formatted

    # -------------------------------------------------------------------------
    @classmethod
    def xlsx(cls, shelter_name, data):
        # TODO docstring

        table_data = {"columns": [],
                      "headers": {},
                      "types": {},
                      "rows": data,
                      }
        for fname, label, ftype in cls.columns():
            table_data["columns"].append(fname)
            table_data["headers"][fname] = label
            table_data["types"][fname] = ftype

        current.deployment_settings.base.xls_title_row = True
        title = current.T("Presence List")
        if shelter_name:
            title = "%s - %s" % (shelter_name, title)
        output = XLSXWriter.encode(table_data,
                                   title = title,
                                   as_stream = True,
                                   )

        disposition = "attachment; filename=\"presence_list.xlsx\""
        response = current.response
        response.headers["Content-Type"] = contenttype(".xlsx")
        response.headers["Content-disposition"] = disposition

        return response.stream(output,
                               chunk_size = DEFAULT_CHUNK_SIZE,
                               request = current.request
                               )

    # -------------------------------------------------------------------------
    @classmethod
    def lookup(cls, shelter):
        """
            Looks up people currently reported present at site

            Args:
                shelter: the cr_shelter Row
            Returns:
                # TODO
        """

        T = current.T

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
                                orderby = (htable.id,
                                           ptable.last_name,
                                           ),
                                )
        units = {}
        data = []
        seen = set()
        for row in rows:
            person = row.pr_person
            if person.id in seen:
                continue
            seen.add(person.id)

            details = {}

            # Differentiate staff/resident
            if row.hrm_human_resource.id:
                details["type"] = T("Staff")
            elif row.dvr_case.id:
                details["type"] = T("Resident")
            else:
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

            data.append(details)

        # Look up housing unit names
        if units:
            utable = s3db.cr_shelter_unit
            query = utable.id.belongs(list(units.keys()))
            rows = db(query).select(utable.id, utable.name)
            for row in rows:
                persons = units[row.id]
                for details in persons:
                    details["unit"] = row.name

        return data

# END =========================================================================
