"""
    Helper functions and classes for RLPPTM

    License: MIT
"""

from gluon import current

from core import FS, S3DateTime, s3_str

# =============================================================================
def drk_default_shelter():
    """
        Lazy getter for the default shelter_id
    """

    s3 = current.response.s3
    shelter_id = s3.drk_default_shelter

    if not shelter_id:
        default_site = current.deployment_settings.get_org_default_site()

        # Get the shelter_id for default site
        if default_site:
            stable = current.s3db.cr_shelter
            query = (stable.site_id == default_site)
            shelter = current.db(query).select(stable.id,
                                            limitby=(0, 1),
                                            ).first()
            if shelter:
                shelter_id = shelter.id

        s3.drk_default_shelter = shelter_id

    return shelter_id

# =============================================================================
class DRKSiteActivityReport:
    """
        Helper class to produce site activity reports ("Residents Report")
    """

    def __init__(self, site_id=None, date=None):
        """
            Args:
                site_id: the site ID (defaults to default site)
                date: the date of the report (defaults to today)
        """

        if site_id is None:
            site_id = current.deployment_settings.get_org_default_site()
        self.site_id = site_id

        if date is None:
            date = current.request.utcnow.date()
        self.date = date

    # -------------------------------------------------------------------------
    def extract(self):
        """
            Extract the data for this report
        """

        db = current.db
        s3db = current.s3db

        T = current.T

        site_id = self.site_id
        date = self.date

        # Get all flags for which cases shall be excluded
        ftable = s3db.dvr_case_flag
        query = (ftable.nostats == True) & \
                (ftable.deleted == False)
        rows = db(query).select(ftable.id)
        nostats = set(row.id for row in rows)

        # Identify the relevant cases
        ctable = s3db.dvr_case
        ltable = s3db.dvr_case_flag_case

        num_nostats_flags = ltable.id.count()
        left = ltable.on((ltable.person_id == ctable.person_id) & \
                         (ltable.flag_id.belongs(nostats)) & \
                         (ltable.deleted == False))

        query = (ctable.site_id == site_id) & \
                ((ctable.date == None) | (ctable.date <= date)) & \
                ((ctable.closed_on == None) | (ctable.closed_on >= date)) & \
                (ctable.archived != True) & \
                (ctable.deleted != True)


        rows = db(query).select(ctable.id,
                                ctable.person_id,
                                ctable.date,
                                ctable.closed_on,
                                num_nostats_flags,
                                groupby = ctable.id,
                                left = left,
                                )

        # Count them
        old_total, ins, outs = 0, 0, 0
        person_ids = set()
        for row in rows:
            if row[num_nostats_flags]:
                continue
            case = row.dvr_case
            person_ids.add(case.person_id)
            if not case.date or case.date < date:
                old_total += 1
            else:
                ins += 1
            if case.closed_on and case.closed_on == date:
                outs += 1
        result = {"old_total": old_total,
                  "new_total": old_total - outs + ins,
                  "ins": ins,
                  "outs": outs,
                  }

        # Add completed appointments as pr_person components
        atypes = {"BAMF": None,
                  "GU": None,
                  "Transfer": None,
                  "X-Ray": None,
                  "Querverlegung": None,
                  }
        COMPLETED = 4
        attable = s3db.dvr_case_appointment_type
        query = attable.name.belongs(set(atypes.keys()))
        rows = db(query).select(attable.id,
                                attable.name,
                                )
        add_components = s3db.add_components
        hooks = []
        for row in rows:
            type_id = row.id
            name = row.name
            atypes[name] = alias = "appointment%s" % type_id
            hook = {"name": alias,
                    "joinby": "person_id",
                    "filterby": {"type_id": type_id,
                                 "status": COMPLETED,
                                 },
                    }
            hooks.append(hook)
        s3db.add_components("pr_person", dvr_case_appointment = hooks)
        date_completed = lambda t: (T("%(event)s on") % {"event": T(t)},
                                    "%s.date" % atypes[t],
                                    )

        # Filtered component for paid allowances
        PAID = 2
        add_components("pr_person",
                       dvr_allowance = {"name": "payment",
                                        "joinby": "person_id",
                                        "filterby": {"status": PAID},
                                        },
                       )

        # Represent paid_on as date
        atable = s3db.dvr_allowance
        atable.paid_on.represent = lambda dt: \
                                   S3DateTime.date_represent(dt,
                                                             utc=True,
                                                             )

        # Filtered component for preliminary residence permit
        s3db.add_components("pr_person",
                            pr_identity = {"name": "residence_permit",
                                           "joinby": "person_id",
                                           "filterby": {"type": 5},
                                           "multiple": False,
                                           },
                            )

        # Filtered component for family
        s3db.add_components("pr_person",
                            pr_group = {"name": "family",
                                        "link": "pr_group_membership",
                                        "joinby": "person_id",
                                        "key": "group_id",
                                        "filterby": {"group_type": 7},
                                        },
                            )

        # Get family roles
        gtable = s3db.pr_group
        mtable = s3db.pr_group_membership
        join = gtable.on(gtable.id == mtable.group_id)
        query = (mtable.person_id.belongs(person_ids)) & \
                (mtable.deleted != True) & \
                (gtable.group_type == 7)
        rows = db(query).select(mtable.person_id,
                                mtable.group_head,
                                mtable.role_id,
                                join = join,
                                )

        # Bulk represent all possible family roles (to avoid repeated lookups)
        represent = mtable.role_id.represent
        rtable = s3db.pr_group_member_role
        if hasattr(represent, "bulk"):
            query = (rtable.group_type == 7) & (rtable.deleted != True)
            roles = db(query).select(rtable.id)
            role_ids = [role.id for role in roles]
            represent.bulk(role_ids)

        # Create a dict of {person_id: role}
        roles = {}
        HEAD_OF_FAMILY = T("Head of Family")
        for row in rows:
            person_id = row.person_id
            role = row.role_id
            if person_id in roles:
                continue
            if (row.group_head):
                roles[person_id] = HEAD_OF_FAMILY
            elif role:
                roles[person_id] = represent(role)

        # Field method to determine the family role
        def family_role(row):
            person_id = row["pr_person.id"]
            return roles.get(person_id, "")

        # Dummy virtual fields to produce empty columns
        from s3dal import Field
        ptable = s3db.pr_person
        empty = lambda row: ""
        if not hasattr(ptable, "xray_place"):
            ptable.xray_place = Field.Method("xray_place", empty)
        if not hasattr(ptable, "family_role"):
            ptable.family_role = Field.Method("family_role", family_role)

        # List fields for the report
        list_fields = ["family.id",
                       (T("ID"), "pe_label"),
                       (T("Name"), "last_name"),
                       "first_name",
                       "date_of_birth",
                       "gender",
                       "person_details.nationality",
                       (T("Family Role"), "family_role"),
                       (T("Room No."), "shelter_registration.shelter_unit_id"),
                       "case_flag_case.flag_id",
                       "dvr_case.comments",
                       date_completed("GU"),
                       date_completed("X-Ray"),
                       (T("X-Ray Place"), "xray_place"),
                       date_completed("BAMF"),
                       (T("BÜMA valid until"), "dvr_case.valid_until"),
                       (T("Preliminary Residence Permit until"), "residence_permit.valid_until"),
                       (T("Allowance Payments"), "payment.paid_on"),
                       (T("Admitted on"), "dvr_case.date"),
                       "dvr_case.origin_site_id",
                       date_completed("Transfer"),
                       date_completed("Querverlegung"),
                       #"dvr_case.closed_on",
                       "dvr_case.status_id",
                       "dvr_case.destination_site_id",
                       ]

        query = FS("id").belongs(person_ids)
        resource = s3db.resource("pr_person", filter = query)

        data = resource.select(list_fields,
                               represent = True,
                               raw_data = True,
                               # Keep families together, eldest member first
                               orderby = ["pr_family_group.id",
                                          "pr_person.date_of_birth",
                                          ],
                               )

        # Generate headers, labels, types for XLS report
        rfields = data.rfields
        columns = []
        headers = {}
        types = {}
        for rfield in rfields:
            colname = rfield.colname
            if colname in ("dvr_case_flag_case.flag_id",
                           "pr_family_group.id",
                           ):
                continue
            columns.append(colname)
            headers[colname] = rfield.label
            types[colname] = rfield.ftype

        # Post-process rows
        rows = []
        for row in data.rows:

            flags = "dvr_case_flag_case.flag_id"
            comments = "dvr_case.comments"

            raw = row["_row"]
            if raw[flags]:
                items = ["%s: %s" % (T("Advice"), s3_str(row[flags]))]
                if raw[comments]:
                    items.insert(0, raw[comments])
                row[comments] = ", ".join(items)
            rows.append(row)

        # Add XLS report data to result
        report = {"columns": columns,
                  "headers": headers,
                  "types": types,
                  "rows": rows,
                  }
        result["report"] = report

        return result

    # -------------------------------------------------------------------------
    def store(self, authorised=None):
        """
            Store this report in dvr_site_activity
        """

        db = current.db
        s3db = current.s3db
        auth = current.auth
        settings = current.deployment_settings

        # Table name and table
        tablename = "dvr_site_activity"
        table = s3db.table(tablename)
        if not table:
            return None

        # Get the current site activity record
        query = (table.date == self.date) & \
                (table.site_id == self.site_id) & \
                (table.deleted != True)
        row = db(query).select(table.id,
                               limitby = (0, 1),
                               orderby = ~table.created_on,
                               ).first()

        # Check permission
        if authorised is None:
            has_permission = current.auth.s3_has_permission
            if row:
                authorised = has_permission("update", tablename, record_id=row.id)
            else:
                authorised = has_permission("create", tablename)
        if not authorised:
            from core import S3PermissionError
            raise S3PermissionError

        # Extract the data
        data = self.extract()

        # Custom header for Excel Export (disabled for now)
        settings.base.xls_title_row = lambda sheet: self.summary(sheet, data)

        # Export as XLS
        title = current.T("Resident List")
        from core import S3Exporter
        exporter = S3Exporter().xls
        report = exporter(data["report"],
                          title = title,
                          as_stream = True,
                          )

        # Construct the filename
        filename = "%s_%s_%s.xls" % (title, self.site_id, str(self.date))

        # Store the report
        report_ = table.report.store(report, filename)
        record = {"site_id": self.site_id,
                  "old_total": data["old_total"],
                  "new_total": data["new_total"],
                  "cases_new": data["ins"],
                  "cases_closed": data["outs"],
                  "date": self.date,
                  "report": report_,
                  }

        # Customize resource
        from core import CRUDRequest
        r = CRUDRequest("dvr", "site_activity",
                        current.request,
                        args = [],
                        get_vars = {},
                        )
        r.customise_resource("dvr_site_activity")

        if row:
            # Trigger auto-delete of the previous file
            row.update_record(report=None)
            # Update it
            success = row.update_record(**record)
            if success:
                s3db.onaccept(table, record, method="create")
                result = row.id
            else:
                result = None
        else:
            # Create a new one
            record_id = table.insert(**record)
            if record_id:
                record["id"] = record_id
                s3db.update_super(table, record)
                auth.s3_set_record_owner(table, record_id)
                auth.s3_make_session_owner(table, record_id)
                s3db.onaccept(table, record, method="create")
                result = record_id
            else:
                result = None

        return result

    # -------------------------------------------------------------------------
    def summary(self, sheet, data=None):
        """
            Header for the Excel sheet

            Args:
                sheet: the sheet
                data: the data dict from extract()

            Returns:
                the number of rows in the header
        """

        length = 3

        if sheet is not None and data is not None:

            T = current.T
            output = (("Date", S3DateTime.date_represent(self.date, utc=True)),
                      ("Previous Total", data["old_total"]),
                      ("Admissions", data["ins"]),
                      ("Departures", data["outs"]),
                      ("Current Total", data["new_total"]),
                      )

            import xlwt
            label_style = xlwt.XFStyle()
            label_style.font.bold = True

            col_index = 3
            for label, value in output:
                label_ = s3_str(T(label))
                value_ = s3_str(value)

                # Adjust column width
                width = max(len(label_), len(value_))
                sheet.col(col_index).width = max(width * 310, 2000)

                # Write the label
                current_row = sheet.row(0)
                current_row.write(col_index, label_, label_style)

                # Write the data
                current_row = sheet.row(1)
                current_row.write(col_index, value_)
                col_index += 1

        return length

# END =========================================================================
