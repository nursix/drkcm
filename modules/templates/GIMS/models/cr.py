"""
    GIMS Shelter Management Extension

    Copyright: 2022 (c) AHSS

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

__all__ = ("CRReceptionCenterModel",
           )

import datetime

from collections import OrderedDict

from gluon import current, Field, DIV, \
                  IS_EMAIL, IS_EMPTY_OR, IS_INT_IN_RANGE, IS_IN_SET, IS_NOT_EMPTY
from gluon.storage import Storage

from core import DataModel, S3Duplicate, S3LocationSelector, S3PriorityRepresent, \
                 S3Represent, S3ReusableField, S3SQLCustomForm, \
                 IS_ONE_OF, IS_PHONE_NUMBER_MULTI, \
                 get_form_record_id, s3_comments, s3_date, s3_meta_fields, \
                 LocationFilter, OptionsFilter, TextFilter, get_filter_options

# =============================================================================
class CRReceptionCenterModel(DataModel):
    """ Custom Model for Reception Centers """

    names = ("cr_reception_center_type",
             "cr_reception_center",
             "cr_reception_center_status",
             )

    def model(self):

        T = current.T
        db = current.db

        s3 = current.response.s3
        crud_strings = s3.crud_strings

        define_table = self.define_table
        configure = self.configure

        # Reusable field for population-type fields
        population = S3ReusableField("population", "integer",
                                     default = 0,
                                     requires = IS_INT_IN_RANGE(0),
                                     )

        # ---------------------------------------------------------------------
        # Reception Center Types
        #
        tablename = "cr_reception_center_type"
        define_table(tablename,
                     Field("name",
                           label = T("Name"),
                           requires = IS_NOT_EMPTY(),
                           ),
                     s3_comments(),
                     *s3_meta_fields())

        # Representation
        type_represent = S3Represent(lookup=tablename)

        # Table configuration
        configure(tablename,
                  deduplicate = S3Duplicate(),
                  )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Add Facility Type"),
            title_display = T("Type Details"),
            title_list = T("Reception Center Types"),
            title_update = T("Edit Facility Type"),
            label_list_button = T("List Facility Types"),
            label_delete_button = T("Delete Facility Type"),
            msg_record_created = T("Facility Type added"),
            msg_record_modified = T("Facility Type updated"),
            msg_record_deleted = T("Facility Type deleted"),
            msg_list_empty = T("No Facility Types currently defined"),
            )

        # ---------------------------------------------------------------------
        # Reception Centers
        #
        status_opts = (("OP", T("operating")),
                       ("SB", T("standby")),
                       ("NA", T("closed")),
                       )
        status = S3ReusableField("status",
                                 default = "OP",
                                 requires = IS_IN_SET(status_opts, zero=None),
                                 represent = S3PriorityRepresent(dict(status_opts),
                                                                 {"OP": "green",
                                                                  "SB": "amber",
                                                                  "NA": "grey"
                                                                  }).represent,
                                 )

        tablename = "cr_reception_center"
        define_table(tablename,
                     Field("name",
                           label = T("Name"),
                           requires = IS_NOT_EMPTY(),
                           ),
                     self.org_organisation_id(
                        comment = None,
                        requires = self.org_organisation_requires(required = True,
                                                                  updateable = True,
                                                                  ),
                        ),
                     self.gis_location_id(
                        widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                                    required_levels = ("L1", "L2", "L3"),
                                                    show_address = True,
                                                    show_postcode = True,
                                                    show_map = True,
                                                    ),
                        ),
                     Field("type_id", "reference cr_reception_center_type",
                           label = T("Facility Type"),
                           requires = IS_ONE_OF(db, "cr_reception_center_type.id",
                                                type_represent,
                                                ),
                           represent = type_represent,
                           ),
                     status(),
                     population("capacity",
                                label = T("Maximum Capacity"),
                                comment = T("The maximum (total) capacity as number of people"),
                                ),
                     population("available_capacity",
                                label = T("Free Capacity"),
                                readable = False,
                                writable = False,
                                ),
                     population(label = T("Current Population (Total)"),
                                # Computed onaccept
                                readable = False,
                                writable = False,
                                ),
                     population("population_registered",
                                label = T("Current Population (Registered)"),
                                ),
                     population("population_unregistered",
                                label = T("Current Population (Unregistered)"),
                                ),
                     population("allocatable_capacity",
                                label = T("Allocatable Capacity"),
                                comment = T("The number of free places currently allocatable"),
                                ),
                     Field("contact_name",
                           label = T("Contact Name"),
                           represent = lambda v, row=None: v if v else "-",
                           ),
                     Field("phone",
                           label = T("Phone"),
                           requires = IS_EMPTY_OR(IS_PHONE_NUMBER_MULTI()),
                           represent = lambda v, row=None: v if v else "-",
                           ),
                     Field("email",
                           label = T("Email"),
                           requires = IS_EMPTY_OR(IS_EMAIL()),
                           represent = lambda v, row=None: v if v else "-",
                           ),
                     Field("occupancy", "integer",
                           label = T("Occupancy %"),
                           represent = self.occupancy_represent,
                           readable = False,
                           writable = False,
                           ),
                     s3_comments(),
                     *s3_meta_fields())

        # Components
        self.add_components(tablename,
                            cr_reception_center_status = {"name": "status",
                                                          "joinby": "facility_id",
                                                          },
                            )

        # Representation
        facility_represent = S3Represent(lookup=tablename)

        # List fields
        list_fields = ["name",
                       "type_id",
                       "location_id",
                       "status",
                       "capacity",
                       "population_registered",
                       "population_unregistered",
                       "available_capacity",
                       "allocatable_capacity",
                       "occupancy",
                       ]

        # Filter widgets
        filter_widgets = [TextFilter(["name",
                                      ],
                                     label = T("Search"),
                                     ),
                          OptionsFilter("type_id",
                                        options = get_filter_options("cr_reception_center_type"),
                                        ),
                          OptionsFilter("status",
                                        default = "OP",
                                        options = OrderedDict(status_opts),
                                        cols = 3,
                                        sort = False,
                                        ),
                          ]

        crud_form = S3SQLCustomForm(# ---- Facility ----
                                    "organisation_id",
                                    "name",
                                    "type_id",
                                    "status",
                                    # ---- Address ----
                                    "location_id",
                                    # ---- Capacity & Population ----
                                    "capacity",
                                    "population_registered",
                                    "population_unregistered",
                                    "allocatable_capacity",
                                    # ---- Contact Information ----
                                    "contact_name",
                                    "phone",
                                    "email",
                                    # ---- Comments ----
                                    "comments",
                                    )

        subheadings = {"organisation_id": T("Facility"),
                       "location_id": T("Address"),
                       "capacity": T("Capacity / Occupancy"),
                       "contact_name": T("Contact Information"),
                       "comments": T("Comments"),
                       }

        # Report options
        axes = ["location_id$L3",
                "location_id$L2",
                "location_id$L1",
                "organisation_id",
                "type_id",
                "status",
                ]

        report_options = {
            "rows": axes,
            "cols": axes,
            "fact": [(T("Number of Facilities"), "count(id)"),
                     (T("Current Population (Total)"), "sum(population)"),
                     (T("Current Population (Registered)"), "sum(population_registered)"),
                     (T("Current Population (Unregistered)"), "sum(population_unregistered)"),
                     (T("Occupancy % (Average)"), "avg(occupancy)"),
                     (T("Allocatable Capacity"), "sum(allocatable_capacity)"),
                     (T("Free Capacity"), "sum(available_capacity)"),
                     (T("Total Capacity"), "sum(capacity)"),
                     ],
            "defaults": {"rows": "location_id$L2",
                         "cols": "type_id",
                         "fact": "sum(population)",
                         "totals": True,
                         },
            }

        # Table configuration
        configure(tablename,
                  crud_form = crud_form,
                  subheadings = subheadings,
                  deduplicate = S3Duplicate(primary = ("name",),
                                            secondary = ("organisation_id",),
                                            ),
                  filter_widgets = filter_widgets,
                  list_fields = list_fields,
                  report_options = report_options,
                  onaccept = self.reception_center_onaccept,
                  )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Add Facility"),
            title_display = T("Facility Details"),
            title_list = T("Reception Centers"),
            title_update = T("Edit Facility"),
            label_list_button = T("List Facilities"),
            label_delete_button = T("Delete Facility"),
            msg_record_created = T("Facility added"),
            msg_record_modified = T("Facility updated"),
            msg_record_deleted = T("Facility deleted"),
            msg_list_empty = T("No Facilities currently registered"),
            )

        # ---------------------------------------------------------------------
        # Reception Center Status
        #
        tablename = "cr_reception_center_status"
        define_table(tablename,
                     Field("facility_id", "reference cr_reception_center",
                           label = T("Facility"),
                           ondelete = "CASCADE",
                           requires = IS_ONE_OF(db, "cr_reception_center.id",
                                                facility_represent,
                                                ),
                           represent = facility_represent,
                           ),
                     s3_date(default = "now",
                             ),
                     s3_date("date_until",
                             readable = False,
                             writable = False,
                             ),
                     status(),
                     population("capacity",
                                label = T("Maximum Capacity"),
                                comment = T("The maximum (total) capacity as number of people"),
                                ),
                     population("available_capacity",
                                label = T("Free Capacity"),
                                ),
                     population(label = T("Current Population (Total)"),
                                ),
                     population("population_registered",
                                label = T("Current Population (Registered)"),
                                ),
                     population("population_unregistered",
                                label = T("Current Population (Unregistered)"),
                                ),
                     population("allocatable_capacity",
                                label = T("Allocatable Capacity"),
                                comment = T("The number of free places currently allocatable"),
                                ),
                     Field("occupancy", "integer",
                           label = T("Occupancy %"),
                           represent = self.occupancy_represent,
                           ),
                     *s3_meta_fields())

        # Filter Widgets
        filter_widgets = [TextFilter(["facility_id$name",
                                      ],
                                     label = T("Search"),
                                     ),
                          OptionsFilter("facility_id$type_id",
                                        options = get_filter_options("cr_reception_center_type"),
                                        ),
                          OptionsFilter("status",
                                        options = OrderedDict(status_opts),
                                        cols = 3,
                                        default = ["OP", "SB"],
                                        sort = False,
                                        ),
                          OptionsFilter("facility_id",
                                        options = get_filter_options("cr_reception_center"),
                                        hidden = True,
                                        ),
                          LocationFilter("facility_id$location_id",
                                         levels = ["L2", "L3"],
                                         hidden = True,
                                         ),
                          ]

        # Timeplot options
        facts = [(T("Current Population (Total)"), "sum(population)"),
                 (T("Current Population (Registered)"), "sum(population_registered)"),
                 (T("Current Population (Unregistered)"), "sum(population_unregistered)"),
                 (T("Occupancy % (Average)"), "avg(occupancy)"),
                 (T("Allocatable Capacity"), "sum(allocatable_capacity)"),
                 (T("Free Capacity"), "sum(available_capacity)"),
                 (T("Total Capacity"), "sum(capacity)"),
                 ]
        timeplot_options = {
            "facts": facts,
            "timestamp": ((T("per interval"), "date,date_until"),
                          #(T("cumulative"), "date"),
                          ),
            "time": ((T("Last 3 Months"), "-3 months", "", "days"),
                     (T("This Month"), "<-0 months", "", "days"),
                     (T("This Week"), "<-0 weeks", "", "days"),
                     ),
            "defaults": {"fact": facts[0],
                         "timestamp": "date,date_until",
                         "time": "<-0 months||days",
                         },
            }

        # Table configuration
        configure(tablename,
                  filter_widgets = filter_widgets,
                  insertable = False,
                  editable = False,
                  deletable = False,
                  orderby = "%s.date desc" % tablename,
                  timeplot_options = timeplot_options,
                  )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            title_display = T("Status"),
            title_list = T("Status History"),
            label_list_button = T("Status History"),
            msg_list_empty = T("No Status History currently registered"),
            )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

    # -------------------------------------------------------------------------
    @classmethod
    def reception_center_onaccept(cls, form):
        """
            Onaccept of reception center
                - update total population
                - update gross available capacity
                - sanitize net available capacity
                - update occupancy (percentage)
                - update status history
        """

        record_id = get_form_record_id(form)
        if not record_id:
            return

        table = current.s3db.cr_reception_center
        query = (table.id == record_id) & \
                (table.deleted == False)
        record = current.db(query).select(table.id,
                                          table.population_registered,
                                          table.population_unregistered,
                                          table.capacity,
                                          table.allocatable_capacity,
                                          limitby = (0, 1),
                                          ).first()

        num_r = record.population_registered
        num_u = record.population_unregistered
        total = (num_r if num_r else 0) + (num_u if num_u else 0)
        update = {"population": total}

        capacity = record.capacity
        if capacity:
            available = max(0, capacity - total)
        else:
            available = 0
        update["available_capacity"] = available

        allocatable = record.allocatable_capacity
        if allocatable > available or allocatable < 0:
            update["allocatable_capacity"] = available

        if capacity > 0:
            occupancy = total * 100 // capacity
        else:
            occupancy = 100
        update["occupancy"] = occupancy

        record.update_record(**update)

        # Update the status history
        cls.update_status_history(record.id)

    # -------------------------------------------------------------------------
    @staticmethod
    def update_status_history(facility_id):
        """
            Updates the status history of a facility

            Args:
                facility_id: the cr_reception_center record ID
        """

        db = current.db
        s3db = current.s3db

        ftable = s3db.cr_reception_center
        stable = s3db.cr_reception_center_status

        status_fields = ("status",
                         "capacity",
                         "available_capacity",
                         "population",
                         "population_registered",
                         "population_unregistered",
                         "allocatable_capacity",
                         "occupancy",
                         )

        # Get the reception center record
        fields = [ftable.id] + [ftable[fn] for fn in status_fields]
        query = (ftable.id == facility_id) & (ftable.deleted == False)
        facility = db(query).select(*fields, limitby=(0, 1)).first()

        # Look up the status record for today
        today = current.request.utcnow.date()
        query = (stable.facility_id == facility_id) & \
                (stable.date == today) & \
                (stable.deleted == False)
        status = db(query).select(stable.id, limitby = (0, 1)).first()
        if not status:
            # Create it
            data = {fn: facility[fn] for fn in status_fields}
            data["facility_id"] = facility.id
            status_id = data["id"] = stable.insert(**data)
            s3db.update_super(stable, status)
            current.auth.s3_set_record_owner(stable, status_id)
            s3db.onaccept(stable, status, method="create")
        else:
            # Update it
            update = {fn: facility[fn] for fn in status_fields}
            status.update_record(**update)
            s3db.onaccept(stable, status, method="update")

        # Update the previous record (set end-date)
        query = (stable.facility_id == facility_id) & \
                (stable.date < today) & \
                (stable.deleted == False)
        status = db(query).select(stable.id,
                                  orderby = ~stable.date,
                                  limitby = (0, 1),
                                  ).first()
        if status:
            status.update_record(date_until = today-datetime.timedelta(days=1))

    # -------------------------------------------------------------------------
    @staticmethod
    def occupancy_represent(value, row=None):
        """
            Representation of occupancy as decision aid, progress-bar style

            Args:
                value: the occupancy value (percentage, integer 0..>100)

            Returns:
                stylable DIV
        """

        if not value:
            value = 0
            css_class = "occupancy-0"
        else:
            reprval = value // 10 * 10 + 10
            if reprval > 100:
                css_class = "occupancy-exc"
            else:
                css_class = "occupancy-%s" % reprval

        return DIV("%s%%" % value,
                   DIV(_class="occupancy %s" % css_class),
                   _class="occupancy-bar",
                   )

# END =========================================================================
