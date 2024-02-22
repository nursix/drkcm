"""
    Activity Management

    Copyright: 2024-2024 (c) Sahana Software Foundation

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

__all__ = ("ActivityModel",
           "ActivityBeneficiaryModel",
           "act_rheader",
           )

import datetime

from gluon import *
from gluon.storage import Storage

from ..core import *

# =============================================================================
class ActivityModel(DataModel):
    """ Data Model for activities of an organisation """

    names = ("act_activity",
             "act_activity_id",
             "act_activity_type",
             )

    def model(self):

        T = current.T
        db = current.db

        s3 = current.response.s3
        crud_strings = s3.crud_strings

        define_table = self.define_table
        configure = self.configure

        # ---------------------------------------------------------------------
        # Activity Type
        #
        tablename = "act_activity_type"
        define_table(tablename,
                     Field("name",
                           label = T("Name"),
                           requires = IS_NOT_EMPTY(),
                           ),
                     Field("code", length=64,
                           label = T("Code"),
                           requires = IS_LENGTH(64, minsize=2),
                           ),
                     Field("obsolete", "boolean",
                           label = T("Obsolete"),
                           default = False,
                           represent = BooleanRepresent(icons=True, colors=True, flag=True),
                           ),
                     CommentsField(),
                     )

        # Table configuration
        configure(tablename,
                  deduplicate = S3Duplicate(primary = ("code",)),
                  )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Add Activity Type"),
            title_display = T("Activity Type"),
            title_list = T("Activity Types"),
            title_update = T("Edit Activity Type"),
            label_list_button = T("List Activity Types"),
            label_delete_button = T("Delete Activity Type"),
            msg_record_created = T("Activity Type added"),
            msg_record_modified = T("Activity Type updated"),
            msg_record_deleted = T("Activity Type deleted"),
            msg_list_empty = T("No Activity Types currently defined"),
            )

        # Field Template
        represent = S3Represent(lookup="act_activity_type")
        activity_type_id = FieldTemplate("type_id", "reference %s" % tablename,
                                         label = T("Activity Type"),
                                         ondelete = "RESTRICT",
                                         represent = represent,
                                         requires = IS_EMPTY_OR(
                                                        IS_ONE_OF(db, "%s.id" % tablename,
                                                                  represent,
                                                                  filterby = "obsolete",
                                                                  filter_opts = (False,),
                                                                  )),
                                         sortby = "name",
                                         )

        # ---------------------------------------------------------------------
        # Activities
        #
        tablename = "act_activity"
        define_table(tablename,
                     self.super_link("doc_id", "doc_entity"),
                     self.org_organisation_id(comment=False),
                     activity_type_id(empty=False),
                     Field("name",
                           label = T("Title"),
                           requires = IS_NOT_EMPTY(),
                           ),
                     # TODO Description?
                     DateField(label = T("Start Date"),
                               empty = False,
                               default = "now",
                               set_min = "#act_activity_end_date",
                               ),
                     # TODO Frequency (single occasion, regular activity)
                     DateField("end_date",
                               label = T("End Date"),
                               set_max = "#act_activity_date",
                               ),
                     # TODO Time formula? Separate event table?
                     Field("time",
                           label = T("Time"),
                           represent = lambda v, row=None: v if v else "-",
                           ),
                     # TODO Alternatives: location_id, site_id?
                     Field("place",
                           label = T("Place"),
                           represent = lambda v, row=None: v if v else "-",
                           ),
                     # TODO Total Effort (Hours)
                     # TODO Total Costs + Currency
                     # TODO Link to financing sector
                     # TODO Link to financing project/program?
                     CommentsField(),
                     )

        # Components
        self.add_components(tablename,
                            act_beneficiary = "activity_id",
                            )

        # Filter widgets
        # TODO Custom DateFilter (needs special interval filter)
        filter_widgets = [TextFilter(["name",
                                      "place",
                                      "time",
                                      "comments",
                                      ],
                                     label = T("Search"),
                                     ),
                          OptionsFilter("type_id"),
                          ]

        # Table configuration
        configure(tablename,
                  filter_widgets = filter_widgets,
                  onvalidation = self.activity_onvalidation,
                  super_entity = ("doc_entity",)
                  )

        # Field Template
        # TODO represent including date? place? time? sector?
        represent = S3Represent(lookup="act_activity")
        activity_id = FieldTemplate("activity_id", "reference %s" % tablename,
                                    label = T("Activity"),
                                    ondelete = "RESTRICT",
                                    represent = represent,
                                    requires = IS_EMPTY_OR(
                                                IS_ONE_OF(db, "%s.id" % tablename,
                                                          represent,
                                                          )),
                                    sortby = "name",
                                    )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Create Activity"),
            title_display = T("Activity Details"),
            title_list = T("Activities"),
            title_update = T("Edit Activity"),
            label_list_button = T("List Activities"),
            label_delete_button = T("Delete Activity"),
            msg_record_created = T("Activity added"),
            msg_record_modified = T("Activity updated"),
            msg_record_deleted = T("Activity deleted"),
            msg_list_empty = T("No Activities currently registered"),
            )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return {"act_activity_id": activity_id,
                }

    # -------------------------------------------------------------------------
    @staticmethod
    def defaults():
        """ Safe defaults for names in case the module is disabled """

        return {"act_activity_id": FieldTemplate.dummy("activity_id"),
                }

    # -------------------------------------------------------------------------
    @staticmethod
    def activity_onvalidation(form):
        """
            Form validation of activity
                - Date interval must include all registered beneficiaries
        """

        T = current.T
        db = current.db
        s3db = current.s3db

        table = s3db.act_activity

        record_id = get_form_record_id(form)
        if record_id:
            fields = ["date", "end_date"]
            data = get_form_record_data(form, table, fields)

            btable = s3db.act_beneficiary
            base = (btable.activity_id == record_id) & \
                   (btable.deleted == False)

            start = data.get("date")
            if start:
                earliest = datetime.datetime.combine(start, datetime.time(0))
                query = base & (btable.date < earliest)
                if db(query).select(btable.id, limitby=(0, 1)).first():
                    form.errors.date = T("There are beneficiaries registered before that date")

            end = data.get("end_date")
            if end:
                latest = datetime.datetime.combine(end + datetime.timedelta(days=1), datetime.time(0))
                query = base & (btable.date >= latest)
                if db(query).select(btable.id, limitby=(0, 1)).first():
                    form.errors.end_date = T("There are beneficiaries registered after that date")

# =============================================================================
class ActivityBeneficiaryModel(DataModel):
    """ Data Model to record beneficiaries of activities """

    names = ("act_beneficiary",
             )

    def model(self):

        T = current.T

        s3 = current.response.s3
        crud_strings = s3.crud_strings

        # ---------------------------------------------------------------------
        # Beneficiary (targeted by an activity at a certain date+time)
        #
        tablename = "act_beneficiary"
        self.define_table(tablename,
                          self.pr_person_id(label = T("Beneficiary"),
                                            empty = False,
                                            ondelete = "CASCADE",
                                            ),
                          self.act_activity_id(empty=False,
                                               ),
                          DateTimeField(default = "now",
                                        empty = False,
                                        future = 0,
                                        ),
                          CommentsField(),
                          )

        # List fields
        list_fields = ["activity_id",
                       "date",
                       "person_id",
                       "comments",
                       ]

        # Table configuration
        self.configure(tablename,
                       list_fields = list_fields,
                       orderby = "%s.date desc" % tablename,
                       onvalidation = self.beneficiary_onvalidation,
                       )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Add Beneficiary"),
            title_display = T("Beneficiary Details"),
            title_list = T("Beneficiaries"),
            title_update = T("Edit Beneficiary"),
            label_list_button = T("List Beneficiaries"),
            label_delete_button = T("Delete Beneficiary"),
            msg_record_created = T("Beneficiary added"),
            msg_record_modified = T("Beneficiary updated"),
            msg_record_deleted = T("Beneficiary deleted"),
            msg_list_empty = T("No Beneficiaries currently registered"),
            )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def beneficiary_onvalidation(form):
        """
            Form validation of beneficiary
                - Date must match activity date interval
        """

        T = current.T
        db = current.db
        s3db = current.s3db

        table = s3db.act_beneficiary

        fields = ["activity_id", "date"]
        data = get_form_record_data(form, table, fields)

        date = data.get("date")
        activity_id = data.get("activity_id")

        if date and activity_id:
            # Verify that date matches activity date interval
            error = None
            date = date.date()
            atable = s3db.act_activity
            query = (atable.id == activity_id)
            activity = db(query).select(atable.date,
                                        atable.end_date,
                                        limitby = (0, 1),
                                        ).first()
            if activity:
                start, end = activity.date, activity.end_date
                if start is not None and start > date:
                    error = T("Activity started only after that date")
                elif end is not None and end < date:
                    error = T("Activity ended before that date")
            if error:
                form.errors.date = error

# =============================================================================
def act_rheader(r, tabs=None):
    """ ACT resource headers """

    if r.representation != "html":
        # Resource headers only used in interactive views
        return None

    tablename, record = s3_rheader_resource(r)
    if tablename != r.tablename:
        resource = current.s3db.resource(tablename, id=record.id)
    else:
        resource = r.resource

    rheader = None
    rheader_fields = []

    if record:

        T = current.T

        if tablename == "act_activity":
            if not tabs:
                tabs = [(T("Basic Details"), None),
                        (T("Beneficiaries"), "beneficiary"),
                        (T("Documents"), "document"),
                        ]
            rheader_fields = [["type_id", "date"],
                              ["place"],
                              ["time"],
                              ]
            rheader_title = "name"

            rheader = S3ResourceHeader(rheader_fields, tabs, title=rheader_title)
            rheader = rheader(r, table=resource.table, record=record)

    return rheader

# END =========================================================================
