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
                           requires = IS_LENGTH(64),
                           ),
                     Field("obsolete", "boolean",
                           label = T("Obsolete"),
                           default = False,
                           represent = BooleanRepresent(icons=True, colors=True, flag=True),
                           ),
                     CommentsField(),
                     )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Add Activity Type"),
            title_display = T("Activity Type"),
            title_list = T("Activity Types"),
            title_update = T("Edit Activity Type"),
            label_list_button = T("List ActivitY Types"),
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
                                                                  # Filter out obsolete types
                                                                  # from reference selectors:
                                                                  not_filterby = "obsolete",
                                                                  not_filter_opts = (True,),
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
                           ),
                     # TODO Alternatives: location_id, site_id?
                     Field("place",
                           label = T("Place"),
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
        # TODO DateFilter
        filter_widgets = [TextFilter(["name",
                                      "place",
                                      "time",
                                      "comments",
                                      ]),
                          OptionsFilter("type_id"),
                          ]

        # Table configuration
        configure(tablename,
                  filter_widgets = filter_widgets,
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
        # TODO onvalidation to check date against activity date interval
        # TODO onaccept to trigger dvr_update_last_seen
        self.configure(tablename,
                       list_fields = list_fields,
                       orderby = "%s.date desc" % tablename,
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
