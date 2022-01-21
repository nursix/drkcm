"""
    RLPPTM Disease Monitoring Extension

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

__all__ = ("DiseaseDaycareTestingInquiryModel",
           "disease_daycare_testing_get_pending_responders",
           )

from collections import OrderedDict

from gluon import current, Field, \
                  IS_EMPTY_OR, IS_INT_IN_RANGE, IS_IN_SET
from gluon.sqlhtml import OptionsWidget
from gluon.storage import Storage

from core import DataModel, OptionsFilter, TextFilter, \
                 get_form_record_id, represent_option, \
                 s3_comments, s3_meta_fields, s3_yes_no_represent

# =============================================================================
class DiseaseDaycareTestingInquiryModel(DataModel):
    """ Model for Data Inquiry on Daycare Testing """

    names = ("disease_daycare_testing",
             )

    def model(self):

        T = current.T

        #db = current.db
        s3 = current.response.s3

        define_table = self.define_table
        crud_strings = s3.crud_strings

        # ---------------------------------------------------------------------
        # Daycare Testing Inquiry
        #
        frequency_opts = (("WEEKLY<1", T("Less than weekly")),
                          ("WEEKLY=1", T("Weekly")),
                          ("WEEKLY>1", T("Several times a week")),
                          )
        boolean_opts = ((True, T("yes")),
                        (False, T("no")),
                        )
        is_boolean = lambda v: (True, None) if v in ("True", True) else \
                               (False, None) if v in ("False", False) else \
                               (None, None)

        tablename = "disease_daycare_testing"
        define_table(tablename,
                     self.org_organisation_id(empty=False, comment=None),
                     Field("daycare_testing", "boolean",
                           label = T("Testing in daycare centers"),
                           default = None,
                           represent = s3_yes_no_represent,
                           requires = [IS_IN_SET(boolean_opts,
                                                 sort = None,
                                                 error_message = T("input required"),
                                                 ),
                                       is_boolean,
                                       ],
                           widget = OptionsWidget.widget,
                           ),
                     Field("regular_testing", "boolean",
                           label = T("Regular testing"),
                           default = None,
                           represent = s3_yes_no_represent,
                           requires = [IS_EMPTY_OR(IS_IN_SET(boolean_opts, sort=None)),
                                       is_boolean,
                                       ],
                           widget = OptionsWidget.widget,
                           ),
                     Field("frequency",
                           label = T("Frequency##activity"),
                           default = None,
                           represent = represent_option(dict(frequency_opts)),
                           requires = IS_EMPTY_OR(IS_IN_SET(frequency_opts,
                                                            zero = None,
                                                            sort = None,
                                                            )),
                           ),
                     Field("number_of_dc", "integer",
                           label = T("Number of daycare centers"),
                           default = None,
                           represent = lambda v, row=None: str(v) if v is not None else "-",
                           requires = IS_EMPTY_OR(IS_INT_IN_RANGE(1, 1000)),
                           ),
                     s3_comments(),
                     *s3_meta_fields())

        # Filter Widgets
        filter_widgets = [TextFilter(["organisation_id$name"],
                                     label = T("Search"),
                                     ),
                          OptionsFilter("daycare_testing",
                                        options = OrderedDict(boolean_opts),
                                        cols = 2,
                                        hidden = True,
                                        sort = False,
                                        ),
                          OptionsFilter("regular_testing",
                                        options = OrderedDict(boolean_opts),
                                        cols = 2,
                                        hidden = True,
                                        sort = False,
                                        ),
                          OptionsFilter("frequency",
                                        options = OrderedDict(frequency_opts),
                                        cols = 3,
                                        hidden = True,
                                        sort = False,
                                        ),
                          ]

        # Report Options
        axes = ["daycare_testing",
                "regular_testing",
                "frequency",
                "organisation_id",
                ]
        facts = [(T("Number of Organizations"), "count(organisation_id)"),
                 (T("Number of Daycare Centers"), "sum(number_of_dc)"),
                 ]
        report_options = {"rows": axes,
                          "cols": axes,
                          "fact": facts,
                          "defaults": {"rows": axes[0],
                                       "cols": axes[2],
                                       "fact": facts[0],
                                       }}

        # Table Configuration
        self.configure(tablename,
                       filter_widgets = filter_widgets,
                       onvalidation = self.daycare_testing_onvalidation,
                       report_options = report_options,
                       )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Create Response"),
            title_display = T("Response Details"),
            title_list = T("Data Inquiry on Testing in Daycare Centers"),
            title_update = T("Edit Response"),
            label_list_button = T("List Responses"),
            label_delete_button = T("Delete Response"),
            msg_record_created = T("Response created"),
            msg_record_modified = T("Response updated"),
            msg_record_deleted = T("Response deleted"),
            msg_list_empty = T("No Responses currently registered"),
            )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def daycare_testing_onvalidation(form):
        """
            Onvalidation of responses to daycare testing inquiry
                - make sure there is only one response per organisation
                - check mandatory inputs
        """

        T = current.T

        s3db = current.s3db
        table = s3db.disease_daycare_testing

        form_vars = form.vars
        record_id = get_form_record_id(form)

        # Make sure this isn't a duplicate
        if "organisation_id" in form_vars:
            organisation_id = form_vars.organisation_id
        else:
            organisation_id = table.organisation_id.default

        query = (table.organisation_id == organisation_id)
        if record_id:
            query &= (table.id != record_id)
        query &= (table.deleted == False)
        duplicate = current.db(query).select(table.id,
                                             limitby = (0, 1),
                                             ).first()
        if duplicate:
            form.errors.organisation_id = T("Response for this organisation already registered")
            return

        daycare_testing = form_vars.daycare_testing
        if daycare_testing:
            # Check mandatory inputs
            is_required = T("input required")
            regular_testing = form_vars.regular_testing
            if regular_testing is None:
                form.errors.regular_testing = is_required
            elif regular_testing:
                if not form_vars.frequency:
                    form.errors.frequency = is_required
                if not form_vars.number_of_dc:
                    form.errors.number_of_dc = is_required
        else:
            # Override any inputs in case-specific fields
            form_vars.regular_testing = False
            form_vars.frequency = None
            form_vars.number_of_dc = None

# =============================================================================
def disease_daycare_testing_get_pending_responders(managed_orgs):
    """
        Identify managed organisations which have yet to respond to
        the daycare testing inquiry

        Args:
            managed_orgs: list of organisation_ids managed by the user
    """

    s3db = current.s3db
    otable = s3db.org_organisation
    rtable = s3db.disease_daycare_testing

    left = rtable.on((rtable.organisation_id == otable.id) & \
                     (rtable.deleted == False))
    query = (otable.id.belongs(managed_orgs)) & \
            (otable.deleted == False) & \
            (rtable.id == None)
    rows = current.db(query).select(otable.id, left=left)

    return [row.id for row in rows]

# END =========================================================================
