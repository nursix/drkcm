"""
    DRK DVR Extensions

    Copyright: 2024 (c) Sahana Software Foundation

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

__all__ = ("DVRSiteActivityModel",
           )

import os

from gluon import current, URL, IS_INT_IN_RANGE, A
from gluon.storage import Storage

from s3dal import Field

from core import CommentsField, DataModel, DateField, DateFilter, OptionsFilter

# =============================================================================
class DVRSiteActivityModel(DataModel):
    """ Model to record the activity of a site over time """

    names = ("dvr_site_activity",
             )

    def model(self):

        T = current.T

        s3 = current.response.s3
        settings = current.deployment_settings

        crud_strings = s3.crud_strings

        configure = self.configure
        define_table = self.define_table

        SITE = settings.get_org_site_label()
        site_represent = self.org_SiteRepresent(show_link=False)

        default_site = settings.get_org_default_site()
        permitted_facilities = current.auth.permitted_facilities(redirect_on_error=False)

        # ---------------------------------------------------------------------
        # Site Activity
        #
        tablename = "dvr_site_activity"
        define_table(tablename,
                     self.super_link("site_id", "org_site",
                                     default = default_site,
                                     filterby = "site_id",
                                     filter_opts = permitted_facilities,
                                     label = SITE,
                                     readable = not default_site,
                                     writable = not default_site,
                                     represent = site_represent,
                                     updateable = True,
                                     ),
                     DateField(future=0),
                     Field("old_total", "integer",
                           default = 0,
                           label = T("Previous Total"),
                           requires = IS_INT_IN_RANGE(0, None),
                           ),
                     Field("cases_new", "integer",
                           default = 0,
                           label = T("Admissions"),
                           requires = IS_INT_IN_RANGE(0, None),
                           ),
                     Field("cases_closed", "integer",
                           default = 0,
                           label = T("Departures"),
                           requires = IS_INT_IN_RANGE(0, None),
                           ),
                     Field("new_total", "integer",
                           default = 0,
                           label = T("Current Total"),
                           requires = IS_INT_IN_RANGE(0, None),
                           ),
                     Field("report", "upload",
                           autodelete = True,
                           label = T("Report"),
                           length = current.MAX_FILENAME_LENGTH,
                           represent = self.report_represent,
                           uploadfolder = os.path.join(current.request.folder,
                                                       "uploads",
                                                       "dvr",
                                                       ),
                           ),
                     CommentsField(),
                     )

        # CRUD Strings
        crud_strings[tablename] = Storage(
            label_create = T("Create Activity Report"),
            title_display = T("Activity Report"),
            title_list = T("Activity Reports"),
            title_update = T("Edit Activity Report"),
            label_list_button = T("List Activity Reports"),
            label_delete_button = T("Delete Activity Report"),
            msg_record_created = T("Activity Report created"),
            msg_record_modified = T("Activity Report updated"),
            msg_record_deleted = T("Activity Report deleted"),
            msg_list_empty = T("No Activity Reports found"),
        )

        # Filter widgets
        date_filter = DateFilter("date")
        date_filter.operator = ["eq"]
        filter_widgets = [date_filter]
        if not default_site:
            site_filter = OptionsFilter("site_id",
                                        label = SITE,
                                        )
            filter_widgets.insert(0, site_filter)

        # Table configuration
        configure(tablename,
                  filter_widgets = filter_widgets,
                  )

        # ---------------------------------------------------------------------
        # Pass names back to global scope (s3.*)
        #
        return None

    # -------------------------------------------------------------------------
    def defaults(self):
        """ Safe defaults for names in case the module is disabled """

        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def report_represent(value):
        """ File representation """

        if value:
            try:
                # Read the filename from the file
                filename = current.db.dvr_site_activity.report.retrieve(value)[0]
            except IOError:
                return current.T("File not found")
            else:
                return A(filename,
                         _href=URL(c="default", f="download", args=[value]))
        else:
            return current.messages["NONE"]

# END =========================================================================
