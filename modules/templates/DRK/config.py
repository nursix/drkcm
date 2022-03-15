"""
    DRK ("Village"): Case Management, Refugee Reception Center, German Red Cross

    License: MIT
"""

import datetime

from collections import OrderedDict

from gluon import current
from gluon.storage import Storage

def config(settings):

    T = current.T

    settings.base.system_name = "Village"
    settings.base.system_name_short = "Village"

    # PrePopulate data
    settings.base.prepopulate += ("DRK",)
    settings.base.prepopulate_demo += ("DRK/Demo",)

    # Theme (folder to use for views/layout.html)
    settings.base.theme = "DRK"

    # Authentication settings
    # Should users be allowed to register themselves?
    settings.security.self_registration = False
    # Do new users need to verify their email address?
    #settings.auth.registration_requires_verification = True
    # Do new users need to be approved by an administrator prior to being able to login?
    #settings.auth.registration_requires_approval = True
    settings.auth.registration_requests_organisation = True
    settings.auth.registration_link_user_to = {"staff": T("Staff"),
                                               "volunteer": T("Volunteer"),
                                               }
    #settings.auth.registration_link_user_to_default = ["staff"]

    # Approval emails get sent to all admins
    settings.mail.approver = "ADMIN"

    # Restrict the Location Selector to just certain countries
    # NB This can also be over-ridden for specific contexts later
    # e.g. Activities filtered to those of parent Project
    settings.gis.countries = ("DE",)
    # Uncomment to display the Map Legend as a floating DIV
    settings.gis.legend = "float"
    # Uncomment to Disable the Postcode selector in the LocationSelector
    #settings.gis.postcode_selector = False # @ToDo: Vary by country (include in the gis_config!)
    # Uncomment to show the Print control:
    # http://eden.sahanafoundation.org/wiki/UserGuidelines/Admin/MapPrinting
    #settings.gis.print_button = True

    # Settings suitable for Housing Units
    # - move into customise fn if also supporting other polygons
    settings.gis.precision = 5
    settings.gis.simplify_tolerance = 0
    settings.gis.bbox_min_size = 0.001
    #settings.gis.bbox_offset = 0.007

    # L10n settings
    # Languages used in the deployment (used for Language Toolbar & GIS Locations)
    # http://www.loc.gov/standards/iso639-2/php/code_list.php
    settings.L10n.languages = OrderedDict([
       ("en", "English"),
       ("de", "German"),
    ])
    # Default language for Language Toolbar (& GIS Locations in future)
    settings.L10n.default_language = "de"
    # Uncomment to Hide the language toolbar
    #settings.L10n.display_toolbar = False
    # Default timezone for users
    settings.L10n.timezone = "Europe/Berlin"
    # Number formats (defaults to ISO 31-0)
    # Decimal separator for numbers (defaults to ,)
    settings.L10n.decimal_separator = "."
    # Thousands separator for numbers (defaults to space)
    settings.L10n.thousands_separator = ","
    # Uncomment this to Translate Layer Names
    #settings.L10n.translate_gis_layer = True
    # Uncomment this to Translate Location Names
    #settings.L10n.translate_gis_location = True
    # Uncomment this to Translate Organisation Names/Acronyms
    #settings.L10n.translate_org_organisation = True
    # Finance settings
    settings.fin.currencies = {
        "EUR" : "Euros",
    #    "GBP" : "Great British Pounds",
    #    "USD" : "United States Dollars",
    }
    settings.fin.currency_default = "EUR"

    # Security Policy
    # http://eden.sahanafoundation.org/wiki/S3AAA#System-widePolicy
    # 1: Simple (default): Global as Reader, Authenticated as Editor
    # 2: Editor role required for Update/Delete, unless record owned by session
    # 3: Apply Controller ACLs
    # 4: Apply both Controller & Function ACLs
    # 5: Apply Controller, Function & Table ACLs
    # 6: Apply Controller, Function, Table ACLs and Entity Realm
    # 7: Apply Controller, Function, Table ACLs and Entity Realm + Hierarchy
    # 8: Apply Controller, Function, Table ACLs, Entity Realm + Hierarchy and Delegations
    #
    settings.security.policy = 5 # Controller, Function & Table ACLs

    # Version details on About-page require login
    settings.security.version_info_requires_login = True

    # -------------------------------------------------------------------------
    # Defaults for custom settings
    #
    settings.custom.autogenerate_case_ids = True

    # -------------------------------------------------------------------------
    # General UI settings
    #
    settings.ui.calendar_clear_icon = True

    from .customise.cr import profile_header

    settings.ui.profile_header = profile_header

    # -------------------------------------------------------------------------
    # AUTH Settings
    #
    from .customise.auth import auth_user_resource

    settings.customise_auth_user_resource = auth_user_resource

    # -------------------------------------------------------------------------
    # CMS Settings
    #
    settings.cms.hide_index = True

    # -------------------------------------------------------------------------
    # CR Settings
    #
    #settings.cr.day_and_night = False
    settings.cr.shelter_population_dynamic = True
    settings.cr.shelter_housing_unit_management = True
    settings.cr.check_out_is_final = False

    # Generate tasks for shelter inspections
    settings.cr.shelter_inspection_tasks = True
    settings.cr.shelter_inspection_task_active_statuses = (2, 3, 6)

    from .customise.cr import cr_shelter_controller, \
                              cr_shelter_registration_resource, \
                              cr_shelter_registration_controller

    settings.customise_cr_shelter_controller = cr_shelter_controller
    settings.customise_cr_shelter_registration_resource = cr_shelter_registration_resource
    settings.customise_cr_shelter_registration_controller = cr_shelter_registration_controller

    # -------------------------------------------------------------------------
    # DVR Settings and Customizations
    #
    # Uncomment this to enable tracking of transfer origin/destination sites
    settings.dvr.track_transfer_sites = True
    # Uncomment this to enable features to manage transferability of cases
    settings.dvr.manage_transferability = True
    # Uncomment this to enable household size in cases, set to "auto" for automatic counting
    settings.dvr.household_size = "auto"
    # Uncomment this to enable features to manage case flags
    settings.dvr.case_flags = True
    # Case activities use single Needs
    #settings.dvr.case_activity_needs_multiple = True
    # Uncomment this to expose flags to mark appointment types as mandatory
    settings.dvr.mandatory_appointments = True
    # Uncomment this to have appointments with personal presence update last_seen_on
    settings.dvr.appointments_update_last_seen_on = True
    # Uncomment this to have allowance payments update last_seen_on
    settings.dvr.payments_update_last_seen_on = True
    # Uncomment this to automatically update the case status when appointments are completed
    settings.dvr.appointments_update_case_status = True
    # Uncomment this to automatically close appointments when registering certain case events
    settings.dvr.case_events_close_appointments = True
    # Configure a regular expression pattern for ID Codes (QR Codes)
    settings.dvr.id_code_pattern = "(?P<label>[^,]*),(?P<family>[^,]*),(?P<last_name>[^,]*),(?P<first_name>[^,]*),(?P<date_of_birth>[^,]*),.*"
    # Issue a "not checked-in" warning in case event registration
    settings.dvr.event_registration_checkin_warning = True
    # Exclude FOOD and SURPLUS-MEALS events from event registration
    settings.dvr.event_registration_exclude_codes = ("FOOD*", "SURPLUS-MEALS")

    from .customise.dvr import dvr_home, \
                               dvr_allowance_controller, \
                               dvr_case_resource, \
                               dvr_case_activity_resource, \
                               dvr_case_activity_controller, \
                               dvr_case_appointment_controller, \
                               dvr_case_event_resource, \
                               dvr_case_event_controller, \
                               dvr_case_event_type_resource, \
                               dvr_note_resource, \
                               dvr_site_activity_resource

    settings.customise_dvr_home = dvr_home
    settings.customise_dvr_allowance_controller = dvr_allowance_controller
    settings.customise_dvr_case_resource = dvr_case_resource
    settings.customise_dvr_case_activity_resource = dvr_case_activity_resource
    settings.customise_dvr_case_activity_controller = dvr_case_activity_controller
    settings.customise_dvr_case_appointment_controller = dvr_case_appointment_controller
    settings.customise_dvr_case_event_resource = dvr_case_event_resource
    settings.customise_dvr_case_event_controller = dvr_case_event_controller
    settings.customise_dvr_case_event_type_resource = dvr_case_event_type_resource
    settings.customise_dvr_note_resource = dvr_note_resource
    settings.customise_dvr_site_activity_resource = dvr_site_activity_resource

    # -------------------------------------------------------------------------
    # Human Resource Module Settings
    #
    settings.hrm.teams_orgs = False

    # -------------------------------------------------------------------------
    # Inventory Module Settings
    #
    settings.inv.facility_label = "Facility"
    settings.inv.facility_manage_staff = False

    # -------------------------------------------------------------------------
    # Organisations Module Settings
    #
    settings.org.default_organisation = "Deutsches Rotes Kreuz"
    settings.org.default_site = "Erstaufnahme Mannheim"

    from .customise.org import org_facility_resource, \
                               org_facility_controller

    settings.customise_org_facility_resource = org_facility_resource
    settings.customise_org_facility_controller = org_facility_controller

    # -------------------------------------------------------------------------
    # Persons Module Settings
    #
    settings.pr.hide_third_gender = False
    settings.pr.separate_name_fields = 2
    settings.pr.name_format= "%(last_name)s, %(first_name)s"

    from .customise.pr import pr_person_resource, \
                              pr_person_controller, \
                              pr_group_membership_controller

    settings.customise_pr_person_resource = pr_person_resource
    settings.customise_pr_person_controller = pr_person_controller
    settings.customise_pr_group_membership_controller = pr_group_membership_controller

    # -------------------------------------------------------------------------
    # Project Module Settings
    #
    settings.project.mode_task = True
    settings.project.sectors = False

    # NB should not add or remove options, but just comment/uncomment
    settings.project.task_status_opts = {#1: T("Draft"),
                                         2: T("New"),
                                         3: T("Assigned"),
                                         #4: T("Feedback"),
                                         #5: T("Blocked"),
                                         6: T("On Hold"),
                                         7: T("Canceled"),
                                         #8: T("Duplicate"),
                                         #9: T("Ready"),
                                         #10: T("Verified"),
                                         #11: T("Reopened"),
                                         12: T("Completed"),
                                         }

    settings.project.task_time = False
    settings.project.my_tasks_include_team_tasks = True

    from .customise.project import project_task_resource

    settings.customise_project_task_resource = project_task_resource

    # -------------------------------------------------------------------------
    # Requests Module Settings
    #
    settings.req.req_type = ("Stock",)
    settings.req.use_commit = False
    settings.req.recurring = False


    # -------------------------------------------------------------------------
    # Security settings
    #
    from .customise.security import security_seized_item_resource

    settings.customise_security_seized_item_resource = security_seized_item_resource

    # -------------------------------------------------------------------------
    def org_site_check(site_id):
        """ Custom tasks for scheduled site checks """

        # Update transferability
        from .controllers import update_transferability
        result = update_transferability(site_id=site_id)

        # Log the result
        msg = "Update Transferability: " \
              "%s transferable cases found for site %s" % (result, site_id)
        current.log.info(msg)

        # Check whether we have a site activity report for yesterday
        YESTERDAY = current.request.utcnow.date() - datetime.timedelta(1)
        rtable = current.s3db.dvr_site_activity
        query = (rtable.date == YESTERDAY) & \
                (rtable.site_id == site_id) & \
                (rtable.deleted != True)
        row = current.db(query).select(rtable.id,
                                       limitby = (0, 1)
                                       ).first()
        if not row:
            # Create one
            from .helpers import DRKSiteActivityReport
            report = DRKSiteActivityReport(date = YESTERDAY,
                                           site_id = site_id,
                                           )
            # Temporarily override authorization,
            # otherwise the report would be empty
            auth = current.auth
            auth.override = True
            try:
                record_id = report.store()
            except:
                record_id = None
            auth.override = False
            if record_id:
                current.log.info("Residents Report created, record ID=%s" % record_id)
            else:
                current.log.error("Could not create Residents Report")

    settings.org.site_check = org_site_check

    # -------------------------------------------------------------------------
    # Comment/uncomment modules here to disable/enable them
    # Modules menu is defined in modules/eden/menu.py
    settings.modules = OrderedDict([
        # Core modules which shouldn't be disabled
        ("default", Storage(
            name_nice = T("Home"),
            restricted = False, # Use ACLs to control access to this module
            access = None,      # All Users (inc Anonymous) can see this module in the default menu & access the controller
            module_type = None  # This item is not shown in the menu
        )),
        ("admin", Storage(
            name_nice = T("Administration"),
            #description = "Site Administration",
            restricted = True,
            access = "|1|",     # Only Administrators can see this module in the default menu & access the controller
            module_type = None  # This item is handled separately for the menu
        )),
        ("appadmin", Storage(
            name_nice = T("Administration"),
            #description = "Site Administration",
            restricted = True,
            module_type = None  # No Menu
        )),
        ("errors", Storage(
            name_nice = T("Ticket Viewer"),
            #description = "Needed for Breadcrumbs",
            restricted = False,
            module_type = None  # No Menu
        )),
        ("gis", Storage(
            name_nice = T("Map"),
            #description = "Situation Awareness & Geospatial Analysis",
            restricted = True,
            module_type = 6,     # 6th item in the menu
        )),
        ("pr", Storage(
            name_nice = T("Person Registry"),
            #description = "Central point to record details on People",
            restricted = True,
            access = "|1|",     # Only Administrators can see this module in the default menu (access to controller is possible to all still)
            module_type = 10
        )),
        ("org", Storage(
            name_nice = T("Organizations"),
            #description = 'Lists "who is doing what & where". Allows relief agencies to coordinate their activities',
            restricted = True,
            module_type = 1
        )),
        ("hrm", Storage(
           name_nice = T("Staff"),
           #description = "Human Resources Management",
           restricted = True,
           module_type = 2,
        )),
        ("vol", Storage(
           name_nice = T("Volunteers"),
           #description = "Human Resources Management",
           restricted = True,
           module_type = 2,
        )),
        ("cms", Storage(
         name_nice = T("Content Management"),
        #description = "Content Management System",
         restricted = True,
         module_type = 10,
        )),
        ("doc", Storage(
           name_nice = T("Documents"),
           #description = "A library of digital resources, such as photos, documents and reports",
           restricted = True,
           module_type = 10,
        )),
        ("msg", Storage(
           name_nice = T("Messaging"),
           #description = "Sends & Receives Alerts via Email & SMS",
           restricted = True,
           # The user-visible functionality of this module isn't normally required. Rather it's main purpose is to be accessed from other modules.
           module_type = None,
        )),
        ("supply", Storage(
           name_nice = T("Supply Chain Management"),
           #description = "Used within Inventory Management, Request Management and Asset Management",
           restricted = True,
           module_type = None, # Not displayed
        )),
        ("inv", Storage(
           name_nice = T("Warehouses"),
           #description = "Receiving and Sending Items",
           restricted = True,
           module_type = 4
        )),
        ("asset", Storage(
           name_nice = T("Assets"),
           #description = "Recording and Assigning Assets",
           restricted = True,
           module_type = 5,
        )),
        ("req", Storage(
           name_nice = T("Requests"),
           #description = "Manage requests for supplies, assets, staff or other resources. Matches against Inventories where supplies are requested.",
           restricted = True,
           module_type = 10,
        )),
        ("project", Storage(
           name_nice = T("Projects"),
           #description = "Tracking of Projects, Activities and Tasks",
           restricted = True,
           module_type = 2
        )),
        ("cr", Storage(
            name_nice = T("Shelters"),
            #description = "Tracks the location, capacity and breakdown of victims in Shelters",
            restricted = True,
            module_type = 10
        )),
        ("dvr", Storage(
          name_nice = T("Residents"),
          #description = "Allow affected individuals & households to register to receive compensation and distributions",
          restricted = True,
          module_type = 10,
        )),
        ("event", Storage(
           name_nice = T("Events"),
           #description = "Activate Events (e.g. from Scenario templates) for allocation of appropriate Resources (Human, Assets & Facilities).",
           restricted = True,
           module_type = 10,
        )),
        ("security", Storage(
           name_nice = T("Security"),
           restricted = True,
           module_type = 10,
        )),
        ("stats", Storage(
           name_nice = T("Statistics"),
           #description = "Manages statistics",
           restricted = True,
           module_type = None,
        )),
    ])

# END =========================================================================
