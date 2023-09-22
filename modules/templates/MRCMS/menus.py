"""
    Custom menus for MRCMS

    License: MIT
"""

from gluon import current, URL
from core import IS_ISO639_2_LANGUAGE_CODE
from s3layouts import MM, M, ML, MP, MA
try:
    from .layouts import OM
except ImportError:
    pass
import s3menus as default

from .helpers import get_default_organisation, get_default_shelter

# =============================================================================
class S3MainMenu(default.S3MainMenu):
    """ Custom Application Main Menu """

    # -------------------------------------------------------------------------
    @classmethod
    def menu(cls):
        """ Compose Menu """

        # Modules menus
        main_menu = MM()(
            cls.menu_modules(),
        )

        # Additional menus
        current.menu.personal = cls.menu_personal()
        current.menu.lang = cls.menu_lang()
        current.menu.about = cls.menu_about()
        current.menu.org = cls.menu_org()

        return main_menu

    # -------------------------------------------------------------------------
    @classmethod
    def menu_modules(cls):
        """ Custom Modules Menu """

        has_permission = current.auth.s3_has_permission

        # Single or multiple organisations?
        if has_permission("create", "org_organisation"):
            organisation_id = None
        else:
            organisation_id = get_default_organisation()
        if organisation_id:
            org_menu = MM("Organization", c="org", f="organisation", args=[organisation_id])
        else:
            org_menu = MM("Organizations", c="org", f="organisation")

        # Single or multiple shelters?
        if has_permission("create", "cr_shelter"):
            shelter_id = None
        else:
            shelter_id = get_default_shelter()
        if shelter_id:
            shelter_menu = MM("Shelter", c="cr", f="shelter", args=[shelter_id])
        else:
            shelter_menu = MM("Shelters", c="cr", f="shelter")

        #Clients
            #dvr/person
        #Shelter(s)
            #Shelter if default shelter and not permitted to create shelters else Shelter list
            #Create if permitted to create shelters
            #Presence Registration if default shelter
        #Organisation(s) <= plural if not single or permitted to create orgs
            #Organisation if default organisation and not permitted to create orgs else Organisation list
            #Staff
        #Security <= requires default organisation/shelter
            #Confiscation
            #Presence list if default shelter

        return [
            MM("Clients", c=("dvr", "pr"), f="person"),
            shelter_menu,
            org_menu,
            MM("Security", c="security", f="seized_item"),
            ]

    # -------------------------------------------------------------------------
    @classmethod
    def menu_org(cls):
        """ Custom Organisation Menu """

        return OM()

    # -------------------------------------------------------------------------
    @classmethod
    def menu_lang(cls, **attr):

        languages = current.deployment_settings.get_L10n_languages()
        represent_local = IS_ISO639_2_LANGUAGE_CODE.represent_local

        # Language selector
        menu_lang = ML("Language", right=True)
        for code in languages:
            # Show Language in it's own Language
            lang_name = represent_local(code)
            menu_lang(
                ML(lang_name, translate=False, lang_code=code, lang_name=lang_name)
                )
        return menu_lang

    # -------------------------------------------------------------------------
    @classmethod
    def menu_personal(cls):
        """ Custom Personal Menu """

        auth = current.auth
        settings = current.deployment_settings

        sr = current.auth.get_system_roles()
        ADMIN = sr.ADMIN

        if not auth.is_logged_in():
            request = current.request
            login_next = URL(args=request.args, vars=request.vars)
            if request.controller == "default" and \
               request.function == "user" and \
               "_next" in request.get_vars:
                login_next = request.get_vars["_next"]

            #self_registration = settings.get_security_self_registration()
            menu_personal = MP()(
                        #MP("Register", c="default", f="user",
                        #   m = "register",
                        #   check = self_registration,
                        #   ),
                        MP("Login", c="default", f="user",
                           m = "login",
                           vars = {"_next": login_next},
                           ),
                        )
            if settings.get_auth_password_retrieval():
                menu_personal(MP("Lost Password", c="default", f="user",
                                 m = "retrieve_password",
                                 ),
                              )
        else:
            s3_has_role = auth.s3_has_role
            is_user_admin = lambda i: \
                            s3_has_role(sr.ORG_ADMIN, include_admin=False) or \
                            s3_has_role(sr.ORG_GROUP_ADMIN, include_admin=False)

            menu_personal = MP()(
                        MP("Administration", c="admin", f="index",
                           restrict = ADMIN,
                           ),
                        MP("Administration", c="admin", f="user",
                           check = is_user_admin,
                           ),
                        MP("Profile", c="default", f="person"),
                        MP("Change Password", c="default", f="user",
                           m = "change_password",
                           ),
                        MP("Logout", c="default", f="user",
                           m = "logout",
                           ),
                        )

        return menu_personal

    # -------------------------------------------------------------------------
    @classmethod
    def menu_about(cls):

        ADMIN = current.auth.get_system_roles().ADMIN

        menu_about = MA(c="default")(
                MA("Help", f="help"),
                #MA("Contact", f="contact"),
                MA("Version", f="about", restrict = ADMIN),
                )

        return menu_about

# =============================================================================
class S3OptionsMenu(default.S3OptionsMenu):
    """ Custom Controller Menus """

    # -------------------------------------------------------------------------
    @staticmethod
    def cr():
        """ CR / Shelter Registry """

        # Single or multiple shelters?
        if current.auth.s3_has_permission("create", "cr_shelter"):
            shelter_id = None
        else:
            shelter_id = get_default_shelter()

        if not shelter_id:
            menu = M(c="cr")(
                        M("Shelters", f="shelter")(
                            M("Create", m="create"),
                            ),
                        )
        else:

            #ADMIN = current.auth.get_system_roles().ADMIN

            menu = M(c="cr")(
                        M("Shelter", f="shelter", args=[shelter_id])(
                            M("Overview",
                            args = [shelter_id, "profile"],
                            ),
                            M("Housing Units",
                            t = "cr_shelter_unit",
                            args = [shelter_id, "shelter_unit"],
                            ),
                        ),
                        #M("Room Inspection", f = "shelter", link=False)(
                        #    M("Register",
                        #      args = [shelter_id, "inspection"],
                        #      t = "cr_shelter_inspection",
                        #      p = "create",
                        #      ),
                        #    M("Overview", f = "shelter_inspection"),
                        #    M("Defects", f = "shelter_inspection_flag"),
                        #    ),
                        #M("Administration",
                        #  link = False,
                        #  restrict = (ADMIN, "ADMIN_HEAD"),
                        #  selectable=False,
                        #  )(
                        #    M("Shelter Flags", f="shelter_flag"),
                        #    ),
                    )

        return menu

    # -------------------------------------------------------------------------
    @classmethod
    def hrm(cls):

        return cls.org()

    # -------------------------------------------------------------------------
    @staticmethod
    def dvr():
        """ DVR / Disaster Victim Registry """

        due_followups = current.s3db.dvr_due_followups() or "0"
        follow_up_label = "%s (%s)" % (current.T("Due Follow-ups"),
                                       due_followups,
                                       )

        sr = current.auth.get_system_roles()
        ADMIN = sr.ADMIN
        ORG_ADMIN = sr.ORG_ADMIN

        return M(c="dvr")(
                M("Current Cases", c=("dvr", "pr"), f="person")(
                    M("Create", m="create", t="pr_person", p="create"),
                    M("All Cases", vars = {"closed": "include"}),
                    ),
                #M("Reports", link=False)(
                #    M("Check-in overdue", c=("dvr", "pr"), f="person",
                #      restrict = (ADMIN, ORG_ADMIN, "CASE_ADMIN"),
                #      vars = {"overdue": "check-in"},
                #      ),
                #    M("Food Distribution overdue", c=("dvr", "pr"), f="person",
                #      restrict = (ADMIN, ORG_ADMIN, "CASE_ADMIN"),
                #      vars = {"overdue": "FOOD*"},
                #      ),
                #    M("Clients Reports", c="dvr", f="site_activity",
                #      ),
                #    M("Food Distribution Statistics", c="dvr", f="case_event",
                #      m = "report",
                #      restrict = (ADMIN, ORG_ADMIN),
                #      vars = {"code": "FOOD*"},
                #      ),
                #    ),
                M("Current Needs", f="case_activity")(
                    M("Emergencies", vars={"~.emergency": "True"}),
                    M(follow_up_label, f="due_followups"),
                    M("Report", m="report"),
                    ),
                M("Appointments", f="case_appointment")(
                    M("Overview"),
                    #M("Import Updates", m="import", p="create",
                    #  restrict = (ADMIN, ORG_ADMIN, "CASE_ADMIN"),
                    #  ),
                    #M("Bulk Status Update", m="manage", p="update",
                    #  restrict = (ADMIN, ORG_ADMIN, "CASE_ADMIN"),
                    #  ),
                    ),
                #M("Event Registration", c="dvr", f="case_event", m="register", p="create"),
                #M("Food Distribution", c="dvr", f="case_event", m="register_food", p="create"),
                M("Archive", link=False)(
                    M("Closed Cases", f="person",
                        restrict = (ADMIN, ORG_ADMIN, "CASE_ADMIN"),
                        vars={"closed": "only"},
                        ),
                    M("Invalid Cases", f="person",
                        vars={"archived": "1"},
                        restrict = (ADMIN, ORG_ADMIN),
                        ),
                    ),
                M("Administration", link=False, restrict=(ADMIN, ORG_ADMIN))(
                    # Org-specific types
                    M("Flags", f="case_flag"),
                    M("Appointment Types", f="case_appointment_type"),
                    #M("Event Types", f="case_event_type"),

                    # Global types
                    M("Case Status", f="case_status", restrict=ADMIN),
                    M("Need Types", f="need", restrict=ADMIN),
                    M("Residence Status Types", f="residence_status_type", restrict=ADMIN),
                    M("Residence Permit Types", f="residence_permit_type", restrict=ADMIN),
                    ),
                )

    # -------------------------------------------------------------------------
    @staticmethod
    def org():
        """ ORG / Organization Registry """

        ADMIN = current.session.s3.system_roles.ADMIN

        # Single or multiple organisations?
        if current.auth.s3_has_permission("create", "org_organisation"):
            organisation_id = None
        else:
            organisation_id = get_default_organisation()
        if organisation_id:
            org_menu = M("Organization", c="org", f="organisation", args=[organisation_id])
        else:
            org_menu = M("Organizations", c="org", f="organisation")(
                            M("Create", m="create"),
                            )

        return M(c=("org", "hrm"))(
                    org_menu,
                    M("Organization Groups", f="group")(
                        M("Create", m="create"),
                        ),
                    M("Staff", c="hrm", f="staff"),
                    M("Administration", link=False, restrict=[ADMIN])(
                        M("Organization Types", f="organisation_type"),
                        ),
                    )

    ## -------------------------------------------------------------------------
    #@staticmethod
    #def project():
    #    """ PROJECT / Project/Task Management """
    #
    #    return M(c="project")(
    #             M("Tasks", f="task")(
    #                M("Create", m="create"),
    #                M("My Open Tasks", vars={"mine":1}),
    #             ),
    #            )
    #
    # -------------------------------------------------------------------------
    @staticmethod
    def security():
        """ SECURITY / Security Management """

        return M(c="security")(
                M("Confiscation", f="seized_item")(
                    M("Create", m="create"),
                    M("Item Types", f="seized_item_type"),
                    M("Depositories", f="seized_item_depository"),
                    ),
                )

# END =========================================================================
