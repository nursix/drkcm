"""
    Custom Menus for GIMS

    License: MIT
"""

from gluon import current, URL
from core import IS_ISO639_2_LANGUAGE_CODE
from s3layouts import MM, M
try:
    from .layouts import *
except ImportError:
    pass
import s3menus as default

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
        """ Modules Menu """

        auth = current.auth

        is_org_group_admin = auth.s3_has_role("ORG_GROUP_ADMIN")

        return [MM("Organizations", c="org", f="organisation", check=is_org_group_admin),
                MM("Organizations", c="org", f="organisation", check=not is_org_group_admin)(
                    MM("My Organizations", vars={"mine": "1"}),
                    MM("All Organizations"),
                    ),
                MM("Shelters", c="cr", f="shelter", restrict=("SHELTER_MANAGER", "SHELTER_READER")),
                MM("Newsletters", c="cms", f="read_newsletter"),
                ]

    # -------------------------------------------------------------------------
    @classmethod
    def menu_org(cls):
        """ Organisation Logo and Name """

        OM = S3OrgMenuLayout
        return OM()

    # -------------------------------------------------------------------------
    @classmethod
    def menu_lang(cls):
        """ Language Selector """

        languages = current.deployment_settings.get_L10n_languages()
        represent_local = IS_ISO639_2_LANGUAGE_CODE.represent_local

        menu_lang = ML("Language", right=True)

        for code in languages:
            # Show each language name in its own language
            lang_name = represent_local(code)
            menu_lang(
                ML(lang_name,
                   translate = False,
                   lang_code = code,
                   lang_name = lang_name,
                   )
            )

        return menu_lang

    # -------------------------------------------------------------------------
    @classmethod
    def menu_personal(cls):
        """ Personal Menu """

        auth = current.auth
        #s3 = current.response.s3
        settings = current.deployment_settings

        ADMIN = current.auth.get_system_roles().ADMIN

        if not auth.is_logged_in():
            request = current.request
            login_next = URL(args=request.args, vars=request.vars)
            if request.controller == "default" and \
               request.function == "user" and \
               "_next" in request.get_vars:
                login_next = request.get_vars["_next"]

            self_registration = settings.get_security_self_registration()
            menu_personal = MP()(
                        MP("Register", c="default", f="user",
                           m = "register",
                           check = self_registration,
                           ),
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
            is_org_admin = lambda i: not s3_has_role(ADMIN) and \
                                     s3_has_role("ORG_ADMIN")
            menu_personal = MP()(
                        MP("Administration", c="admin", f="index",
                           restrict = ADMIN,
                           ),
                        MP("Administration", c="admin", f="user",
                           check = is_org_admin,
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

        menu_about = MA(c="default")(
            MA("Help", f="help"),
            MA("Contact", f="contact"),
            MA("Privacy", f="index", args=["privacy"]),
            MA("Legal Notice", f="index", args=["legal"]),
            MA("Version", f="about", restrict = ("ORG_GROUP_ADMIN")),
        )
        return menu_about

# =============================================================================
class S3OptionsMenu(default.S3OptionsMenu):
    """ Custom Controller Menus """

    # -------------------------------------------------------------------------
    @classmethod
    def cms(cls):

        if current.request.function in ("read_newsletter", "newsletter"):
            menu = M(c="cms")(
                    M("Newsletters", c="cms", f="read_newsletter")(
                        M("Inbox", f="read_newsletter",
                          check = lambda this: this.following()[0].check_permission(),
                          ),
                        M("Compose and Send", f="newsletter", p="create"),
                        ),
                    )
        else:
            menu = super().cms()

        return menu

    # -------------------------------------------------------------------------
    @staticmethod
    def cr():
        """ CR / Shelter Registry """

        ADMIN = current.auth.get_system_roles().ADMIN

        return M(c="cr")(
                    M("Shelters", f="shelter")(
                        M("Create", m="create"),
                        M("Map", m="map"),
                        ),
                    M("Statistics", link=False)(
                        M("Capacity", f="shelter", m="report"),
                        M("Current Population##shelter", f="shelter_population", m="report"),
                        ),
                    M("Administration", link=False, restrict=(ADMIN,))(
                        M("Shelter Types", f="shelter_type"),
                        M("Shelter Services", f="shelter_service"),
                        M("Population Types", f="population_type"),
                        ),
                )

    # -------------------------------------------------------------------------
    @staticmethod
    def org():
        """ ORG / Organization Registry """

        auth = current.auth

        sysroles = auth.get_system_roles()

        ADMIN = sysroles.ADMIN
        ORG_ADMIN = sysroles.ORG_ADMIN
        ORG_GROUP_ADMIN = sysroles.ORG_GROUP_ADMIN

        is_org_group_admin = auth.s3_has_role(ORG_GROUP_ADMIN)

        return M(c="org")(
                    M("Organizations", f="organisation")(
                        M("My Organizations", vars={"mine": 1},
                          restrict=ORG_ADMIN,
                          check=not is_org_group_admin,
                          ),
                        M("All Organizations",
                          restrict=ORG_ADMIN,
                          check=not is_org_group_admin,
                          ),
                        M("Create Organization", m="create", restrict=ORG_GROUP_ADMIN),
                        ),
                    M("Administration", restrict=ADMIN)(
                        M("Organization Groups", f="group"),
                        M("Organization Types", f="organisation_type"),
                        #M("Sectors", f="sector"),
                        )
                    )

# END =========================================================================
