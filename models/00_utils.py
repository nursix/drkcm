# -*- coding: utf-8 -*-

"""
    Common Utilities run most requests
"""

# =============================================================================
# Special local requests (e.g. from scheduler)
#
if request.is_local:
    # This is a request made from the local server

    f = get_vars.get("format", None)
    auth_token = get_vars.get("subscription", None)
    if auth_token and f == "msg":
        # Subscription lookup request (see S3Notifications.notify())
        rtable = s3db.pr_subscription_resource
        stable = s3db.pr_subscription
        utable = s3db.pr_person_user
        join = [stable.on(stable.id == rtable.subscription_id),
                utable.on(utable.pe_id == stable.pe_id)]

        user = db(rtable.auth_token == auth_token).select(utable.user_id,
                                                          join=join,
                                                          limitby=(0, 1)) \
                                                  .first()
        if user:
            # Impersonate subscriber
            auth.s3_impersonate(user.user_id)
        else:
            # Anonymous request
            auth.s3_impersonate(None)

# =============================================================================
# Check Permissions & fail as early as we can
#
# Set user roles
# - requires access to tables
auth.s3_set_roles()

# Check access to this controller
if not auth.permission.has_permission("read"):
    auth.permission.fail()

# =============================================================================
# Initialize Date/Time Settings
#
s3base.s3_get_tzinfo()

# =============================================================================
# Menus
#
from s3layouts import *
import s3menus as default_menus

S3MainMenu = default_menus.S3MainMenu
S3OptionsMenu = default_menus.S3OptionsMenu

current.menu = Storage(oauth="", options=None, override={})
if auth.permission.format in ("html"):

    # NB cascading templates:
    #
    # - uses the last of S3MainMenu/S3OptionsMenu definition in the
    #   template cascade
    # - templates can override just one of S3MainMenu/S3OptionsMenu,
    #   while "inheriting" the other one from the cascade
    # - final fallback is the default menu
    # - layouts.py is always loaded from the *theme* location, so that
    #   the HTML matches the theme's CSS.
    #
    # Example:
    #
    # - have an S3MainMenu in templates/MY/SUB/menus.py
    # - settings.template = ["MY", "MY.SUB"]
    # - settings.theme = "MY"
    # => will use:
    # - Layouts from templates/MYTEMPLATE/layouts.py
    # - S3MainMenu from templates/MY/SUB/menus.py
    # - S3OptionsMenu from templates/MY/menus.py

    menu_locations = []
    template = settings.get_template()
    if template != "default":
        if isinstance(template, (tuple, list)):
            menu_locations.extend(template[::-1])
        else:
            menu_locations.append(template)

    if menu_locations:
        custom_main_menu = custom_options_menu = False

        package = "applications.%s.modules.templates.%%s.menus" % appname
        for name in menu_locations:
            if name == "default":
                continue
            try:
                deployment_menus = __import__(package % name,
                                              fromlist=["S3MainMenu",
                                                        "S3OptionsMenu",
                                                        ],
                                              )
            except ImportError:
                # No menus.py (using except is faster than os.stat)
                continue
            else:
                if not custom_main_menu and \
                   hasattr(deployment_menus, "S3MainMenu"):
                    S3MainMenu = deployment_menus.S3MainMenu
                    custom_main_menu = True
                if not custom_options_menu and \
                   hasattr(deployment_menus, "S3OptionsMenu"):
                    S3OptionsMenu = deployment_menus.S3OptionsMenu
                    custom_options_menu = True
                if custom_main_menu and custom_options_menu:
                    break

    # Instantiate main menu
    main = S3MainMenu.menu()
else:
    main = None

menu = current.menu
menu["main"] = main

# Override controller menus
# @todo: replace by current.menu.override
s3_menu_dict = {}

# Enable access to this function from modules
from core import crud_controller
current.crud_controller = crud_controller

# END =========================================================================
