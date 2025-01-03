# =============================================================================
#   Final actions before running controllers
# =============================================================================

# Pass Theme to Compiler
settings.set_theme()

# Empty dict to store custom CRUD views
s3.views = {}

if request.controller != "default":
    if session.s3.pending_consent:
        # Enforce consent response
        redirect(URL(c="default", f="user", args=["consent"], vars={"_next": URL()}))
    elif session.s3.mandatory_page:
        # Enforce mandatory page
        # (that page must reset sessions.s3.mandatory_page when satisfied)
        mandatory = settings.get_auth_mandatory_page()
        if mandatory:
            next_url = mandatory() if callable(mandatory) else mandatory
        else:
            next_url = None
        if next_url:
            redirect(next_url)

if auth.permission.format in ("html",):

    # Should we use Content-Delivery Networks?
    s3.cdn = settings.get_base_cdn()

    # Compose the options menu
    controller = request.controller
    if controller not in s3_menu_dict:
        # No custom menu, so use standard menu for this controller
        menu.options = OptionsMenu(controller).menu
    else:
        # Use custom menu
        menu.options = s3_menu_dict[controller]

    # Add breadcrumbs
    menu.breadcrumbs = OptionsMenu.breadcrumbs

# Re-route controller
c, f = request.controller, request.function
if c == "custom":
    # Must not be accessed directly
    raise HTTP(404, 'invalid controller (%s/%s)' % (c, f))

rest_controllers = settings.get_base_rest_controllers()
if rest_controllers and (c, f) in rest_controllers:
    request.args = [c, f] + request.args
    request.controller = "custom"
    request.function = "index" if f == "index" else "rest"

# END =========================================================================
