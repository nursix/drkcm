"""
    Activity Management - Controllers
"""

module = request.controller
resourcename = request.function

if not settings.has_module(module):
    raise HTTP(404, body="Module disabled: %s" % module)

# =============================================================================
def index():
    """ Module's Home Page """

    return s3db.cms_index(module, alt_function="index_alt")

# -----------------------------------------------------------------------------
def index_alt():
    """
        Module homepage for non-Admin users when no CMS content found
    """

    # Just redirect to the list of activities
    s3_redirect_default(URL(f="activity"))

# =============================================================================
def activity_type():
    """ Activity Types: CRUD Controller """

    return crud_controller()

# -----------------------------------------------------------------------------
def activity():
    """ Activities: CRUD Controller """

    return crud_controller(rheader=s3db.act_rheader)

# END =========================================================================
