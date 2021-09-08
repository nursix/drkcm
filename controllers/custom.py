# -*- coding: utf-8 -*-

"""
    Re-routed (custom) controllers
"""

# -----------------------------------------------------------------------------
def rest():
    """
        Vanilla RESTful CRUD controller
    """

    c, f = request.args[:2]
    request.args = request.args[2:]

    request.controller, request.function = c, f

    rest_controllers = settings.get_base_rest_controllers()
    resource = rest_controllers.get((c, f))
    if isinstance(resource, tuple) and len(resource) == 2:
        prefix, name = resource
    else:
        prefix, name = c, f

    return s3_rest_controller(prefix, name)

# END =========================================================================
