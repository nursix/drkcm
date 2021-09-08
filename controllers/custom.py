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

    request.controller = c
    request.function = f

    return s3_rest_controller()

# END =========================================================================
