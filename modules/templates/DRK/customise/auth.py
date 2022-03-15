"""
    AUTH module customisations for DRK

    License: MIT
"""

from gluon import current

# -------------------------------------------------------------------------
def auth_user_resource(r, tablename):

    settings = current.deployment_settings

    table = current.s3db.auth_user
    field = table.organisation_id

    field.default = settings.get_org_default_organisation()

# END =========================================================================
