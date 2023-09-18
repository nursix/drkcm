"""
    HRM module customisations for MRCMS

    License: MIT
"""

from gluon import current

# -------------------------------------------------------------------------
def hrm_human_resource_resource(r, tablename):

    T = current.T
    s3db = current.s3db

    phone_label = current.deployment_settings.get_ui_label_mobile_phone()
    list_fields = ["organisation_id",
                   "person_id",
                   "job_title_id",
                   "site_id",
                   (T("Email"), "person_id$email.value"),
                   (phone_label, "person_id$phone.value"),
                   "status",
                   ]

    s3db.configure("hrm_human_resource",
                   list_fields = list_fields,
                   )

    # Configure components to inherit realm_entity from person
    s3db.configure("pr_person",
                   realm_components = ("person_details",
                                       "contact",
                                       "address",
                                       "identity",
                                       ),
                   )

# -------------------------------------------------------------------------
def hrm_human_resource_controller(**attr):

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        if not r.component:
            current.deployment_settings.ui.open_read_first = True

        return result

    s3.prep = prep

    return attr

# END =========================================================================
