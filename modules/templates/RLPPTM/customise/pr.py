"""
    PR module customisations for RLPPTM

    License: MIT
"""

from gluon import current, IS_NOT_EMPTY

# -------------------------------------------------------------------------
def pr_person_resource(r, tablename):

    s3db = current.s3db

    # Configure components to inherit realm_entity from person
    s3db.configure("pr_person",
                    realm_components = ("person_details",
                                        "contact",
                                        "address",
                                        ),
                    )

# -------------------------------------------------------------------------
def pr_person_controller(**attr):

    s3 = current.response.s3
    settings = current.deployment_settings

    T = current.T

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Restrict data formats
        from ..helpers import restrict_data_formats
        restrict_data_formats(r)

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        from core import S3SQLCustomForm, StringTemplateParser

        # Determine order of name fields
        NAMES = ("first_name", "middle_name", "last_name")
        keys = StringTemplateParser.keys(settings.get_pr_name_format())
        name_fields = [fn for fn in keys if fn in NAMES]

        if r.controller in ("default", "hrm") and not r.component:
            # Personal profile (default/person) or staff

            # Last name is required
            table = r.resource.table
            table.last_name.requires = IS_NOT_EMPTY()

            # Custom Form
            crud_fields = name_fields + ["date_of_birth", "gender"]
            r.resource.configure(crud_form = S3SQLCustomForm(*crud_fields),
                                 deletable = False,
                                 )

        if r.component_name == "address":
            ctable = r.component.table

            # Configure location selector and geocoder
            from core import S3LocationSelector
            field = ctable.location_id
            field.widget = S3LocationSelector(levels = ("L1", "L2", "L3", "L4"),
                                              required_levels = ("L1", "L2", "L3"),
                                              show_address = True,
                                              show_postcode = True,
                                              show_map = True,
                                              )
            s3.scripts.append("/%s/static/themes/RLP/js/geocoderPlugin.js" % r.application)

        elif r.component_name == "human_resource":

            phone_label = settings.get_ui_label_mobile_phone()
            r.component.configure(list_fields= ["job_title_id",
                                                "site_id",
                                                (T("Email"), "person_id$email.value"),
                                                (phone_label, "person_id$phone.value"),
                                                "status",
                                                ],
                                  deletable = False,
                                  )
            s3.crud_strings["hrm_human_resource"]["label_list_button"] = T("List Staff Records")

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import rlpptm_profile_rheader, rlpptm_hr_rheader
    controller = current.request.controller
    if controller == "default":
        attr["rheader"] = rlpptm_profile_rheader
    elif controller == "hrm":
        attr["rheader"] = rlpptm_hr_rheader

    return attr


# END =========================================================================
