"""
    PR module customisations for RLPPTM

    License: MIT
"""

from gluon import current, IS_NOT_EMPTY

from core import get_form_record_id

# -----------------------------------------------------------------------------
def add_person_tags():
    """
        Person tags as filtered components
            - for embedding in form
    """

    s3db = current.s3db

    s3db.add_components("pr_person",
                        pr_person_tag = ({"name": "tax_id",
                                          "joinby": "person_id",
                                          "filterby": {"tag": "TAXID"},
                                          "multiple": False,
                                          },
                                         ),
                        )

# -----------------------------------------------------------------------------
def person_postprocess(form):
    """
        Postprocess person-form
            - update manager info status tag for all organisations
              for which the person is marked as test station manager
    """

    record_id = get_form_record_id(form)
    if not record_id:
        return

    # Lookup active HR records with org_contact flag
    db = current.db
    s3db = current.s3db

    table = s3db.hrm_human_resource
    query = (table.person_id == record_id) & \
            (table.org_contact == True) & \
            (table.status == 1) & \
            (table.deleted == False)
    rows = db(query).select(table.organisation_id,
                            groupby = table.organisation_id,
                            )

    # Update manager info status tag for each org
    from .org import update_mgrinfo
    for row in rows:
        update_mgrinfo(row.organisation_id)

# -----------------------------------------------------------------------------
def pr_person_resource(r, tablename):

    s3db = current.s3db

    # Configure components to inherit realm_entity from person
    s3db.configure("pr_person",
                    realm_components = ("person_details",
                                        "contact",
                                        "address",
                                        ),
                    )

# -----------------------------------------------------------------------------
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

        controller = r.controller
        if controller in ("default", "hrm") and not r.component:
            # Personal profile (default/person) or staff

            # Last name is required
            table = r.resource.table
            table.last_name.requires = IS_NOT_EMPTY()

            # Custom Form
            crud_fields = name_fields + ["date_of_birth", "gender"]

            # Expose Tax ID in personal profile
            if controller == "default":

                add_person_tags()

                component = r.resource.components.get("tax_id")
                field = component.table.value
                # Not translated as specific for the German context:
                field.comment = "Nur Teststellenverantwortliche: Ihre bei der Kassenärztl. Vereinigung angegebene Steuer-ID"

                crud_fields.append((T("Tax ID"), "tax_id.value"))

            r.resource.configure(crud_form = S3SQLCustomForm(*crud_fields,
                                                             postprocess = person_postprocess,
                                                             ),
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

# -----------------------------------------------------------------------------
def contact_update_mgrinfo(record_id, pe_id=None):
    """
        Updates the manager info status tag of related organisations

        Args:
            record_id: the pr_contact record_id
    """

    db = current.db
    s3db = current.s3db

    ctable = s3db.pr_contact
    ptable = s3db.pr_person
    htable = s3db.hrm_human_resource

    join = [htable.on((htable.person_id == ptable.id) & \
                      (htable.deleted == False)),
            ]
    if pe_id:
        query = (ptable.pe_id == pe_id)
    else:
        join.insert(0, ptable.on(ptable.pe_id == ctable.pe_id))
        query = (ctable.id == record_id)
    rows = db(query).select(htable.organisation_id, join=join)

    from .org import update_mgrinfo
    for row in rows:
        update_mgrinfo(row.organisation_id)

# -----------------------------------------------------------------------------
def contact_ondelete(row):

    contact_update_mgrinfo(row.id, pe_id=row.pe_id)

# -----------------------------------------------------------------------------
def contact_onaccept(form):

    record_id = get_form_record_id(form)
    if not record_id:
        return
    contact_update_mgrinfo(record_id)

# -----------------------------------------------------------------------------
def pr_contact_resource(r, tablename):

    s3db = current.s3db

    s3db.add_custom_callback("pr_contact", "onaccept", contact_onaccept)
    s3db.add_custom_callback("pr_contact", "ondelete", contact_ondelete)

# END =========================================================================
