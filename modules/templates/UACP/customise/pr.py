"""
    PR module customisations for UACP

    License: MIT
"""

from gluon import current, URL, redirect, IS_EMPTY_OR, IS_NOT_EMPTY, TAG

# -------------------------------------------------------------------------
def person_onaccept(form):

    # Get record ID
    form_vars = form.vars
    if "id" in form_vars:
        record_id = form_vars.id
    elif hasattr(form, "record_id"):
        record_id = form.record_id
    else:
        return

    db = current.db
    s3db = current.s3db

    # Get the record
    table = s3db.pr_person
    query = (table.id == record_id)
    record = db(query).select(table.id,
                              table.pe_label,
                              limitby = (0, 1),
                              ).first()
    if not record:
        return

    if not record.pe_label:
        record.update_record(pe_label="C-%07d" % record_id)
        s3db.update_super(table, record)

# -------------------------------------------------------------------------
def pr_person_resource(r, tablename):

    s3db = current.s3db

    # Configure components to inherit realm_entity from
    # the person record incl. on realm updates
    s3db.configure("pr_person",
                   realm_components = ("assistance_measure",
                                       "case_activity",
                                       "case_language",
                                       "address",
                                       "contact",
                                       "contact_emergency",
                                       "group_membership",
                                       "image",
                                       "note",
                                       "person_details",
                                       "person_tag",
                                       ),
                   )

    # Custom callback to assign an ID
    s3db.add_custom_callback("pr_person", "onaccept", person_onaccept)

# -------------------------------------------------------------------------
def pr_person_controller(**attr):

    T = current.T
    s3db = current.s3db
    settings = current.deployment_settings

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        controller = r.controller

        # Never show all cases
        if controller == "br" and "closed" not in r.get_vars:
            r.get_vars.closed = "0"

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource
        table = resource.table

        from core import S3SQLCustomForm, \
                         S3SQLInlineComponent, \
                         StringTemplateParser

        # Determine order of name fields
        NAMES = ("first_name", "middle_name", "last_name")
        keys = StringTemplateParser.keys(settings.get_pr_name_format())
        name_fields = [fn for fn in keys if fn in NAMES]

        if controller == "br":

            # Configure anonymizer rules
            from ..anonymize import rlpcm_person_anonymize
            resource.configure(anonymize = rlpcm_person_anonymize())

            ctable = s3db.br_case
            record = r.record

            if not r.component:
                # Module-specific field and form configuration

                # Adapt fields to module context
                multiple_orgs = s3db.br_case_read_orgs()[0]

                # Configure pe_label (r/o, auto-generated onaccept)
                field = table.pe_label
                field.label = T("ID")
                field.readable = bool(record)
                field.writable = False

                # Hide gender
                field = table.gender
                field.default = None
                field.readable = field.writable = False

                # Address
                if settings.get_br_case_address():
                    address = S3SQLInlineComponent(
                                    "address",
                                    label = T("Current Address"),
                                    fields = [("", "location_id")],
                                    filterby = {"field": "type",
                                                "options": "1",
                                                },
                                    link = False,
                                    multiple = False,
                                    )
                else:
                    address = None

                # If there is a default status for new cases,
                # hide the status field in create-form
                field = ctable.status_id
                if not record and field.default:
                    field.readable = field.writable = False

                # Configure case.organisation_id
                field = ctable.organisation_id
                field.comment = None
                if not current.auth.s3_has_role("RELIEF_PROVIDER"):
                    ctable = s3db.br_case
                    field.default = settings.get_org_default_organisation()
                    field.readable = field.writable = bool(field.default)
                else:
                    default_org, selectable = s3db.br_case_default_org()
                    if default_org:
                        field.writable = selectable
                        field.readable = selectable or multiple_orgs
                    field.default = default_org
                requires = field.requires
                if isinstance(requires, IS_EMPTY_OR):
                    field.requires = requires.other

                # CRUD form
                crud_fields = ["case.date",
                               "case.organisation_id",
                               "case.human_resource_id",
                               "case.status_id",
                               "pe_label",
                               # +name fields
                               "case.household_size",
                               address,
                               S3SQLInlineComponent(
                                    "contact",
                                    fields = [("", "value")],
                                    filterby = {"field": "contact_method",
                                                "options": "SMS",
                                                },
                                    label = T("Mobile Phone"),
                                    multiple = False,
                                    name = "phone",
                                    ),
                               "case.comments",
                               "case.invalid",
                               ]

                # Filters
                from core import LocationFilter, \
                                 OptionsFilter, \
                                 TextFilter, \
                                 get_filter_options
                filter_widgets = [TextFilter(["pe_label",
                                              "last_name",
                                              "first_name",
                                              ],
                                              label = T("Search"),
                                              ),
                                    LocationFilter("address.location_id",
                                                   levels = ("L2", "L3"),
                                                   ),
                                    ]

                # List fields
                list_fields = ["pe_label",
                               # +name fields
                               "case.date",
                               "case.status_id",
                               ]

                # Add organisation if user can see cases from multiple orgs
                if multiple_orgs:
                    filter_widgets.insert(-1,
                        OptionsFilter("case.organisation_id",
                                      options = lambda: get_filter_options("org_organisation"),
                                      ))
                    list_fields.insert(-2, "case.organisation_id")

                # Insert name fields in name-format order
                NAMES = ("first_name", "middle_name", "last_name")
                keys = StringTemplateParser.keys(settings.get_pr_name_format())
                name_fields = [fn for fn in keys if fn in NAMES]
                crud_fields[5:5] = name_fields
                list_fields[1:1] = name_fields

                resource.configure(crud_form = S3SQLCustomForm(*crud_fields),
                                   filter_widgets = filter_widgets,
                                   list_fields = list_fields,
                                   )

        elif controller == "default":
            # Personal profile (default/person)

            # Configure Anonymizer
            from core import S3Anonymize
            s3db.set_method("pr_person",
                            method = "anonymize",
                            action = S3Anonymize,
                            )
            if r.method == "anonymize" and \
                r.http == "POST" and r.representation == "json":
                # Override standard prep blocking non-interactive requests
                result = True

            # Configure anonymizer rules
            from ..anonymize import rlpcm_person_anonymize
            resource.configure(anonymize = rlpcm_person_anonymize())

            if not r.component:

                # Last name is required
                table = r.resource.table
                table.last_name.requires = IS_NOT_EMPTY()

                # Custom Form
                crud_fields = name_fields
                address = S3SQLInlineComponent(
                                "address",
                                label = T("Current Address"),
                                fields = [("", "location_id")],
                                filterby = {"field": "type",
                                            "options": "1",
                                            },
                                link = False,
                                multiple = False,
                                )
                crud_fields.append(address)
                r.resource.configure(crud_form = S3SQLCustomForm(*crud_fields),
                                     deletable = False,
                                     )
        return result
    s3.prep = prep

    standard_postp = s3.postp
    def custom_postp(r, output):

        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if r.controller in ("br", "default") and \
            not r.component and isinstance(output, dict):

            if r.record and r.method in (None, "update", "read"):

                # Custom CRUD buttons
                if "buttons" not in output:
                    buttons = output["buttons"] = {}
                else:
                    buttons = output["buttons"]

                # Anonymize-button
                from core import S3AnonymizeWidget
                anonymize = S3AnonymizeWidget.widget(r, _class="action-btn anonymize-btn")

                # Render in place of the delete-button
                buttons["delete_btn"] = TAG[""](anonymize)
        return output
    s3.postp = custom_postp

    # Custom rheader
    c = current.request.controller
    from ..rheaders import rlpcm_profile_rheader, rlpcm_br_rheader
    if c == "default":
        # Logout post-anonymize if the user has removed their account
        auth = current.auth
        user = auth.user
        if user:
            utable = auth.settings.table_user
            account = current.db(utable.id == user.id).select(utable.deleted,
                                                              limitby=(0, 1),
                                                              ).first()
            if not account or account.deleted:
                redirect(URL(c="default", f="user", args=["logout"]))
        else:
            redirect(URL(c="default", f="index"))
        attr["rheader"] = rlpcm_profile_rheader
    elif c == "br":
        attr["rheader"] = rlpcm_br_rheader

    return attr

# END =========================================================================
