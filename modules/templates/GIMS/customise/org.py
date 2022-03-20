"""
    ORG module customisations for GIMS

    License: MIT
"""

from gluon import current

from ..helpers import restrict_data_formats

# -------------------------------------------------------------------------
def org_organisation_controller(**attr):

    T = current.T
    settings = current.deployment_settings
    s3 = current.response.s3

    # Enable bigtable features
    settings.base.bigtable = True

    # Custom prep
    standard_prep = s3.prep
    def prep(r):
        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        auth = current.auth
        s3db = current.s3db

        resource = r.resource

        is_org_group_admin = auth.s3_has_role("ORG_GROUP_ADMIN")

        # Add invite-method for ORG_GROUP_ADMIN role
        from templates.RLPPTM.helpers import InviteUserOrg
        s3db.set_method("org_organisation",
                        method = "invite",
                        action = InviteUserOrg,
                        )

        mine = False
        if not is_org_group_admin:

            if r.get_vars.get("mine") == "1":
                mine = True
                # Filter to those the user can update
                aquery = current.auth.s3_accessible_query("update", "org_organisation")
                if aquery:
                    resource.add_filter(aquery)

            # Restrict data formats
            restrict_data_formats(r)

        if not r.component:
            if r.interactive:

                from core import S3SQLCustomForm, \
                                 S3SQLInlineComponent, \
                                 S3SQLInlineLink, \
                                 OptionsFilter, \
                                 TextFilter, \
                                 get_filter_options

                # Custom form
                if is_org_group_admin:
                    types = S3SQLInlineLink("organisation_type",
                                            field = "organisation_type_id",
                                            search = False,
                                            label = T("Type"),
                                            multiple = settings.get_org_organisation_types_multiple(),
                                            widget = "multiselect",
                                            )
                else:
                    types = None

                crud_fields = ["name",
                               "acronym",
                               types,
                               S3SQLInlineComponent(
                                    "contact",
                                    fields = [("", "value")],
                                    filterby = {"field": "contact_method",
                                                "options": "EMAIL",
                                                },
                                    label = T("Email"),
                                    multiple = False,
                                    name = "email",
                                    ),
                               "phone",
                               "website",
                               "logo",
                               "comments",
                               ]

                # Filters
                text_fields = ["name",
                               "acronym",
                               "website",
                               "phone",
                               ]
                if is_org_group_admin:
                    text_fields.append("email.value")
                if not mine:
                    text_fields.extend(["office.location_id$L3",
                                        "office.location_id$L1",
                                        ])
                filter_widgets = [TextFilter(text_fields,
                                             label = T("Search"),
                                             ),
                                  ]
                if is_org_group_admin:
                    filter_widgets.extend([
                        OptionsFilter(
                            "organisation_type__link.organisation_type_id",
                            label = T("Type"),
                            options = lambda: get_filter_options("org_organisation_type"),
                            ),
                        ])

                resource.configure(crud_form = S3SQLCustomForm(*crud_fields),
                                   filter_widgets = filter_widgets,
                                   )

            # Custom list fields
            if is_org_group_admin:
                list_fields = ["name",
                               "organisation_type__link.organisation_type_id",
                               (T("Description"), "comments"),
                               "office.location_id$L3",
                               "office.location_id$L1",
                               "website",
                               "phone",
                               (T("Email"), "email.value"),
                               ]
            elif not mine:
                list_fields = ["name",
                               "organisation_type__link.organisation_type_id",
                               (T("Description"), "comments"),
                               #"office.location_id$L3",
                               #"office.location_id$L1",
                               "website",
                               "phone",
                               ]
                if auth.user:
                    list_fields[3:3] = ("office.location_id$L3",
                                        "office.location_id$L1",
                                        )

            else:
                list_fields = ["name",
                               (T("Description"), "comments"),
                               "website",
                               "phone",
                               (T("Email"), "email.value"),
                               ]

            r.resource.configure(list_fields = list_fields)

        elif r.component_name == "office":

            ctable = r.component.table

            field = ctable.phone1
            field.label = T("Phone #")

        return result
    s3.prep = prep

    # Custom rheader
    from ..rheaders import org_rheader
    attr["rheader"] = org_rheader

    return attr

# END =========================================================================
