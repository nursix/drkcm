"""
    SUPPLY module customisations for MRCMS

    License: MIT
"""

from gluon import current

from core import IS_ONE_OF, S3SQLCustomForm, S3SQLInlineLink

# -------------------------------------------------------------------------
def supply_distribution_type_controller(**attr):

    T = current.T
    db = current.db
    s3db = current.s3db

    s3 = current.response.s3

    # Custom prep
    standard_prep = s3.prep
    def prep(r):

        # Call standard prep
        result = standard_prep(r) if callable(standard_prep) else True

        resource = r.resource
        table = resource.table

        # Enable residents_only flag
        field = table.residents_only
        field.readable = field.writable = True

        if not r.component:

            # Organisation is required
            from ..helpers import permitted_orgs
            organisations = permitted_orgs("update", "supply_distribution_type")

            otable = s3db.org_organisation
            dbset = db(otable.id.belongs(organisations))

            field = table.organisation_id
            field.requires = IS_ONE_OF(dbset, "org_organisation.id",
                                       field.represent,
                                       sort = True,
                                       zero = None,
                                       )

            if len(organisations) == 1:
                field.default = organisations[0]
                field.readable = field.writable = False

                ftable = s3db.dvr_case_flag
                dbset = db(ftable.organisation_id == organisations[0])

                for alias in ("flag_required", "flag_debarring"):
                    component = resource.components.get(alias)
                    ltable = component.link.table
                    field = ltable.flag_id
                    field.requires = IS_ONE_OF(dbset, "dvr_case_flag.id",
                                               field.represent,
                                               sort=True,
                                               )
            else:
                field.default = None
                field.readable = field.writable = True

                # Multiple organisations
                # => must filter flag options dynamically (not ideal UX-wise)
                jquery_ready = s3.jquery_ready
                filter_opts = lambda alias: '''
$.filterOptionsS3({
 'trigger':'organisation_id',
 'target':{'name':'flag_id','alias':'%s','inlineType':'link'},
 'lookupPrefix':'dvr',
 'lookupResource':'case_flag',
 'showEmptyField':false
})''' % alias
                for alias in ("flag_required", "flag_debarring"):
                    script = filter_opts(alias)
                    if script not in jquery_ready:
                        jquery_ready.append(script)

            # Embed flags_required/flags_debarring in CRUD form
            crud_form = S3SQLCustomForm("organisation_id",
                                        "name",
                                        "max_per_day",
                                        "min_interval",
                                        "residents_only",
                                        S3SQLInlineLink("flag_required",
                                                        label = T("Case Flags Required"),
                                                        field = "flag_id",
                                                        render_list = True,
                                                        ),
                                        S3SQLInlineLink("flag_debarring",
                                                        label = T("Case Flags Debarring"),
                                                        field = "flag_id",
                                                        render_list = True,
                                                        ),
                                        "active",
                                        "comments",
                                        )

            list_fields = ["name",
                           "max_per_day",
                           "min_interval",
                           "residents_only",
                           "active",
                           "comments",
                           ]
            resource.configure(crud_form = crud_form,
                               list_fields = list_fields,
                               )

        return result
    s3.prep = prep

    return attr

# END =========================================================================
