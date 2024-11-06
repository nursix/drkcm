"""
    SUPPLY module customisations for MRCMS

    License: MIT
"""

from gluon import current

from core import CustomController, IS_ONE_OF, S3SQLCustomForm, S3SQLInlineLink

# -------------------------------------------------------------------------
def supply_distribution_set_controller(**attr):

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

        if not r.component:

            # Organisation is required
            from ..helpers import permitted_orgs
            organisations = permitted_orgs("update", "supply_distribution_set")

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

            # Custom list fields
            list_fields = ["name",
                           "active",
                           "max_per_day",
                           "min_interval",
                           "residents_only",
                           "comments",
                           ]
            if table.organisation_id.readable:
                list_fields.insert(0, "organisation_id")

            resource.configure(crud_form = crud_form,
                               list_fields = list_fields,
                               )

        return result
    s3.prep = prep

    return attr

# -------------------------------------------------------------------------
def supply_distribution_resource(r, tablename):

    s3db = current.s3db

    # Read-only (except via registration UI)
    s3db.configure("supply_distribution",
                   insertable = False,
                   editable = False,
                   deletable = False,
                   )

# -------------------------------------------------------------------------
def supply_distribution_controller(**attr):

    s3 = current.response.s3

    # Custom postp
    standard_postp = s3.postp
    def custom_postp(r, output):
        # Call standard postp
        if callable(standard_postp):
            output = standard_postp(r, output)

        if r.method == "register":
            CustomController._view("MRCMS", "register_distribution.html")
        return output
    s3.postp = custom_postp

    return attr

# -------------------------------------------------------------------------
def supply_distribution_item_resource(r, tablename):

    s3db = current.s3db

    # Read-only (except via registration UI)
    s3db.configure("supply_distribution_item",
                   insertable = False,
                   editable = False,
                   deletable = False,
                   )

# -------------------------------------------------------------------------
def supply_item_resource(r, tablename):

    db = current.db
    s3db = current.s3db
    auth = current.auth

    # Filter to items linked to accessible catalogs
    # => special multi-tenancy with catalog separation (TODO make this a generic option?)
    for resource in (r.resource, r.component):
        if resource and resource.tablename == "supply_item":

            citable = s3db.supply_catalog_item
            accessible = auth.s3_accessible_query("read", citable)
            permitted_items = db(accessible)._select(citable.item_id, distinct=True)

            table = resource.table
            query = table.id.belongs(permitted_items)
            resource.add_filter(query)

            break

    # TODO Move into prep:
    list_fields = ["name",
                   "code",
                   "um",
                   "item_category_id",
                   "catalog_id",
                   # TODO show catalog organisation only if user can access
                   #      catalogs from multiple orgs?
                   #      => also add filter for catalog organisation
                   "catalog_id$organisation_id",
                   ]

    s3db.configure("supply_item",
                   list_fields = list_fields,
                   )

# END =========================================================================
