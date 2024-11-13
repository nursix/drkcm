"""
    Supply

    Generic Supply functionality such as catalogs and items that are used across multiple applications
"""

module = request.controller

if not settings.has_module("supply"):
    raise HTTP(404, body="Module disabled: %s" % module)

# =============================================================================
def index():
    """
        Application Home page
    """

    module_name = settings.modules[module].get("name_nice")
    response.title = module_name
    return {"module_name": module_name,
            }

# -----------------------------------------------------------------------------
def brand():
    """ RESTful CRUD controller """

    return crud_controller()

# -----------------------------------------------------------------------------
def catalog():
    """ RESTful CRUD controller """

    def prep(r):

        record = r.record

        if r.component_name == "catalog_item":

            table = r.component.table
            field = table.item_category_id

            # Filter selectable categories to current catalog
            ctable = s3db.supply_item_category
            dbset = db(ctable.catalog_id == r.record.id)
            field.requires = IS_EMPTY_OR(
                                IS_ONE_OF(dbset, "supply_item_category.id",
                                          field.represent,
                                          sort = True,
                                          ))

            # Filter items by context organisation
            field = table.item_id
            organisation_id = record.organisation_id if record else None
            if organisation_id:
                field = table.item_id
                from core import S3AutocompleteWidget
                field.widget = S3AutocompleteWidget("supply", "item",
                                                    filter = "org=%s" % organisation_id,
                                                    )
        return True
    s3.prep = prep

    return crud_controller(rheader=s3db.supply_catalog_rheader)

# -----------------------------------------------------------------------------
def catalog_item():
    """ RESTful CRUD controller """

    return crud_controller()

# -----------------------------------------------------------------------------
def item():
    """ RESTful CRUD controller """

    # Defined in the Model for use from Multiple Controllers for unified menus
    return s3db.supply_item_controller()

# -----------------------------------------------------------------------------
def item_category():
    """ RESTful CRUD controller """

    def prep(r):
        table = s3db.supply_item_category
        if r.get_vars.get("assets") == "1":
            # Category must be one that supports Assets
            f = table.can_be_asset
            # Default anyway
            #f.default = True
            f.readable = f.writable = False

        if r.id:
            # Should not be able to set the Parent to this record
            # @ToDo: Also prevent setting to any of the categories of which this is an ancestor
            the_set = db(table.id != r.id)
            table.parent_item_category_id.requires = IS_EMPTY_OR(
                IS_ONE_OF(the_set, "supply_item_category.id",
                          s3db.supply_ItemCategoryRepresent(use_code=False),
                          sort=True)
                )

        return True
    s3.prep = prep

    return crud_controller()

# -----------------------------------------------------------------------------
def item_entity():
    """ RESTful CRUD controller """

    # Defined in the Model for use from Multiple Controllers for unified menus
    return s3db.supply_item_entity_controller()

# -----------------------------------------------------------------------------
def item_pack():
    """ RESTful CRUD controller """

    s3db.configure("supply_item_pack",
                   listadd = False,
                   )

    return crud_controller()

# -----------------------------------------------------------------------------
def kit_item():
    """ RESTful CRUD controller """

    return crud_controller()

# -----------------------------------------------------------------------------
def person_item():
    """ RESTful CRUD controller """

    return crud_controller()

# -----------------------------------------------------------------------------
def person_item_status():
    """ RESTful CRUD controller """

    return crud_controller()

# =============================================================================
# Distributions
#
def distribution_set():
    """ Distribution Sets: CRUD Controller """

    def prep(r):

        record = r.record
        if r.component_name == "distribution_set_item" and record:
            # Filter items by context organisation
            organisation_id = record.organisation_id
            if organisation_id:
                field = r.component.table.item_id
                from core import S3AutocompleteWidget
                field.widget = S3AutocompleteWidget("supply", "item",
                                                    filter = "org=%s" % organisation_id,
                                                    )
        return True
    s3.prep = prep

    return crud_controller(rheader=s3db.supply_distribution_rheader)

# -----------------------------------------------------------------------------
def distribution():
    """ Distributions: CRUD Controller """

    def prep(r):

        record = r.record
        if r.component_name == "distribution_item" and record:
            # Filter items by context organisation
            organisation_id = record.organisation_id
            if organisation_id:
                field = r.component.table.item_id
                from core import S3AutocompleteWidget
                field.widget = S3AutocompleteWidget("supply", "item",
                                                    filter = "org=%s" % organisation_id,
                                                    )
        return True
    s3.prep = prep

    return crud_controller(rheader=s3db.supply_distribution_rheader)

# -----------------------------------------------------------------------------
def distribution_item():
    """ Distribution Items: CRUD Controller """

    return crud_controller(rheader=s3db.supply_distribution_rheader)

# END =========================================================================
