"""
    Location Filters

    Copyright: 2013-2021 (c) Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following
    conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    OTHER DEALINGS IN THE SOFTWARE.
"""

__all__ = ("LocationFilter",
           "MapFilter",
           )

import json

from collections import OrderedDict

from gluon import current, INPUT, SPAN, TAG, IS_IN_SET
from gluon.storage import Storage

from ..resource import FS, S3ResourceField
from ..tools import JSONSEPARATORS
from ..ui import S3MultiSelectWidget

from .base import FilterWidget

# =============================================================================
class LocationFilter(FilterWidget):
    """
        Hierarchical Location Filter Widget

        Keyword Args:
            ** Widget appearance:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            no_opts: text to show if no options available

            ** Options-lookup:
            levels: list of location hierarchy levels
            resource: alternative resource to look up options
            lookup: field in the alternative resource to look up
            options: fixed set of options (list of gis_location IDs)

            ** Multiselect-dropdowns:
            search: show search-field to search for options
            header: show header with bulk-actions
            selectedList: number of selected items to show on
                          button before collapsing into number of items
    """

    css_base = "location-filter"

    operator = "belongs"

    # -------------------------------------------------------------------------
    def __init__(self, field=None, **attr):
        """
            Constructor to configure the widget

            Args:
                field: the selector(s) for the field(s) to filter by
                attr: configuration options for this widget
        """

        if not field:
            field = "location_id"

        # Translate options using gis_location_name?
        settings = current.deployment_settings
        translate = settings.get_L10n_translate_gis_location()
        if translate:
            language = current.session.s3.language
            if language == "en":
                translate = False
        self.translate = translate

        super(LocationFilter, self).__init__(field=field, **attr)

        if "label" not in self.opts:
            self.opts.label = current.T("Filter by Location")

        self._levels = None

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        ftype, levels, noopt = self._options(resource, values=values)
        if noopt:
            return SPAN(noopt, _class="no-options-available")

        T = current.T
        s3 = current.response.s3

        attr = self._attr(resource)

        # Filter class (default+custom)
        css = attr.get("class")
        _class = "%s %s" % (css, self.css_base) if css else self.css_base
        attr["_class"] = _class

        if "multiselect-filter-widget" not in _class:
            _class = "%s multiselect-filter-widget" % _class
        opts = self.opts
        if not opts.get("hidden") and "active" not in _class:
            _class = "%s active" % _class

        # Header-option for multiselect
        header_opt = opts.get("header", False)
        if header_opt is False or header_opt is True:
            setting = current.deployment_settings \
                             .get_ui_location_filter_bulk_select_option()
            if setting is not None:
                header_opt = setting

        # Add one multi-select widget per level
        field_name = self.field
        fname = self._prefix(field_name) if resource else field_name
        operator = self.operator

        base_id = attr["_id"]
        base_name = attr["_name"]

        widgets = []
        w_append = widgets.append

        for index, level in enumerate(levels):

            w_attr = dict(attr)

            # Unique ID/name
            w_attr["_id"] = "%s-%s" % (base_id, level)
            w_attr["_name"] = name = "%s-%s" % (base_name, level)

            # Dummy field
            dummy_field = Storage(name=name, type=ftype)

            # Find relevant values to pre-populate the widget
            level_values = values.get("%s$%s__%s" % (fname, level, operator))
            placeholder = T("Select %(location)s") % {"location": levels[level]["label"]}
            w = S3MultiSelectWidget(search = opts.get("search", "auto"),
                                    header = header_opt,
                                    selectedList = opts.get("selectedList", 3),
                                    noneSelectedText = placeholder,
                                    )

            if index == 0:
                # Visible Multiselect Widget added to the page
                w_attr["_class"] = _class

                options = levels[level]["options"]
                dummy_field.requires = IS_IN_SET(options, multiple=True)

                widget = w(dummy_field, level_values, **w_attr)
            else:
                # Hidden+empty dropdown added to the page, options and
                # multiselect will be activated when the higher level
                # is selected
                w_attr["_class"] = "%s hide" % _class

                # Store the current jquery_ready
                jquery_ready = s3.jquery_ready
                s3.jquery_ready = []

                # Build the widget with the MultiSelect activation script
                dummy_field.requires = IS_IN_SET([], multiple=True)
                widget = w(dummy_field, level_values, **w_attr)

                # Extract the MultiSelect activation script from updated jquery_ready
                script = s3.jquery_ready[0]
                s3.jquery_ready = jquery_ready

                # Wrap the script & reinsert
                script = '''S3.%s=function(){%s}''' % (name.replace("-", "_"), script)
                s3.js_global.append(script)

            w_append(widget)

        return TAG[""](*widgets)

    # -------------------------------------------------------------------------
    def data_element(self, variable):
        """
            Construct the hidden element that holds the
            URL query term corresponding to an input element in the widget.

            Args:
                variable: the URL query variable

            Returns:
                list of hidden inputs
        """

        widget_id = self.attr["_id"]

        return [INPUT(_type = "hidden",
                      _id = "%s-%s-data" % (widget_id, level),
                      _class = "filter-widget-data %s-data" % self.css_base,
                      _value = variable[i],
                      )
                for i, level in enumerate(self.levels)
                ]

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Look up filter options, to Ajax-update the filter widget
            when resource data have changed

            Args:
                resource: the CRUDResource to look up the options from

            Returns:
                the options as dict
                {selector_id: [name, ...] or {name: local_name: ...}}
        """

        attr = self._attr(resource)
        levels, noopt = self._options(resource, inject_hierarchy=False)[1:3]

        opts = {}
        base_id = attr["_id"]
        for level in levels:
            if noopt:
                opts["%s-%s" % (base_id, level)] = str(noopt)
            else:
                options = levels[level]["options"]
                opts["%s-%s" % (base_id, level)] = options

        return opts

    # -------------------------------------------------------------------------
    @property
    def levels(self):
        """
            Get the (initialized) levels options

            Returns:
                an ordered dict {Lx: {"label": label, options: [] or {}}}
        """

        levels = self._levels
        if levels is None:
            opts = self.opts

            # Lookup the appropriate labels from the GIS configuration
            if "levels" in opts:
                hierarchy = current.gis.get_location_hierarchy()
                levels = OrderedDict()
                for level in opts.levels:
                    levels[level] = hierarchy.get(level, level)
            else:
                levels = current.gis.get_relevant_hierarchy_levels(as_dict=True)

            translate = self.translate
            for level in levels:
                levels[level] = {"label": levels[level],
                                 "options": {} if translate else [],
                                 }
            self._levels = levels

        return levels

    # -------------------------------------------------------------------------
    def _options(self, resource, values=None, inject_hierarchy=True):
        """
            Generate the options for the filter

            Args:
                resource: the resource to look up the options from
                values: the currently selected values, a dict {selector: [values]}
                inject_hierarchy: add the location hierarchy to global JS

            Returns:
                a tuple (ftype, levels, no_opts)

            Notes:
                - levels is a dict like:
                    {Lx: {"label": label,
                          "options": [name, ...] or {name: local_name, ...},
                          }}
                - the injected hierarchy is a nested JSON object like:
                    {topLx: {name: {name: {name: ...}}}}
                - the injected local names are a map:
                    {name: local_name, ...}
        """

        s3db = current.s3db

        opts = self.opts
        translate = self.translate

        ftype = "reference gis_location"
        levels = self.levels

        no_opts = opts.get("no_opts")
        if not no_opts:
            no_opts = current.T("No options available")

        default = (ftype, levels, no_opts)

        # Resolve the field selector
        selector = None
        if resource is None:
            rname = opts.get("resource")
            if rname:
                resource = s3db.resource(rname)
                selector = opts.get("lookup", "location_id")
        else:
            selector = self.field

        filters_added = False

        options = opts.get("options")
        if options:
            # Fixed options (=list of location IDs)
            resource = s3db.resource("gis_location", id=options)

        elif selector:
            # Resolve selector against resource
            rfield = S3ResourceField(resource, selector)
            if not rfield.field or rfield.ftype != ftype:
                raise TypeError("invalid selector: %s" % selector)

            # Exclude empty FKs
            resource.add_filter(FS(selector) != None)

            # Filter out old Locations
            resource.add_filter(FS("%s$end_date" % selector) == None)

            filters_added = True
        else:
            # Neither fixed options nor resource to look them up
            return default

        # Lookup options
        rows = self._lookup_options(levels,
                                    resource,
                                    selector = selector,
                                    location_ids = options,
                                    path = translate,
                                    )

        if filters_added:
            # Remove them
            rfilter = resource.rfilter
            rfilter.filters.pop()
            rfilter.filters.pop()
            rfilter.query = None
            rfilter.transformed = None

        # Make sure the selected options are in the available options
        if values:
            rows = self._add_selected(rows, values, levels, translate)

        if not rows:
            # No options
            return default

        # Generate a name localization lookup dict
        local_names = self._get_local_names(rows) if translate else {}

        # Populate levels-options and hierarchy
        toplevel = list(levels.keys())[0]
        hierarchy = {toplevel: {}}
        for row in rows:
            h = hierarchy[toplevel]
            for level in levels:
                name = row[level]
                if not name:
                    continue
                options = levels[level]["options"]
                if name not in options:
                    if translate:
                        options[name] = local_names.get(name, name)
                    else:
                        options.append(name)
                if inject_hierarchy:
                    if name not in h:
                        h[name] = {}
                    h = h[name]

        # Sort options
        self._sort_options(levels, translate=translate)

        # Inject the location hierarchy
        if inject_hierarchy:
            js_global = current.response.s3.js_global
            jsons = lambda v: json.dumps(v, separators=JSONSEPARATORS)
            hierarchy = "S3.location_filter_hierarchy=%s" % jsons(hierarchy)
            js_global.append(hierarchy)
            if translate:
                # Also inject local names map
                local_names = "S3.location_name_l10n=%s" % jsons(local_names)
                js_global.append(local_names)

        return (ftype, levels, None)

    # -------------------------------------------------------------------------
    @staticmethod
    def _lookup_options(levels, resource, selector=None, location_ids=None, path=False):
        """
            Look up the filter options from the resource: i.e. the immediate Lx
            ancestors for all locations referenced by selector

            Args:
                levels: the relevant Lx levels, tuple of "L1", "L2" etc
                resource: the master resource
                selector: the selector for the location reference
                location_ids: use these location_ids rather than looking them
                              up from the resource
                path: include the Lx path in the result rows, to lookup
                      local names for options (which is done via IDs in
                      the path)

            Returns:
                gis_location Rows, or None

            Note:
                path=True potentially requires additional iterations in order
                to reduce the paths to only relevant Lx levels (so that fewer
                local names would be extracted) - which though limits the
                performance gain if there actually are only few or no translations.
                If that becomes a problem somewhere, we can make the iteration
                mode controllable by a separate parameter.
        """

        db = current.db
        s3db = current.s3db

        ltable = s3db.gis_location
        if location_ids:
            # Fixed set
            location_ids = set(location_ids)
        else:
            # Lookup from resource
            location_ids = set()

            # Resolve the selector
            rfield = resource.resolve_selector(selector)

            # Get the joins for the selector
            from ..resource import S3Joins
            joins = S3Joins(resource.tablename)
            joins.extend(rfield._joins)
            join = joins.as_list()

            # Add a join for gis_location
            join.append(ltable.on(ltable.id == rfield.field))

            # Accessible query for the master table
            query = resource.get_query()

        # Fields we want to extract for Lx ancestors
        fields = [ltable.id] + [ltable[level] for level in levels]
        if path:
            fields.append(ltable.path)

        # Suppress instantiation of LazySets in rows (we don't need them)
        rname = db._referee_name
        db._referee_name = None

        rows = None
        while True:

            if location_ids:
                query = ltable.id.belongs(location_ids)
                join = None

            # Extract all target locations resp. parents which are Lx
            if path:
                #...of relevant levels
                relevant_lx = (ltable.level.belongs(levels))
            else:
                #...of any level
                relevant_lx = (ltable.level != None)
            lx = db(query & relevant_lx).select(join = join,
                                                groupby = ltable.id,
                                                *fields
                                                )

            # Add to result rows
            if lx:
                rows = (rows | lx) if rows else lx

            # Pick subset for parent lookup
            if lx and location_ids:
                # ...all parents which are not Lx of relevant levels
                remaining = location_ids - set(row.id for row in lx)
                if remaining:
                    query = ltable.id.belongs(remaining)
                else:
                    # No more parents to look up
                    break
            else:
                # ...all locations which are not Lx
                if path:
                    # ...or not of relevant levels
                    query &= ((ltable.level == None) | (~(ltable.level.belongs(levels))))
                else:
                    query &= (ltable.level == None)

            # From subset, just extract the parent ID
            query &= (ltable.parent != None)
            parents = db(query).select(ltable.parent,
                                       join = join,
                                       groupby = ltable.parent,
                                       )

            location_ids = set(row.parent for row in parents if row.parent)
            if not location_ids:
                break

        # Restore referee name
        db._referee_name = rname

        return rows

    # -------------------------------------------------------------------------
    @staticmethod
    def _add_selected(rows, values, levels, translate=False):
        """
            Add currently selected values to the options

            Args:
                rows: the referenced gis_location Rows
                values: the currently selected values as {select: [name, ...]}
                levels: the relevant hierarchy levels
                translate: whether location names shall be localized

            Returns:
                the updated gis_location Rows
        """

        db = current.db
        s3db = current.s3db

        ltable = s3db.gis_location
        accessible = current.auth.s3_accessible_query("read", ltable)

        fields = [ltable.id] + [ltable[l] for l in levels]
        if translate:
            fields.append(ltable.path)

        for f, v in values.items():
            if not v:
                continue
            level = "L%s" % f.split("L", 1)[1][0]
            query = accessible & \
                    (ltable.level == level) & \
                    (ltable.name.belongs(v) & \
                    (ltable.end_date == None))
            selected = db(query).select(*fields)
            if rows:
                rows &= selected
            else:
                rows = selected

        return rows

    # -------------------------------------------------------------------------
    @staticmethod
    def _get_local_names(rows):
        """
            Look up the local names for locations

            Args:
                rows: the gis_location Rows (must contain "path" attribute)

            Returns:
                a mapping {name: local_name}
        """

        local_names = {}

        ids = set()
        for row in rows:
            path = row.path
            if path:
                path = path.split("/")
            else:
                if "id" in row:
                    path = current.gis.update_location_tree(row)
                    path = path.split("/")
            if path:
                ids |= set(path)

        if ids:
            s3db = current.s3db
            ltable = s3db.gis_location
            ntable = s3db.gis_location_name
            query = (ltable.id.belongs(ids)) & \
                    (ntable.deleted == False) & \
                    (ntable.location_id == ltable.id) & \
                    (ntable.language == current.session.s3.language)
            nrows = current.db(query).select(ltable.name,
                                             ntable.name_l10n,
                                             limitby = (0, len(ids)),
                                             )
            for row in nrows:
                local_names[row.gis_location.name] = row.gis_location_name.name_l10n

        return local_names

    # -------------------------------------------------------------------------
    @staticmethod
    def _sort_options(levels, translate=False):
        """
            Sort the filter options per level

            Args:
                levels: the levels-dict (see self.levels)
                translate: whether location names have been localized
        """

        if translate:
            for level in levels:
                options = levels[level]["options"]
                levels[level]["options"] = OrderedDict(sorted(options.items()))
        else:
            for level in levels:
                levels[level]["options"].sort()

    # -------------------------------------------------------------------------
    def _selector(self, resource, fields):
        """
            Helper method to generate a filter query selector for the
            given field(s) in the given resource.

            Args:
                resource: the CRUDResource
                fields: the field selectors (as strings)

            Returns:
                the field label and the filter query selector, or None if
                none of the field selectors could be resolved
        """

        prefix = self._prefix

        if resource:
            rfield = S3ResourceField(resource, fields)
            label = rfield.label
        else:
            label = None

        if "levels" in self.opts:
            levels = self.opts.levels
        else:
            levels = current.gis.get_relevant_hierarchy_levels()

        fields = ["%s$%s" % (fields, level) for level in levels]
        if resource:
            selectors = []
            for field in fields:
                try:
                    rfield = S3ResourceField(resource, field)
                except (AttributeError, TypeError):
                    continue
                selectors.append(prefix(rfield.selector))
        else:
            selectors = fields
        if selectors:
            return label, "|".join(selectors)
        else:
            return label, None

    # -------------------------------------------------------------------------
    @classmethod
    def _variable(cls, selector, operator):
        """
            Construct URL query variable(s) name from a filter query
            selector and the given operator(s)

            Args:
                selector: the selector
                operator: the operator (or tuple/list of operators)

            Returns:
                the URL query variable name (or list of variable names)
        """

        selectors = selector.split("|")
        return ["%s__%s" % (selector, operator) for selector in selectors]

# =============================================================================
class MapFilter(FilterWidget):
    """
        Map filter widget, normally configured for "~.location_id$the_geom"

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
    """

    css_base = "map-filter"

    operator = "intersects"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        settings = current.deployment_settings

        if not settings.get_gis_spatialdb():
            current.log.warning("No Spatial DB => Cannot do Intersects Query yet => Disabling MapFilter")
            return ""

        attr_get = self.attr.get
        opts_get = self.opts.get

        css = attr_get("class")
        _class = "%s %s" % (css, self.css_base) if css else self.css_base

        _id = attr_get("_id")

        # Hidden INPUT to store the WKT
        hidden_input = INPUT(_type = "hidden",
                             _class = _class,
                             _id = _id,
                             )

        # Populate with the value, if given
        if values not in (None, []):
            if type(values) is list:
                values = values[0]
            hidden_input["_value"] = values

        # Map Widget
        map_id = "%s-map" % _id

        c, f = resource.tablename.split("_", 1)
        c = opts_get("controller", c)
        f = opts_get("function", f)

        ltable = current.s3db.gis_layer_feature
        query = (ltable.controller == c) & \
                (ltable.function == f) & \
                (ltable.deleted == False)
        layer = current.db(query).select(ltable.layer_id,
                                         ltable.name,
                                         limitby=(0, 1)
                                         ).first()
        try:
            layer_id = layer.layer_id
        except AttributeError:
            # No prepop done?
            layer_id = None
            layer_name = resource.tablename
        else:
            layer_name = layer.name

        feature_resources = [{"name"     : current.T(layer_name),
                              "id"       : "search_results",
                              "layer_id" : layer_id,
                              "filter"   : opts_get("filter"),
                              },
                             ]

        button = opts_get("button")
        if button:
            # No need for the toolbar
            toolbar = opts_get("toolbar", False)
        else:
            # Need the toolbar
            toolbar = True

        _map = current.gis.show_map(id = map_id,
                                    height = opts_get("height", settings.get_gis_map_height()),
                                    width = opts_get("width", settings.get_gis_map_width()),
                                    collapsed = True,
                                    callback = '''S3.search.s3map('%s')''' % map_id,
                                    feature_resources = feature_resources,
                                    toolbar = toolbar,
                                    add_polygon = True,
                                    )

        return TAG[""](hidden_input,
                       button,
                       _map,
                       )

# END =========================================================================
