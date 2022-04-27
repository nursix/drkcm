"""
    Option Filters

    Copyright: 2013-2022 (c) Sahana Software Foundation

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

__all__ = ("OptionsFilter",
           "HierarchyFilter",
           )

import re

from collections import OrderedDict

from gluon import current, DIV, INPUT, LABEL, SPAN, TAG, IS_IN_SET
from gluon.storage import Storage

from ..tools import s3_get_foreign_key, s3_str
from ..ui import S3CascadeSelectWidget, S3GroupedOptionsWidget, \
                 S3HierarchyWidget, S3MultiSelectWidget
from ..resource import S3ResourceField, S3ResourceQuery, S3URLQuery

from .base import FilterWidget

# =============================================================================
class OptionsFilter(FilterWidget):
    """
        Options filter widget

        Keyword Args:
            ** Widget appearance:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            widget: widget to use, "select", "multiselect" (default),
                    or "groupedopts"
            no_opts: text to show if no options available

            ** Options-lookup:
            options: fixed options as dict {value: label}, or a callable
                     that returns that dict
            resource: alternative resource to look up options
            lookup: field in the alternative resource
            reverse_lookup: perform a reverse lookup (default: True, should
                            be set to False if the referenced table is much
                            bigger than the filter table)

            ** Options-representation:
            represent: custom represent for looked-up options
                       (overrides field representation method)
            translate: translate the option labels in the fixed set (looked-up
                       option sets will use the field representation method
                       instead)
            none: True, or a label, to include explicit None-option for many-to-many
                  fields (by default, the None-option will not be included),
            sort: alpha-sort the options (default: True)

            ** multiselect-specific options:
            search: show search-field to search for options
            header: show header with bulk-actions
            selectedList: number of selected items to show on button before
                          collapsing into number of items

            ** groupedopts-specific options:
            cols: number of columns of checkboxes
            size: maximum size of multi-letter options groups
            help_field: field in the referenced table to display on hovering
                        over a foreign key option

            ** special purpose / custom filters:
            anyall: use user-selectable any/all alternatives even if field is
                    not a list-type
    """

    css_base = "options-filter"

    operator = "belongs"

    alternatives = ["anyof", "contains"]

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self._attr(resource)
        opts_get = self.opts.get
        name = attr["_name"]

        # Get the options
        ftype, options = self._options(resource, values=values)
        if options is None:
            options = []
            hide_widget = True
            hide_noopt = ""
        else:
            options = OrderedDict(options)
            hide_widget = False
            hide_noopt = " hide"

        # Any-All-Option : for many-to-many fields the user can
        # search for records containing all the options or any
        # of the options:
        if len(options) > 1 and (ftype[:4] == "list" or opts_get("anyall")):
            operator = opts_get("operator", None)
            if operator:
                # Fixed operator
                any_all = ""
            else:
                # User choice (initially set to "all")
                any_all = True
                operator = "contains"

            if operator == "anyof":
                filter_type = "any"
            else:
                filter_type = "all"
            self.operator = operator

            if any_all:
                # Provide a form to prompt the user to choose
                T = current.T
                any_all = DIV(LABEL("%s:" % T("Match")),
                              LABEL(INPUT(_name = "%s_filter" % name,
                                          _id = "%s_filter_any" % name,
                                          _type = "radio",
                                          _value = "any",
                                          value = filter_type,
                                          ),
                                    T("Any##filter_options"),
                                    _for = "%s_filter_any" % name,
                                    ),
                              LABEL(INPUT(_name = "%s_filter" % name,
                                          _id = "%s_filter_all" % name,
                                          _type = "radio",
                                          _value = "all",
                                          value = filter_type,
                                          ),
                                    T("All##filter_options"),
                                    _for = "%s_filter_all" % name,
                                    ),
                              _class="s3-options-filter-anyall",
                              )
        else:
            any_all = ""

        # Initialize widget
        #widget_type = opts_get("widget")
        # Use groupedopts widget if we specify cols, otherwise assume multiselect
        cols = opts_get("cols", None)
        if cols:
            widget_class = "groupedopts-filter-widget"
            w = S3GroupedOptionsWidget(options = options,
                                       multiple = opts_get("multiple", True),
                                       cols = cols,
                                       size = opts_get("size", 12),
                                       help_field = opts_get("help_field"),
                                       sort = opts_get("sort", True),
                                       orientation = opts_get("orientation"),
                                       table = opts_get("table", True),
                                       no_opts = opts_get("no_opts", None),
                                       option_comment = opts_get("option_comment", False),
                                       )
        else:
            # Default widget_type = "multiselect"
            widget_class = "multiselect-filter-widget"
            w = S3MultiSelectWidget(search = opts_get("search", "auto"),
                                    header = opts_get("header", False),
                                    selectedList = opts_get("selectedList", 3),
                                    noneSelectedText = opts_get("noneSelectedText", "Select"),
                                    multiple = opts_get("multiple", True),
                                    )


        # Add widget class and default class
        classes = attr.get("_class", "").split() + [widget_class, self.css_base]
        if hide_widget:
            classes.append("hide")
        attr["_class"] = " ".join(set(classes)) if classes else None

        # Render the widget
        dummy_field = Storage(name = name,
                              type = ftype,
                              requires = IS_IN_SET(options, multiple=True),
                              )
        widget = w(dummy_field, values, **attr)

        return TAG[""](any_all,
                       widget,
                       SPAN(self.no_opts,
                            _class = "no-options-available%s" % hide_noopt,
                            ),
                       )

    # -------------------------------------------------------------------------
    @property
    def no_opts(self):
        """
            Get the label for "no options available"

            Returns:
                the label (lazyT)
        """

        label = self.opts.no_opts
        if not label:
            label = current.T("No options available")
        return label

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        opts = self.opts
        attr = self._attr(resource)
        ftype, options = self._options(resource)

        if options is None:
            options = {attr["_id"]: {"empty": str(self.no_opts)}}
        else:
            #widget_type = opts["widget"]
            # Use groupedopts widget if we specify cols, otherwise assume multiselect
            cols = opts.get("cols", None)
            if cols:
                # Use the widget method to group and sort the options
                widget = S3GroupedOptionsWidget(
                                options = options,
                                multiple = True,
                                cols = cols,
                                size = opts["size"] or 12,
                                help_field = opts["help_field"],
                                sort = opts.get("sort", True),
                                )
                options = {attr["_id"]:
                           widget._options({"type": ftype}, [])}
            else:
                # Multiselect
                # Produce a simple list of tuples
                options = {attr["_id"]: [(k, s3_str(v))
                                         for k, v in options]}

        return options

    # -------------------------------------------------------------------------
    def _options(self, resource, values=None):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
                values: the currently selected values (resp. filter default)

            Returns:
                tuple (ftype, opt_list)
        """

        opts = self.opts

        # Resolve the filter field
        selector = self.field
        if isinstance(selector, (tuple, list)):
            selector = selector[0]

        if resource is None:
            rname = opts.get("resource")
            if rname:
                resource = current.s3db.resource(rname)
                lookup = opts.get("lookup")
                if lookup:
                    selector = lookup

        if resource:
            rfield = S3ResourceField(resource, selector)
            field = rfield.field
            ftype = rfield.ftype
        else:
            rfield = field = None
            ftype = "string"

        # Determine available options
        options = opts.options
        if options is not None:
            # Dict {value: label} or a callable returning that dict:
            if callable(options):
                options = options()
            opt_keys = list(options.keys())

        elif resource:
            if ftype == "boolean":
                opt_keys = (True, False)
            else:
                opt_keys = self._lookup_options(resource, rfield)

        else:
            opt_keys = []

        # Make sure the selected options are in the available options
        # (not possible if we have a fixed options dict)
        if options is None and values:
            self._add_selected(opt_keys, values, ftype)

        # No options available?
        if len(opt_keys) < 1 or len(opt_keys) == 1 and not opt_keys[0]:
            return ftype, None

        # Represent the options
        if options is not None:
            if opts.translate:
                # Translate the labels
                T = current.T
                opt_list = [(opt, T(label))
                            if isinstance(label, str) else (opt, label)
                            for opt, label in options.items()
                            ]
            else:
                opt_list = list(options.items())
        else:
            opt_list = self._represent_options(field, opt_keys)

        # Sort the options
        opt_list, has_none = self._sort_options(opt_list)

        # Add none-option if configured and not in options list yet
        none = opts.none
        if none and not has_none:
            # Add none-option
            if none is True:
                none = current.messages["NONE"]
            opt_list.append((None, none))

        # Browsers automatically select the first option in single-selects,
        # but that doesn't filter the data, so the first option must be
        # empty if we don't have a default:
        if not opts.get("multiple", True) and not self.values:
            opt_list.insert(0, ("", ""))

        return ftype, opt_list

    # -------------------------------------------------------------------------
    def _lookup_options(self, resource, rfield):
        """
            Lookup the filter options from resource

            Args:
                resource: the CRUDResource to filter
                rfield: the filter field (S3ResourceField)

            Returns:
                list of options (keys only, no represent)
        """

        colname, rows = None, None

        field = rfield.field
        if field and self.opts.reverse_lookup is not False:
            virtual = False

            ktablename, key = s3_get_foreign_key(field, m2m=False)[:2]
            if ktablename:
                ktable = current.s3db.table(ktablename)
                key_field = ktable[key]
                colname = str(key_field)

                # Try a reverse-lookup, i.e. select records from the
                # referenced table that are linked to at least one
                # record in the filtered table
                query = resource.get_query()
                rfilter = resource.rfilter
                if rfilter:
                    join = rfilter.get_joins()
                    left = rfilter.get_joins(left=True)
                else:
                    join = left = None

                query &= (key_field == field) & \
                         current.auth.s3_accessible_query("read", ktable)

                # If the filter field is in a joined table itself,
                # include the join for that table
                joins = rfield.join
                for tname in joins:
                    query &= joins[tname]

                opts = self.opts

                # Filter options by location?
                location_filter = opts.get("location_filter")
                if location_filter and "location_id" in ktable:
                    location = current.session.s3.location_filter
                    if location:
                        query &= (ktable.location_id == location)

                # Filter options by organisation?
                org_filter = opts.get("org_filter")
                if org_filter and "organisation_id" in ktable:
                    root_org = current.auth.root_org()
                    if root_org:
                        query &= ((ktable.organisation_id == root_org) | \
                                  (ktable.organisation_id == None))

                rows = current.db(query).select(key_field,
                                                resource._id.min(),
                                                groupby = key_field,
                                                join = join,
                                                left = left,
                                                )
        else:
            virtual = not bool(field)

        if rows is None:
            # Fall back to regular forward-lookup, i.e. select all
            # unique values in the filter field
            multiple = rfield.ftype[:5] == "list:"
            groupby = field if field and not multiple else None
            rows = resource.select([rfield.selector],
                                   limit = None,
                                   groupby = groupby,
                                   virtual = virtual,
                                   as_rows = True,
                                   )
            colname = rfield.colname
        else:
            multiple = False

        # Extract option keys from rows
        opt_keys = set()
        if rows:
            for row in rows:
                val = row[colname]
                if virtual and callable(val):
                    val = val()
                if (multiple or virtual) and isinstance(val, (list, tuple, set)):
                    opt_keys.update(val)
                else:
                    opt_keys.add(val)

        return list(opt_keys)

    # -------------------------------------------------------------------------
    @staticmethod
    def _add_selected(opt_keys, values, ftype):
        """
            Add currently selected values to the options

            Args:
                opt_keys: list of option keys to add the values to
                values: list of currently selected values (resp. defaults)
                ftype: the field type
        """

        numeric = ftype in ("integer", "id") or ftype[:9] == "reference"
        for v in values:
            if numeric and v is not None:
                try:
                    value = int(v)
                except ValueError:
                    # not valid for this field type => skip
                    continue
            else:
                value = v
            if value not in opt_keys and \
               (not isinstance(value, int) or str(value) not in opt_keys):
                opt_keys.append(value)

    # -------------------------------------------------------------------------
    def _represent_options(self, field, opt_keys):
        """
            Represent the filter options

            Args:
                field: the filter field (Field)
                opt_keys: list of options (keys only)

            Returns:
                opt_list: list of options, as tuples (key, repr)
        """

        T = current.T
        EMPTY = T("None")

        opts = self.opts

        # Custom represent? (otherwise fall back to field.represent)
        represent = opts.represent
        if not represent:
            represent = field.represent if field else None

        ftype = str(field.type) if field else "virtual"

        if callable(represent):
            if hasattr(represent, "bulk"):
                # S3Represent => use bulk option
                opt_dict = represent.bulk(opt_keys, list_type=False, show_link=False)
                if None in opt_keys:
                    opt_dict[None] = EMPTY
                elif None in opt_dict:
                    del opt_dict[None]
                if "" in opt_keys:
                    opt_dict[""] = EMPTY
                opt_list = list(opt_dict.items())
            else:
                # Simple represent-function
                varnames = represent.__code__.co_varnames
                args = {"show_link": False} if "show_link" in varnames else {}
                if ftype[:5] == "list:":
                    # Represent-function expects a list
                    repr_opt = lambda opt: (opt, EMPTY) if opt in (None, "") else \
                                           (opt, represent([opt], **args))
                else:
                    repr_opt = lambda opt: (opt, EMPTY) if opt in (None, "") else \
                                           (opt, represent(opt, **args))
                opt_list = [repr_opt(k) for k in opt_keys]

        elif isinstance(represent, str) and ftype[:9] == "reference":
            # String template to be fed from the referenced record

            # Get the referenced table
            db = current.db
            ktable = db[ftype[10:]]

            k_id = ktable._id.name

            # Get the fields referenced by the string template
            fieldnames = [k_id]
            fieldnames += re.findall(r"%\(([a-zA-Z0-9_]*)\)s", represent)
            represent_fields = [ktable[fieldname] for fieldname in fieldnames]

            # Get the referenced records
            query = (ktable.id.belongs([k for k in opt_keys
                                              if str(k).isdigit()])) & \
                    (ktable.deleted == False)
            rows = db(query).select(*represent_fields).as_dict(key=k_id)

            # Run all referenced records against the format string
            opt_list = []
            ol_append = opt_list.append
            for opt_value in opt_keys:
                if opt_value in rows:
                    opt_represent = represent % rows[opt_value]
                    if opt_represent:
                        ol_append((opt_value, opt_represent))

        else:
            # Straight string representations of the values (fallback)
            opt_list = [(opt_value, s3_str(opt_value))
                        for opt_value in opt_keys if opt_value]

        return opt_list

    # -------------------------------------------------------------------------
    def _sort_options(self, opt_list):
        """
            Sort the options list

            Args:
                opt_list: list of option tuples [(value, repr), ...]

            Returns:
                tuple (sorted_list, has_none), has_none indicating whether
                an option with value=None is present in the list
        """

        opts = self.opts

        # Sort the options
        if opts.get("sort", True):
            try:
                opt_list.sort(key=lambda item: item[1])
            except TypeError:
                opt_list.sort(key=lambda item: s3_str(item[1]))

        # Rewrite the list to handle None
        options, has_none = [], False
        for k, v in opt_list:
            if k is None:
                # Include only if explicitly configured
                none = opts.none
                if none:
                    has_none = True
                    options.append((k, v if none is True else none))
            else:
                options.append((k, v))

        return options, has_none

    # -------------------------------------------------------------------------
    @staticmethod
    def _values(get_vars, variable):
        """
            Helper method to get all values of a URL query variable

            Args:
                get_vars: the GET vars (a dict)
                variable: the name of the query variable

            Returns:
                a list of values
        """

        if not variable:
            return []

        # Match __eq before checking any other operator
        selector = S3URLQuery.parse_key(variable)[0]
        for key in ("%s__eq" % selector, selector, variable):
            if key in get_vars:
                values = S3URLQuery.parse_value(get_vars[key])
                if not isinstance(values, (list, tuple)):
                    values = [values]
                return values

        return []

# =============================================================================
class HierarchyFilter(FilterWidget):
    """
        Filter widget for hierarchical types

        Keyword Arguments:
            lookup: name of the lookup table
            represent: representation method for the key
            multiple: allow selection of multiple options
            leafonly: only leaf nodes can be selected
            cascade: automatically select child nodes when selecting a
                     parent node
            bulk_select: provide an option to select/deselect all nodes

        See Also:
            S3HierarchyWidget
    """

    css_base = "hierarchy-filter"

    operator = "belongs"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        # Currently selected values
        selected = []
        append = selected.append
        if not isinstance(values, (list, tuple, set)):
            values = [values]
        for v in values:
            if isinstance(v, int) or str(v).isdigit():
                append(v)

        # Resolve the field selector
        rfield = S3ResourceField(resource, self.field)

        # Instantiate the widget
        opts = self.opts
        bulk_select = current.deployment_settings \
                             .get_ui_hierarchy_filter_bulk_select_option()
        if bulk_select is None:
            bulk_select = opts.get("bulk_select", False)

        if opts.get("widget") == "cascade":
            formstyle = current.deployment_settings.get_ui_filter_formstyle()
            w = S3CascadeSelectWidget(lookup = opts.get("lookup"),
                                      formstyle = formstyle,
                                      multiple = opts.get("multiple", True),
                                      filter = opts.get("filter"),
                                      leafonly = opts.get("leafonly", True),
                                      cascade = opts.get("cascade"),
                                      represent = opts.get("represent"),
                                      inline = True,
                                      )
        else:
            w = S3HierarchyWidget(lookup = opts.get("lookup"),
                                  multiple = opts.get("multiple", True),
                                  filter = opts.get("filter"),
                                  leafonly = opts.get("leafonly", True),
                                  cascade = opts.get("cascade", False),
                                  represent = opts.get("represent"),
                                  bulk_select = bulk_select,
                                  none = opts.get("none"),
                                  )

        # Render the widget
        widget = w(rfield.field, selected, **self._attr(resource))
        widget.add_class(self.css_base)

        return widget

    # -------------------------------------------------------------------------
    def variable(self, resource, get_vars=None):
        """
            Generate the name for the URL query variable for this
            widget, detect alternative __typeof queries.

            Args:
                resource: the resource

            Returns:
                the URL query variable name (or list of variable names if
                there are multiple operators)
        """

        label, self.selector = self._selector(resource, self.field)

        if not self.selector:
            return None

        if "label" not in self.opts:
            self.opts["label"] = label

        selector = self.selector

        if self.alternatives and get_vars is not None:
            # Get the actual operator from get_vars
            operator = self._operator(get_vars, self.selector)
            if operator:
                self.operator = operator

        variable = self._variable(selector, self.operator)

        if not get_vars or not resource or variable in get_vars:
            return variable

        # Detect and resolve __typeof queries
        resolve = S3ResourceQuery._resolve_hierarchy
        selector = resource.prefix_selector(selector)
        for key, value in list(get_vars.items()):

            if key.startswith(selector):
                selectors, op = S3URLQuery.parse_expression(key)[:2]
            else:
                continue
            if op != "typeof" or len(selectors) != 1:
                continue

            rfield = resource.resolve_selector(selectors[0])
            if rfield.field:
                values = S3URLQuery.parse_value(value)
                field, nodeset, none = resolve(rfield.field, values)[1:]
                if field and (nodeset or none):
                    if nodeset is None:
                        nodeset = set()
                    if none:
                        nodeset.add(None)
                    get_vars.pop(key, None)
                    get_vars[variable] = [str(v) for v in nodeset]
            break

        return variable

# END =========================================================================
