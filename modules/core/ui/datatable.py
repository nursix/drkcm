"""
    Data Table Builder

    Copyright: 2009-2021 (c) Sahana Software Foundation

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

__all__ = ("DataTable",
           )

import re

from gluon import current, URL, \
                  A, DIV, FORM, INPUT, SPAN, TABLE, TBODY, TD, TH, THEAD, TR
from gluon.serializers import json as jsons

from s3dal import Expression, S3DAL

from ..tools import s3_orderby_fields, s3_set_extension, s3_str

# =============================================================================
class DataTable:
    """
        Data Table Builder
        - uses jQuery dataTables, together with s3.ui.datatable.js
    """

    def __init__(self, rfields, data, table_id=None, orderby=None):
        """
            Args:
                rfields: the table columns (list of S3ResourceField)
                data: the data (list of Storage)
                table_id: the data table DOM ID
                orderby: DAL orderby expression used to extract the data
        """

        if not table_id:
            import uuid
            table_id = "list_%s" % uuid.uuid4().hex
        self.table_id = table_id

        self.data = data
        self.rfields = rfields

        colnames = []
        labels = {}
        append = colnames.append
        for rfield in rfields:
            colname = rfield.colname
            labels[colname] = rfield.label
            append(colname)

        self.colnames = colnames
        self.labels = labels

        self._orderby = orderby
        self.dt_ordering = None

    # -------------------------------------------------------------------------
    @property
    def orderby(self):
        """
            Converts the DAL orderby expression into a datatable ordering
            expression

            Returns:
                list of tuples [(col_idx, direction), ...]
        """

        dt_ordering = self.dt_ordering
        if not dt_ordering:

            orderby = self._orderby
            if orderby:
                # Resolve orderby expression into column names
                orderby_dirs = {}
                orderby_cols = []

                adapter = S3DAL()
                INVERT = adapter.INVERT

                append = orderby_cols.append
                for f in s3_orderby_fields(None, orderby, expr=True):
                    if type(f) is Expression:
                        colname = str(f.first)
                        direction = "desc" if f.op == INVERT else "asc"
                    else:
                        colname = str(f)
                        direction = "asc"
                    orderby_dirs[colname] = direction
                    append(colname)

                # Helper function to resolve a reference's "sortby" into
                # a list of column names
                ftuples = {}
                def resolve_sortby(rfield):
                    colname = rfield.colname
                    if colname in ftuples:
                        return ftuples[colname]
                    ftype = rfield.ftype
                    sortby = None
                    if ftype[:9] == "reference":
                        field = rfield.field
                        if hasattr(field, "sortby") and field.sortby:
                            sortby = field.sortby
                            if not isinstance(sortby, (tuple, list)):
                                sortby = [sortby]
                            p = "%s.%%s" % ftype[10:].split(".")[0]
                            sortby = [p % fname for fname in sortby]
                    ftuples[colname] = sortby
                    return sortby

                dt_ordering = [] # order expression for datatable
                append = dt_ordering.append

                # Match orderby-fields against table columns (=rfields)
                pos = 0
                seen = set()
                skip = seen.add
                for i, colname in enumerate(orderby_cols):
                    if i < pos:
                        # Already consumed by sortby-tuple
                        continue
                    direction = orderby_dirs[colname]
                    for col_idx, rfield in enumerate(self.rfields):
                        if col_idx in seen:
                            # Column already in dt_ordering
                            continue
                        sortby = None
                        if rfield.colname == colname:
                            # Match a single orderby-field
                            sortby = (colname,)
                        else:
                            # Match between sortby and the orderby-field tuple
                            # (must appear in same order and sorting direction)
                            sortby = resolve_sortby(rfield)
                            if not sortby or \
                               sortby != orderby_cols[i:i + len(sortby)] or \
                               any(orderby_dirs[c] != direction for c in sortby):
                                sortby = None
                        if sortby:
                            append([col_idx, direction])
                            pos += len(sortby)
                            skip(col_idx)
                            break
            else:
                dt_ordering = [[1, "asc"]]

            self.dt_ordering = dt_ordering

        return dt_ordering

    # -------------------------------------------------------------------------
    def html(self, totalrows, filteredrows, **attr):
        """
            Builds the datatable HTML (=a FORM with the embedded TABLE)

            Args:
                totalrows: number of rows available
                filteredrows: number of rows matching filters

            Keyword Args:
                see config()

            Returns:
                a FORM instance
        """

        colnames, action_col = self.columns(self.colnames, attr)

        table_id = self.table_id
        pagination = attr.get("dt_pagination", True)

        table = self.table(table_id,
                           colnames,
                           action_col,
                           # Pagination passes data via cacheLastJson,
                           # rendering only one row here to produce the
                           # <table> structure
                           limit = 1 if pagination else None,
                           )
        if pagination:
            numrows = len(self.data)
            cache_data = self.json(totalrows,
                                   filteredrows,
                                   1, # draw
                                   colnames = colnames,
                                   action_col = action_col,
                                   stringify = False,
                                   )
            cache = {"cacheLower": 0,
                     "cacheUpper": numrows if filteredrows > numrows else filteredrows,
                     "cacheLastJson": cache_data,
                     }
        else:
            cache = None

        config = self.config(self.orderby, **attr)
        config["id"] = table_id

        return self.form(table,
                         table_id,
                         config,
                         #self.orderby,
                         self.rfields,
                         cache,
                         **attr)

    # -------------------------------------------------------------------------
    def table(self, table_id, colnames, action_col=0, limit=None):
        """
            Builds the HTML table.

            Args:
                table_id: DOM ID for the table
                colnames: list of column keys
                action_col: index of the action column

            Returns:
                a TABLE instance
        """

        header_row = TR()
        labels = self.labels
        for field in colnames:
            label = "" if field == "BULK" else labels[field]
            header_row.append(TH(label))
        header = THEAD(header_row)

        body = TBODY()
        data = self.data
        if data:
            if limit:
                data = data[:limit]

            dbid = colnames[action_col]
            bulk_checkbox = self.bulk_checkbox

            addrow = body.append
            for index, row in enumerate(data):
                details = TR(_class="odd" if index % 2 else "even")
                append = details.append
                for field in colnames:
                    if field == "BULK":
                        append(TD(bulk_checkbox(row[dbid])))
                    else:
                        append(TD(row[field]))
                addrow(details)

        table = TABLE(header, body, _id=table_id, _class="dataTable display")

        if current.deployment_settings.get_ui_datatables_responsive():
            table.add_class("responsive")

        return table

    # -------------------------------------------------------------------------
    @staticmethod
    def config(orderby, **attr):
        """
            Generates the datatable config JSON (value for hidden input)

            Args:
                orderby: the datatable ordering expression

            Keyword Args:
                ** Basic configuration
                dt_ajax_url: The URL to be used for the Ajax call
                dt_base_url: base URL to construct export format URLs, resource
                             default URL without any URL method or query part
                dt_dom : The Datatable DOM initialisation variable, describing
                         the order in which elements are displayed.
                         See http://datatables.net/ref for more details.
                dt_formkey: a form key (XSRF protection for Ajax-actions)

                ** Pagination
                dt_pagination : Is pagination enabled, dafault True
                dt_pageLength : The default number of records that will be shown
                dt_lengthMenu: The menu options for the number of records to be shown
                dt_pagingType : How the pagination buttons are displayed

                ** Searching
                dt_searching: Enable or disable filtering of data.

                ** Row Actions
                dt_row_actions: list of actions (each a dict), overrides
                                current.response.s3.actions
                dt_action_col: The column where the action buttons will be placed

                ** Bulk Actions
                dt_bulk_actions: list of labels for the bulk actions.
                dt_bulk_col: The column in which the checkboxes will appear,
                             by default it will be the column immediately
                             before the first data item
                dt_bulk_single: only allow a single row to be selected
                dt_bulk_selected: A list of selected items

                ** Grouping
                dt_group: The column(s) that is(are) used to group the data
                dt_group_totals: The number of record in each group.
                                 This will be displayed in parenthesis
                                 after the group title.
                dt_group_titles: The titles to be used for each group.
                                 These are a list of lists with the inner list
                                 consisting of two values, the repr from the
                                 db and the label to display. This can be more than
                                 the actual number of groups (giving an empty group).
                dt_group_space: Insert a space between the group heading and the next group
                dt_shrink_groups: If set then the rows within a group will be hidden
                                  two types are supported, 'individual' and 'accordion'
                dt_group_types: The type of indicator for groups that can be 'shrunk'
                                Permitted valies are: 'icon' (the default) 'text' and 'none'

                ** Contents Rendering
                dt_text_maximum_len: The maximum length of text before it is condensed
                dt_text_condense_len: The length displayed text is condensed down to

                ** Styles
                dt_styles: dictionary of styles to be applied to a list of ids
                           for example: {"warning" : [1,3,6,7,9], "alert" : [2,10,13]}
                dt_col_widths: dictionary of columns to apply a width to
                               for example: {1 : 15, 2 : 20}

                ** Other Features
                dt_double_scroll: Render double scroll bars (top+bottom), only available
                                  with settings.ui.datatables_responsive=False
        """

        request = current.request
        settings = current.deployment_settings
        s3 = current.response.s3

        attr_get = attr.get

        # Default Ajax URL
        ajax_url = attr_get("dt_ajax_url")
        if not ajax_url:
            ajax_url = URL(c = request.controller,
                           f = request.function,
                           args = request.args,
                           vars = request.get_vars,
                           )
        ajax_url = s3_set_extension(ajax_url, "aadata")

        # Default length menu
        if settings.get_base_bigtable():
            default_length_menu = [[25, 50, 100], # not All
                                   [25, 50, 100],
                                   ]
        else:
            default_length_menu = [[25, 50, -1],
                                   [25, 50, s3_str(current.T("All"))],
                                   ]

        # Configuration (passed to client-side script via hidden input)
        config = {
            "utf8": False,

            # Basic Configuration
            "ajaxUrl": ajax_url,
            "dom": attr_get("dt_dom", settings.get_ui_datatables_dom()),

            # Pagination
            "pagination": attr_get("dt_pagination", True),
            "pageLength": attr_get("dt_pageLength", s3.ROWSPERPAGE),
            "lengthMenu": attr_get("dt_lengthMenu", default_length_menu),
            "pagingType": attr_get("dt_pagingType", settings.get_ui_datatables_pagingType()),

            # Searching
            "searching": attr_get("dt_searching", True),

            # Contents Rendering
            "textMaxLength": attr_get("dt_text_maximum_len", 80),
            "textShrinkLength": attr_get("dt_text_condense_len", 75),

            }

        action_col = attr_get("dt_action_col", 0)

        # Bulk Actions
        bulk_col = attr_get("dt_bulk_col", 0)
        bulk_actions = attr_get("dt_bulk_actions")
        if bulk_actions:
            if not isinstance(bulk_actions, list):
                bulk_actions = [bulk_actions]
            config.update(bulkActions = bulk_actions,
                          bulkCol = bulk_col,
                          bulkSingle = bool(attr_get("dt_bulk_single")),
                          )
            if bulk_col <= action_col:
                action_col += 1

        # Row actions
        row_actions = attr_get("dt_row_actions", s3.actions)
        if row_actions is None:
            row_actions = []
        config.update(actionCol = action_col,
                      rowActions = row_actions,
                      )

        # Grouping
        groups = attr_get("dt_group")
        if groups:
            if not isinstance(groups, list):
                groups = [groups]
            dt_group = []
            for group in groups:
                if bulk_actions and bulk_col <= group:
                    group += 1
                if action_col >= group:
                    group -= 1
                dt_group.append([group, "asc"])
            config.update(group = dt_group,
                          groupTotals = attr_get("dt_group_totals", []),
                          groupTitles = attr_get("dt_group_titles", []),
                          groupSpacing = attr_get("dt_group_space"),
                          groupIcon = attr_get("dt_group_types", []),
                          shrinkGroupedRows = attr_get("dt_shrink_groups"),
                          )

        # Orderby
        for order in orderby:
            if bulk_actions:
                if bulk_col <= order[0]:
                    order[0] += 1
            if action_col > 0 and action_col >= order[0]:
                order[0] -= 1
        config["order"] = orderby

        # Fixed column widths
        col_widths = attr_get("dt_col_widths")
        if col_widths is not None:
            # NB This requires "table-layout:fixed" in your CSS
            # You will likely need to specify all column widths if you do this
            # & won't have responsiveness
            config["colWidths"] = col_widths

        row_styles = attr_get("dt_styles")
        if not row_styles:
            row_styles = s3.dataTableStyle
        if row_styles:
            config["rowStyles"] = row_styles

        return config

    # -------------------------------------------------------------------------
    @classmethod
    def form(cls, table, table_id, config, rfields=None, cache=None, **attr):
        """
            Assembles the wrapper FORM for the data table, including
                - export icons
                - hidden inputs with configuration and cache parameters

            Args:
                table: The HTML table
                table_id: The DOM ID of the table
                orderby: the datatable ordering expression
                         - see http://datatables.net/reference/option/order
                rfields: the table columns (list of S3ResourceField)
                cache: parameters/data for the client-side cache

            Keyword Args:
                see config()

            Returns:
                a FORM instance
        """

        request = current.request
        s3 = current.response.s3
        settings = current.deployment_settings

        # Append table ID to response.s3.dataTableID
        table_ids = s3.dataTableID
        if not table_ids or not isinstance(table_ids, list):
            s3.dataTableID = [table_id]
        elif table_id not in table_ids:
            table_ids.append(table_id)

        attr_get = attr.get

        # Double Scroll
        if not settings.get_ui_datatables_responsive():
            double_scroll = attr_get("dt_double_scroll")
            if double_scroll is None:
                double_scroll = settings.get_ui_datatables_double_scroll()
            if double_scroll:
                if s3.debug:
                    script = "/%s/static/scripts/jquery.doubleScroll.js" % request.application
                else:
                    script = "/%s/static/scripts/jquery.doubleScroll.min.js" % request.application
                if script not in s3.scripts:
                    s3.scripts.append(script)
                table.add_class("doublescroll")

        # Build the form
        form = FORM(_class="dt-wrapper")

        # Form key (XSRF protection for Ajax actions)
        formkey = attr_get("dt_formkey")
        if formkey:
            form["hidden"] = {"_formkey": formkey}

        # Export formats
        if not s3.no_formats:
            form.append(cls.export_formats(base_url = attr_get("dt_base_url"),
                                           permalink = attr_get("dt_permalink"),
                                           rfields = rfields,
                                           ))
        # The HTML table
        form.append(table)

        # Hidden inputs for configuration and data
        def add_hidden(name, suffix, value):
            form.append(INPUT(_type = "hidden",
                              _id = "%s_%s" % (table_id, suffix),
                              _name = name,
                              _value = value,
                              ))

        add_hidden("config", "configurations", jsons(config))

        if cache:
            add_hidden("cache", "dataTable_cache", jsons(cache))

        bulk_actions = attr_get("dt_bulk_actions")
        if bulk_actions:
            bulk_selected = attr_get("dt_bulk_selected", "")
            if isinstance(bulk_selected, list):
                bulk_selected = ",".join(bulk_selected)
            add_hidden("mode", "dataTable_bulkMode", "Inclusive")
            add_hidden("selected", "dataTable_bulkSelection", "[%s]" % bulk_selected)
            add_hidden("filterURL", "dataTable_filterURL", config["ajaxUrl"])

        # InitComplete callback (processed in views/dataTables.html)
        callback = settings.get_ui_datatables_initComplete()
        if callback:
            s3.dataTable_initComplete = callback

        return form

    # -------------------------------------------------------------------------
    def json(self,
             totalrows,
             filteredrows,
             draw,
             colnames = None,
             action_col = None,
             stringify = True,
             **attr):
        """
            Builds a JSON object to update the data table

            Args:
                totalrows: number of rows available
                filteredrows: number of rows matching filters
                draw: unaltered copy of "draw" parameter sent from the client
                stringify: serialize the JSON object as string

            Keyword Args:
                dt_action_col: see config()
                dt_bulk_actions: see config()
                dt_bulk_col: see config()
        """

        if not colnames:
            colnames, action_col = self.columns(self.colnames, attr)

        dbid = colnames[action_col]
        bulk_checkbox = self.bulk_checkbox

        data_array = []
        addrow = data_array.append
        for row in self.data:
            details = []
            append = details.append
            for colname in colnames:
                if colname == "BULK":
                    append(str(bulk_checkbox(row[dbid])))
                else:
                    append(s3_str(row[colname]))
            addrow(details)

        output = {"recordsTotal": totalrows,
                  "recordsFiltered": filteredrows,
                  "data": data_array,
                  "draw": draw,
                  }

        if stringify:
            output = jsons(output)

        return output

    # -------------------------------------------------------------------------
    @staticmethod
    def columns(colnames, attr):
        """
            Adds the action columns into the columns list

            Args:
                colnames: the list of column keys

            Keyword Args:
                dt_action_col: see config()
                dt_bulk_actions: see config()
                dt_bulk_col: see config()

            Returns:
                tuple (colnames, action_col), with the revised list of column
                keys and the index of the action column
        """

        attr_get = attr.get

        # Move the action column (first column) to the right place
        action_col = attr_get("dt_action_col", 0) % len(colnames)
        if action_col != 0:
            colnames = colnames[1:action_col+1] + [colnames[0]] + colnames[action_col+1:]

        # Insert the bulk action column, if necessary
        bulk_actions = attr_get("dt_bulk_actions")
        if bulk_actions:
            bulk_col = attr_get("dt_bulk_col", 0) % len(colnames)
            colnames.insert(bulk_col, "BULK")
            if bulk_col <= action_col:
                action_col += 1

        return colnames, action_col

    # -------------------------------------------------------------------------
    @staticmethod
    def bulk_checkbox(dbid):
        """
            Constructs a checkbox to select a row for bulk action

            Args:
                dbid: the row ID (=value of the action column)

            Returns:
                the checkbox (INPUT instance)
        """

        return INPUT(_class = "bulkcheckbox",
                     _type = "checkbox",
                     _value = False,
                     data = {"dbid": dbid},
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def export_formats(base_url = None,
                       permalink = None,
                       rfields = None,
                       ):
        """
            Constructs the export options widget

            Args:
                base_url: the base URL of the datatable (without
                          method or query vars) to construct export URLs
                permalink: the search result URL (including filters) for
                           the user to bookmark
                rfields: the table columns (list of S3ResourceField) to
                         auto-detect export format options, e.g. KML if
                         there is a location reference

            Notes:
                - the overall list of possible export formats (and their
                  respective icons/titles) is determined by
                  settings.ui.export_formats
                - the export formats available for a particular request
                  is controlled by current.response.s3.formats
                  * enable a format by adding extension:url
                  * disable a format by adding extension:None
                - default formats XLS, PDF and KML are always available
                  unless explicitly disabled in s3.formats
                - KML is only available by default if the table contains
                  a suitable location reference
        """

        T = current.T
        request = current.request

        settings = current.deployment_settings
        s3 = current.response.s3

        if base_url is None:
            base_url = request.url

        # Strip format extensions (e.g. .aadata or .iframe)
        default_url = base_url
        default_url = re.sub(r"(\/[a-zA-Z0-9_]*)(\.[a-zA-Z]*)", r"\g<1>", default_url)

        # Keep any URL filters
        get_vars = request.get_vars
        if get_vars:
            query = "&".join("%s=%s" % (k, v) for k, v in get_vars.items())
            default_url = "%s?%s" % (default_url, query)

        # Construct row of export icons
        # - icons appear in reverse order due to float-right
        icons = SPAN(_class = "list_formats")

        # All export formats
        export_formats = settings.get_ui_export_formats()
        if export_formats:
            icons.append("%s:" % T("Export as"))

            # Default available formats
            default_formats = ("xls", "pdf")

            # Available formats
            formats = dict(s3.formats)

            # Auto-add KML if there is a suitable location reference
            if "kml" not in formats and rfields:
                kml_fields = {"location_id", "site_id"}
                if any(rfield.fname in kml_fields for rfield in rfields):
                    formats["kml"] = default_url

            EXPORT = T("Export in %(format)s format")

            for fmt in export_formats:

                title = None
                if isinstance(fmt, tuple):
                    if len(fmt) >= 3:
                        title = fmt[2]
                    fmt, css = fmt[:2] if len(fmt) >= 2 else (fmt[0], "")
                else:
                    css = ""

                if fmt in default_formats:
                    url = formats.get(fmt, default_url)
                else:
                    url = formats.get(fmt)
                if not url:
                    continue

                css_class = "dt-export export_%s" % fmt
                if css:
                    css_class = "%s %s" % (css_class, css)

                if title is None:
                    if fmt == "map":
                        title = T("Show on Map")
                    else:
                        title = EXPORT % {"format": fmt.upper()}

                icons.append(DIV(_class = css_class,
                                 _title = title,
                                 data = {"url": url,
                                         "extension": fmt.split(".")[-1],
                                         },
                                 ))

        export_options = DIV(_class = "dt-export-options")

        # Append the permalink (if any)
        if permalink is not None:
            label = settings.get_ui_label_permalink()
            if label:
                link = A(T(label),
                         _href = permalink,
                         _class = "permalink",
                         )
                export_options.append(link)
                if len(icons):
                    export_options.append(" | ")

        # Append the icons
        export_options.append(icons)

        return export_options

    # -------------------------------------------------------------------------
    @staticmethod
    def i18n():
        """
            Generates a JavaScript fragment to inject translated
            strings (i18n.*) used by s3.ui.datatable.js, added to
            view in views/dataTables.html

            returns:
                the JavaScript fragment as str
        """

        T = current.T

        strings = {"sortAscending":  T("activate to sort column ascending"),
                   "sortDescending":  T("activate to sort column descending"),
                   "first":  T("First"),
                   "last":  T("Last"),
                   "next":  T("Next"),
                   "previous":  T("Previous"),
                   "emptyTable":  T("No records found"),
                   "info":  T("Showing _START_ to _END_ of _TOTAL_ entries"),
                   "infoEmpty":  T("Showing 0 to 0 of 0 entries"),
                   "infoFiltered":  T("(filtered from _MAX_ total entries)"),
                   "infoThousands":  current.deployment_settings.get_L10n_thousands_separator(),
                   "lengthMenu":  T("Show %(number)s entries") % {"number": "_MENU_"},
                   "loadingRecords":  T("Loading"),
                   "processing":  T("Processing"),
                   "search":  T("Search"),
                   "zeroRecords":  T("No matching records found"),
                   "selectAll":  T("Select All"),
                   }

        return "\n".join('''i18n.%s="%s"'''% (k, v) for k, v in strings.items())

# END =========================================================================
