"""
    Date Range Filters

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

__all__ = ("DateFilter",
           "AgeFilter",
           )

import datetime

from gluon import current, DIV, INPUT, LABEL, OPTION, SELECT, TAG
from gluon.storage import Storage

from s3dal import Field

from ..tools import IS_UTC_DATE, S3DateTime, s3_decode_iso_datetime, s3_relative_datetime
from ..ui import S3CalendarWidget
from ..resource import S3ResourceField

from .valuerange import RangeFilter

# =============================================================================
class DateFilter(RangeFilter):
    """
        Date Range Filter Widget
            - use a single field or a pair of fields for start_date/end_date

        Keyword Args:
            fieldtype: explicit field type "date" or "datetime" to
                       use for context or virtual fields
            hide_time: don't show time selector

    """

    css_base = "date-filter"

    operator = ["ge", "le"]

    # Untranslated labels for individual input boxes.
    input_labels = {"ge": "From", "le": "To"}

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        css_base = self.css_base

        attr = self.attr
        opts_get = self.opts.get

        # CSS class and element ID
        css = attr.get("class")
        _class = "%s %s" % (css, css_base) if css else css_base

        _id = attr["_id"]
        if not resource and not _id:
            raise SyntaxError("%s: _id parameter required when rendered without resource." % \
                              self.__class__.__name__)

        # Picker options
        clear_text = opts_get("clear_text", None)
        hide_time = opts_get("hide_time", False)

        # Selectable Range
        if self._auto_range():
            minimum, maximum = self._options(resource)
        else:
            minimum = maximum = None

        # Generate the input elements
        filter_widget = DIV(_id=_id, _class=_class)
        append = filter_widget.append

        # Classes and labels for the individual date/time inputs
        T = current.T
        input_class = "%s-%s" % (css_base, "input")
        input_labels = self.input_labels

        get_variable = self._variable

        fields = self.field
        if type(fields) is not list:
            fields = [fields]
            selector = self.selector
        else:
            selectors = self.selector.split("|")

        start = True
        for field in fields:
            # Determine the field type
            if resource:
                rfield = S3ResourceField(resource, field)
                field = rfield.field
            else:
                rfield = field = None

            if not field:
                if rfield:
                    # Virtual field
                    tname, fname = rfield.tname, rfield.fname
                else:
                    # Filter form without resource
                    tname, fname = "notable", "datetime"
                ftype = opts_get("fieldtype", "datetime")
                # S3CalendarWidget requires a Field
                field = Field(fname, ftype, requires = IS_UTC_DATE())
                field.tablename = field._tablename = tname
            else:
                ftype = rfield.ftype

            if len(fields) == 1:
                operators = self.operator
            elif start:
                operators = ["ge"]
                selector = selectors[0]
                start = False
            else:
                operators = ["le"]
                selector = selectors[1]
                input_class += " end_date"

            # Do we want a timepicker?
            timepicker = False if ftype == "date" or hide_time else True
            if timepicker and "datetimepicker" not in input_class:
                input_class += " datetimepicker"
            if ftype != "date" and hide_time:
                # Indicate that this filter is for a datetime field but
                # with a hidden time selector (so it shall add a suitable
                # time fragment automatically)
                input_class += " hide-time"

            for operator in operators:

                input_id = "%s-%s" % (_id, operator)

                # Make the two inputs constrain each other
                set_min = set_max = None
                if operator == "ge":
                    set_min = "#%s-%s" % (_id, "le")
                elif operator == "le":
                    set_max = "#%s-%s" % (_id, "ge")

                # Instantiate the widget
                widget = S3CalendarWidget(timepicker = timepicker,
                                          month_selector = True,
                                          minimum = minimum,
                                          maximum = maximum,
                                          set_min = set_min,
                                          set_max = set_max,
                                          clear_text = clear_text,
                                          )

                # Currently selected value
                dtstr = self._format_value(values,
                                           get_variable(selector, operator),
                                           timepicker = timepicker,
                                           )

                # Render the widget
                picker = widget(field,
                                dtstr,
                                _class = input_class,
                                _id = input_id,
                                _name = input_id,
                                )

                if operator in input_labels:
                    label = DIV(LABEL("%s:" % T(input_labels[operator]),
                                      _for = input_id,
                                      ),
                                _class = "range-filter-label",
                                )
                else:
                    label = ""

                # Append label and widget
                append(DIV(label,
                           DIV(picker,
                               _class = "range-filter-widget",
                               ),
                           _class = "range-filter-field",
                           ))

        return filter_widget

    # -------------------------------------------------------------------------
    def __call__(self, resource, get_vars=None, alias=None):
        """
            Entry point for the form builder
            - subclassed from FilterWidget to handle 'available' selector

            Args:
                resource: the CRUDResource to render the widget for
                get_vars: the GET vars (URL query vars) to prepopulate
                          the widget
                alias: the resource alias to use
        """

        self.alias = alias

        # Initialize the widget attributes
        self._attr(resource)

        # Extract the URL values to populate the widget
        variable = self.variable(resource, get_vars)

        defaults = {}
        for k, v in self.values.items():
            if k.startswith("available"):
                selector = k
            else:
                selector = self._prefix(k)
            defaults[selector] = v

        if type(variable) is list:
            values = Storage()
            for k in variable:
                if k in defaults:
                    values[k] = defaults[k]
                else:
                    values[k] = self._values(get_vars, k)
        else:
            if variable in defaults:
                values = defaults[variable]
            else:
                values = self._values(get_vars, variable)

        # Construct and populate the widget
        widget = self.widget(resource, values)

        # Recompute variable in case operator got changed in widget()
        if self.alternatives:
            variable = self._variable(self.selector, self.operator)

        # Construct the hidden data element
        data = self.data_element(variable)

        if type(data) is list:
            data.append(widget)
        else:
            data = [data, widget]
        return TAG[""](*data)

    # -------------------------------------------------------------------------
    def data_element(self, variable):
        """
            Overrides FilterWidget.data_element(), constructs multiple
            hidden INPUTs (one per variable) with element IDs of the form
            <id>-<operator>-data (where no operator is translated as "eq").

            Args:
                variable: the variable(s)
        """

        fields = self.field
        if type(fields) is not list:
            # Use function from RangeFilter parent class
            return super(DateFilter, self).data_element(variable)

        selectors = self.selector.split("|")
        operators = self.operator

        elements = []
        widget_id = self.attr["_id"]

        start = True
        for selector in selectors:
            if start:
                operator = operators[0]
                start = False
            else:
                operator = operators[1]
            variable = self._variable(selector, [operator])[0]

            elements.append(
                INPUT(_type = "hidden",
                      _id = "%s-%s-data" % (widget_id, operator),
                      _class = "filter-widget-data %s-data" % self.css_base,
                      _value = variable,
                      ))

        return elements

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        if self._auto_range():

            minimum, maximum = self._options(resource)
            ISO = "%Y-%m-%dT%H:%M:%S"
            if minimum:
                minimum = minimum.strftime(ISO)
            if maximum:
                maximum = maximum.strftime(ISO)

            attr = self._attr(resource)
            options = {attr["_id"]: {"min": minimum,
                                     "max": maximum,
                                     }}
        else:
            options = {}

        return options

    # -------------------------------------------------------------------------
    def _options(self, resource):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
                as_str: return date as ISO-formatted string not raw DateTime
        """

        query = resource.get_query()
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        fields = self.field
        if type(fields) is list:
            # Separate start/end fields
            srfield = S3ResourceField(resource, fields[0])
            erfield = S3ResourceField(resource, fields[0])

            # Include field joins (if fields are in joined tables)
            sjoins = srfield.join
            for tname in sjoins:
                query &= sjoins[tname]
            ejoins = erfield.join
            for tname in ejoins:
                if tname not in sjoins:
                    query &= ejoins[tname]

            start_field = srfield.field
            end_field = erfield.field

            row = current.db(query).select(start_field.min(),
                                           start_field.max(),
                                           end_field.max(),
                                           join = join,
                                           left = left,
                                           ).first()
            minimum = row[start_field.min()]
            maximum = row[start_field.max()]
            end_max = row[end_field.max()]
            if end_max:
                maximum = max(maximum, end_max)
        else:
            # Single filter field
            rfield = S3ResourceField(resource, fields)

            # Include field joins (if field is in joined table)
            joins = rfield.join
            for tname in joins:
                query &= joins[tname]

            field = rfield.field
            row = current.db(query).select(field.min(),
                                           field.max(),
                                           join = join,
                                           left = left,
                                           ).first()
            minimum = row[field.min()]
            maximum = row[field.max()]

        # Ensure that we can select the extreme values
        minute_step = 5
        timedelta = datetime.timedelta
        if minimum:
            minimum -= timedelta(minutes = minute_step)
        if maximum:
            maximum += timedelta(minutes = minute_step)

        return minimum, maximum

    # -------------------------------------------------------------------------
    def _auto_range(self):
        """
            Whether to automatically determine minimum/maximum selectable
            dates; deployment setting with per-widget override option
            "auto_range"

            Returns:
                bool
        """

        auto_range = self.opts.get("auto_range")
        if auto_range is None:
            # Not specified for widget => apply global setting
            auto_range = current.deployment_settings.get_search_dates_auto_range()

        return auto_range

    # -------------------------------------------------------------------------
    @staticmethod
    def _format_value(values, variable, timepicker=True):
        """
            Format a selected value in local format as expected by
            the calender widget

            Args:
                values: the selected values as dict {variable: value}
                variable: the relevant variable
                timepicker: whether the widget uses a time picker

            Returns:
                the formatted value as str
        """

        value = values.get(variable)
        if value in (None, []):
            value = None
        elif type(value) is list:
            value = value[0]

        # Widget expects a string in local calendar and format
        if isinstance(value, str):
            # URL filter or filter default come as string in
            # Gregorian calendar and ISO format => convert into
            # a datetime
            try:
                dt = s3_decode_iso_datetime(value)
            except ValueError:
                dt = None
        else:
            # Assume datetime
            dt = value

        if dt:
            if timepicker:
                dtstr = S3DateTime.datetime_represent(dt, utc=False)
            else:
                dtstr = S3DateTime.date_represent(dt, utc=False)
        else:
            dtstr = None

        return dtstr

# =============================================================================
class AgeFilter(RangeFilter):

    css_base = "age-filter"

    operator = ["le", "ge"]

    # Untranslated labels for individual input boxes.
    input_labels = {"le": "", "lt": "", "gt": "up to", "ge": "up to"}

    # -------------------------------------------------------------------------
    def __init__(self, field=None, **attr):
        """
            Keyword Args:
                exact       - in which set to include exact age matches
                              * "from age to >age" (="from")
                              * "from <age to age" (="to")
                              * both (the default)

            Note:
                - with exact="from" or "to", filtering for "from age to age"
                  would result in an empty set
                - with exact="both", filtering for "from age to age" will
                  return those records with exactly that age (to the day)
        """

        super().__init__(field=field, **attr)

        mode = self.opts.get("exact")
        if mode == "from":
            self.operator = ["le", "gt"]
        elif mode == "to":
            self.operator = ["lt", "ge"]

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        css_base = self.css_base

        attr = self.attr
        css = attr.get("class")
        attr["_class"] = "%s %s" % (css, css_base) if css else css_base

        input_class = "%s-%s" % (css_base, "input")
        input_labels = self.input_labels

        _id = attr["_id"]
        _variable = self._variable
        selector = self.selector

        # Generate the input elements
        input_elements = DIV()
        ie_append = input_elements.append
        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)
            variable = _variable(selector, operator)

            # The currently selected value
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]

            # Selectable options
            options = self.options(value)
            zero = self.opts.get("zero", "") if operator in ("le", "lt") else ""
            input_opts = [OPTION(zero, _value="")]
            selected_value = None
            for l, v, _, selected in options:
                if selected:
                    selected_value = v
                option = OPTION(l, _value=v, _selected="selected" if selected else None)
                input_opts.append(option)

            # Input Element
            input_box = SELECT(input_opts,
                               _id = input_id,
                               _class = input_class,
                               _value = selected_value,
                               )

            label = input_labels[operator]
            if label:
                label = DIV(LABEL("%s:" % T(input_labels[operator]),
                                  _for = input_id,
                                  ),
                            _class = "age-filter-label",
                            )

            ie_append(DIV(label,
                          DIV(input_box,
                              _class = "age-filter-widget",
                              ),
                          _class = "range-filter-field",
                          ))

        return input_elements

    # -------------------------------------------------------------------------
    def options(self, selected):
        """
            Returns the options for an age selector

            Args:
                operator: the operator for the selector
                selected: the currently selected cutoff-date (datetime.date)

            Returns:
                A sorted list of options [(label, value, cutoff-date, is-selected)]
        """

        opts = self.opts
        minimum = max(1, opts.get("minimum", 0))
        maximum = opts.get("maximum", 120)

        T = current.T

        cutoff = self.cutoff_date

        options = []
        append = options.append
        for i in range(minimum, maximum + 1):

            label = "1 %s" % T("year") if i == 1 else "%s %s" % (i, T("years"))
            value = "-%sY" % i
            append((label, value, cutoff(value), value == selected))

        # Add other options
        # - options format: (label, relative-date-expression)
        extra = self.opts.get("extra_options")
        if extra:
            for label, value in extra:
                append((label, value, cutoff(value), value == selected))

        options = sorted(options, key=lambda i: i[2], reverse=True)

        return options

    # -------------------------------------------------------------------------
    @staticmethod
    def cutoff_date(value):
        """
            Calculates the cutoff-date for a relative-date string

            Args:
                value: a relative-date string

            Returns:
                the cutoff-date (datetime.date), or None for invalid values
        """

        dt = s3_relative_datetime(value)
        return S3DateTime.to_local(dt).date() if dt else None

# END =========================================================================
