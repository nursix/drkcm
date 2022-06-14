"""
    Value Range Filters

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

__all__ = ("RangeFilter",
           "SliderFilter",
           )

from gluon import current, DIV, INPUT, LABEL

from ..resource import S3ResourceField, S3URLQuery

from .base import FilterWidget

# =============================================================================
class RangeFilter(FilterWidget):
    """
        Numerical Range Filter Widget

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
    """

    # Overall class
    css_base = "range-filter"

    operator = ["ge", "le"]

    # Untranslated labels for individual input boxes.
    input_labels = {"ge": "Minimum", "le": "Maximum"}

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
        input_elements = DIV(_class="range-filter")
        ie_append = input_elements.append

        _id = attr["_id"]
        _variable = self._variable
        selector = self.selector

        for operator in self.operator:

            input_id = "%s-%s" % (_id, operator)

            input_box = INPUT(_name = input_id,
                              _id = input_id,
                              _type = "text",
                              _class = input_class,
                              )

            variable = _variable(selector, operator)

            # Populate with the value, if given
            # if user has not set any of the limits, we get [] in values.
            value = values.get(variable, None)
            if value not in [None, []]:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = value
                input_box["value"] = value

            ie_append(DIV(DIV(LABEL("%s:" % T(input_labels[operator]),
                                    _for = input_id,
                                    ),
                              _class = "range-filter-label",
                              ),
                          DIV(input_box,
                              _class = "range-filter-widget",
                              ),
                          _class = "range-filter-field",
                          ))

        return input_elements

    # -------------------------------------------------------------------------
    def data_element(self, variable):
        """
            Overrides FilterWidget.data_element(), constructs multiple
            hidden INPUTs (one per variable) with element IDs of the form
            <id>-<operator>-data (where no operator is translated as "eq").

            Args:
                variable: the variable(s)
        """

        if variable is None:
            operators = self.operator
            if type(operators) is not list:
                operators = [operators]
            variable = self._variable(self.selector, operators)
        else:
            # Split the operators off the ends of the variables.
            if type(variable) is not list:
                variable = [variable]
            parse_key = S3URLQuery.parse_key
            operators = [parse_key(v)[1] for v in variable]

        elements = []
        widget_id = self.attr["_id"]

        for o, v in zip(operators, variable):
            elements.append(
                INPUT(_type = "hidden",
                      _id = "%s-%s-data" % (widget_id, o),
                      _class = "filter-widget-data %s-data" % self.css_base,
                      _value = v,
                      ))

        return elements

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        minimum, maximum = self._options(resource)

        attr = self._attr(resource)
        options = {attr["_id"]: {"min": minimum,
                                 "max": maximum,
                                 }}
        return options

    # -------------------------------------------------------------------------
    def _options(self, resource):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource
        """

        # Find only values linked to records the user is
        # permitted to read, and apply any resource filters
        # (= use the resource query)
        query = resource.get_query()

        # Must include rfilter joins when using the resource
        # query (both inner and left):
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        rfield = S3ResourceField(resource, self.field)
        field = rfield.field

        row = current.db(query).select(field.min(),
                                       field.max(),
                                       join = join,
                                       left = left,
                                       ).first()

        minimum = row[field.min()]
        maximum = row[field.max()]

        return minimum, maximum

# =============================================================================
class SliderFilter(RangeFilter):
    """
        Filter widget for Ranges which is controlled by a Slider instead of
        INPUTs, wraps jQueryUI's Range Slider in S3.range_slider in S3.js

        Keyword Args:
            minimum: the minimum selectable value (defaults to minimum
                     actual value in the records)
            maximum: the maximum selectable value (defaults to maximum
                     actual value in the records)
            step: slider step length (default 1)
            type: the data type of the filter field ("float" or "int",
                  default is "int")
    """

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        T = current.T

        attr = self.attr
        opts = self.opts

        # CSS classes
        css_base = self.css_base
        css = attr.get("class")
        attr["_class"] = "%s %s range-filter-slider" % (css, css_base) if css else css_base
        input_class = "%s-%s" % (css_base, "input")

        # Widget
        widget = DIV(**attr)
        widget_id = attr["_id"]

        # Slider
        slider_id = "%s_slider" % str(self.field).replace(".", "_")
        slider = DIV(_id=slider_id)
        widget.append(slider)

        # Selectable range
        minimum = opts.get("minimum")
        maximum = opts.get("maximum")
        if minimum is None or maximum is None:
            min_value, max_value, empty = self._options(resource)
            if minimum is not None:
                min_value = minimum
            elif min_value is None or min_value > 0 and empty:
                min_value = 0
            if maximum is not None:
                max_value = maximum
            elif max_value is None:
                max_value = 0

        # Input fields
        input_ids = []
        selected = []
        for operator in self.operator:

            input_id = "%s-%s" % (widget_id, operator)
            input_ids.append(input_id)

            input_box = INPUT(_name = input_id,
                              _id = input_id,
                              _type = "text",
                              _class = input_class,
                              )

            variable = self._variable(self.selector, operator)

            value = values.get(variable)
            if value or value == 0:
                if type(value) is list:
                    value = value[0]
                input_box["_value"] = input_box["value"] = value
                selected.append(value)
            else:
                if operator == "ge":
                    selected.append(min_value)
                else:
                    selected.append(max_value)

            label = "%s:" % T(self.input_labels[operator])
            widget.append(DIV(DIV(LABEL(label,
                                        _for = input_id,
                                        ),
                                  _class = "range-filter-label",
                                  ),
                              DIV(input_box,
                                  _class = "range-filter-widget",
                                  ),
                              _class = "range-filter-field",
                              ))

        s3 = current.response.s3

        # Inject script
        script = '''i18n.slider_help="%s"''' % \
                 current.T("Click on the slider to choose a value")
        s3.js_global.append(script)

        datatype = opts.get("type", "int")
        if datatype == "int":
            script = '''S3.range_slider('%s','%s','%s',%i,%i,%i,[%i,%i])'''
        else:
            script = '''S3.range_slider('%s','%s','%s',%f,%f,%f,[%i,%i])'''
        params = (slider_id,
                  input_ids[0],
                  input_ids[1],
                  min_value,
                  max_value,
                  opts.get("step", 1),
                  selected[0],
                  selected[1],
                  )
        s3.jquery_ready.append(script % params)

        return widget

    # -------------------------------------------------------------------------
    def ajax_options(self, resource):
        """
            Method to Ajax-retrieve the current options of this widget

            Args:
                resource: the CRUDResource
        """

        minimum, maximum = self._options(resource)[:2]

        attr = self._attr(resource)
        options = {attr["_id"]: {"min": minimum,
                                 "max": maximum,
                                 }}
        return options

    # -------------------------------------------------------------------------
    def _options(self, resource):
        """
            Helper function to retrieve the current options for this
            filter widget

            Args:
                resource: the CRUDResource

            Returns:
                tuple (min_value, max_value, empty), with "empty"
                indicating whether there are records with None-values
        """

        db = current.db

        # Find only values linked to records the user is
        # permitted to read, and apply any resource filters
        # (= use the resource query)
        query = resource.get_query()

        # Must include rfilter joins when using the resource
        # query (both inner and left):
        rfilter = resource.rfilter
        if rfilter:
            join = rfilter.get_joins()
            left = rfilter.get_joins(left = True)
        else:
            join = left = None

        rfield = S3ResourceField(resource, self.field)

        # If the filter field is in a joined table itself,
        # include the join for that table
        joins = rfield.join
        for tname in joins:
            query &= joins[tname]

        field = rfield.field
        row = db(query).select(field.min(),
                               field.max(),
                               join = join,
                               left = left,
                               ).first()

        minimum = row[field.min()]
        maximum = row[field.max()]

        # Check if there are records with no value
        empty = db(query & (field == None)).select(resource.table.id,
                                                   join = join,
                                                   left = left,
                                                   limitby = (0, 1)
                                                   ).first()

        return minimum, maximum, bool(empty)

# END =========================================================================
