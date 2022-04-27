"""
    Text Filters

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

__all__ = ("TextFilter",
           )

from gluon import INPUT

from .base import FilterWidget

# =============================================================================
class TextFilter(FilterWidget):
    """
        Text filter widget

        Keyword Args:
            label: label for the widget
            comment: comment for the widget
            hidden: render widget initially hidden (="advanced" option)
            match_any: match any of the strings
    """

    css_base = "text-filter"

    operator = "like"

    # -------------------------------------------------------------------------
    def widget(self, resource, values):
        """
            Render this widget as HTML helper object(s)

            Args:
                resource: the resource
                values: the search values from the URL query
        """

        attr = self.attr

        if "_size" not in attr:
            attr.update(_size="40")
        css = attr.get("class")
        attr["_class"] = "%s %s" % (css, self.css_base) if css else self.css_base
        attr["_type"] = "text"

        # Match any or all of the strings entered?
        data = attr.get("data", {})
        data["match"] = "any" if self.opts.get("match_any") else "all"
        attr["data"] = data

        values = [v.strip("*") for v in values if v is not None]
        if values:
            attr["_value"] = " ".join(values)

        return INPUT(**attr)

# END =========================================================================
