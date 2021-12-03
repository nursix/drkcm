"""
    Map View/Widget with Search Result Layer

    Copyright: (c) 2010-2021 Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
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

from gluon import current, URL

from ..filters import S3FilterForm
from ..tools import get_crud_string

from .base import CRUDMethod

# =============================================================================
class S3Map(CRUDMethod):
    """
        Class to generate a Map linked to Search filters
    """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point to apply map method to CRUDRequests
                - produces a full page with S3FilterWidgets above a Map

            Args:
                r: the CRUDRequest instance
                attr: controller attributes for the request

            Returns:
                output object to send to the view
        """

        output = None

        if r.http == "GET":
            representation = r.representation
            if representation == "html":
                output = self.page(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def page(self, r, **attr):
        """
            Map page

            Args:
                r: the CRUDRequest instance
                attr: controller attributes for the request
        """

        output = {}

        if r.representation in ("html", "iframe"):

            response = current.response
            resource = self.resource
            get_config = resource.get_config
            tablename = resource.tablename

            widget_id = "default_map"

            title = get_crud_string(tablename, "title_map")
            output["title"] = title

            # Filter widgets
            filter_widgets = get_config("filter_widgets", None)
            if filter_widgets and not self.hide_filter:
                advanced = False
                for widget in filter_widgets:
                    if "hidden" in widget.opts and widget.opts.hidden:
                        advanced = resource.get_config("map_advanced", True)
                        break

                request = self.request
                # Apply filter defaults (before rendering the data!)
                S3FilterForm.apply_filter_defaults(r, resource)
                filter_formstyle = get_config("filter_formstyle", None)
                submit = resource.get_config("map_submit", True)
                filter_form = S3FilterForm(filter_widgets,
                                           formstyle=filter_formstyle,
                                           advanced=advanced,
                                           submit=submit,
                                           ajax=True,
                                           # URL to update the Filter Widget Status
                                           ajaxurl=r.url(method="filter",
                                                         vars={},
                                                         representation="options"),
                                           _class="filter-form",
                                           _id="%s-filter-form" % widget_id,
                                           )
                get_vars = request.get_vars
                filter_form = filter_form.html(resource, get_vars=get_vars, target=widget_id)
            else:
                # Render as empty string to avoid the exception in the view
                filter_form = ""

            output["form"] = filter_form

            # Map
            output["map"] = self.widget(r, widget_id=widget_id,
                                        callback='''S3.search.s3map()''', **attr)

            # View
            response.view = self._view(r, "map.html")

        else:
            r.error(415, current.ERROR.BAD_FORMAT)

        return output

    # -------------------------------------------------------------------------
    def widget(self,
               r,
               method = "map",
               widget_id = None,
               visible = True,
               **attr):
        """
            Render a Map widget suitable for use in an S3Filter-based page
            such as S3Summary

            Args:
                r: the CRUDRequest
                method: the widget method
                widget_id: the widget ID
                visible: whether the widget is initially visible
                attr: controller attributes

            Keyword Args:
                callback: callback to show the map:
                        - "DEFAULT".............call show_map as soon as all
                                                components are loaded and ready
                                                (= use default show_map callback)
                        - custom JavaScript.....invoked as soon as all components
                                                are loaded an ready
                        - None..................only load the components, map
                                                will be shown by a later explicit
                                                call to show_map (this is the default
                                                here since the map DIV would typically
                                                be hidden initially, e.g. summary tab)
        """

        callback = attr.get("callback")

        if not widget_id:
            widget_id = "default_map"

        gis = current.gis
        tablename = self.tablename

        ftable = current.s3db.gis_layer_feature

        def lookup_layer(prefix, name):
            query = (ftable.controller == prefix) & \
                    (ftable.function == name)
            layers = current.db(query).select(ftable.layer_id,
                                              ftable.style_default,
                                              )
            if len(layers) > 1:
                layers.exclude(lambda row: row.style_default == False)
            if len(layers) == 1:
                layer_id = layers.first().layer_id
            else:
                # We can't distinguish
                layer_id = None
            return layer_id

        prefix = r.controller
        name = r.function
        layer_id = lookup_layer(prefix, name)
        if not layer_id:
            # Try the tablename
            prefix, name = tablename.split("_", 1)
            layer_id = lookup_layer(prefix, name)

        # This URL is ignored if we have a layer_id:
        url = URL(extension="geojson", args=None, vars=r.get_vars)

        # Retain any custom filter parameters for the layer lookup
        custom_params = {k: v for k, v in r.get_vars.items() if k[:2] == "$$"}

        # @ToDo: Support maps with multiple layers (Dashboards)
        #_id = "search_results_%s" % widget_id
        _id = "search_results"
        feature_resources = [{"name"          : current.T("Search Results"),
                              "id"            : _id,
                              "layer_id"      : layer_id,
                              "tablename"     : tablename,
                              "url"           : url,
                              "custom_params" : custom_params,
                              # We activate in callback after ensuring URL is updated for current filter status
                              "active"        : False,
                              }]

        settings = current.deployment_settings
        catalogue_layers = settings.get_gis_widget_catalogue_layers()
        legend = settings.get_gis_legend()
        search = settings.get_gis_search_geonames()
        toolbar = settings.get_gis_toolbar()
        wms_browser = settings.get_gis_widget_wms_browser()
        if wms_browser:
            config = gis.get_config()
            if config.wmsbrowser_url:
                wms_browser = wms_browser = {"name" : config.wmsbrowser_name,
                                             "url" : config.wmsbrowser_url,
                                             }
            else:
                wms_browser = None

        map_widget = gis.show_map(id = widget_id,
                                  feature_resources = feature_resources,
                                  catalogue_layers = catalogue_layers,
                                  collapsed = True,
                                  legend = legend,
                                  toolbar = toolbar,
                                  save = False,
                                  search = search,
                                  wms_browser = wms_browser,
                                  callback = callback,
                                  )
        return map_widget

# END =========================================================================
