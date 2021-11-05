"""
    Layer Styles

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

import json

from gluon import current, URL
from gluon.storage import Storage

from ..tools import JSONERRORS

from .base import GIS
from .marker import Marker
from .layers import CLUSTER_DISTANCE, CLUSTER_THRESHOLD

# =============================================================================
class Style:
    """
        Represents a Map Style
    """

    def __init__(self,
                 style_id = None,
                 layer_id = None,
                 aggregate = None):

        db = current.db
        s3db = current.s3db
        table = s3db.gis_style
        fields = [table.marker_id,
                  table.opacity,
                  table.popup_format,
                  # @ToDo: if-required
                  #table.url_format,
                  table.cluster_distance,
                  table.cluster_threshold,
                  table.style,
                  ]

        if style_id:
            query = (table.id == style_id)
            limitby = (0, 1)

        elif layer_id:
            config = GIS.get_config()
            # @ToDo: if record_id:
            query = (table.layer_id == layer_id) & \
                    (table.record_id == None) & \
                    ((table.config_id == config.id) | \
                     (table.config_id == None))
            if aggregate is not None:
                query &= (table.aggregate == aggregate)
            fields.append(table.config_id)
            limitby = (0, 2)

        else:
            # Default style for this config
            # - falling back to Default config
            config = GIS.get_config()
            ctable = db.gis_config
            query = (table.config_id == ctable.id) & \
                    ((ctable.id == config.id) | \
                     (ctable.uuid == "SITE_DEFAULT")) & \
                    (table.layer_id == None)
            fields.append(ctable.uuid)
            limitby = (0, 2)

        styles = db(query).select(*fields,
                                  limitby=limitby)

        if len(styles) > 1:
            if layer_id:
                # Remove the general one
                _filter = lambda row: row.config_id == None
            else:
                # Remove the Site Default
                _filter = lambda row: row["gis_config.uuid"] == "SITE_DEFAULT"
            styles.exclude(_filter)

        if styles:
            style = styles.first()
            if not layer_id and "gis_style" in style:
                style = style["gis_style"]
        else:
            current.log.error("Style not found!")
            style = None

        if style:
            if style.marker_id:
                style.marker = Marker(marker_id = style.marker_id)
            if aggregate is True:
                # Use gis/location controller in all reports
                style.url_format = "%s/{id}.plain" % URL(c="gis", f="location")
            elif layer_id:
                # Build from controller/function
                ftable = s3db.gis_layer_feature
                layer = db(ftable.layer_id == layer_id).select(ftable.controller,
                                                               ftable.function,
                                                               limitby = (0, 1)
                                                               ).first()
                if layer:
                    style.url_format = "%s/{id}.plain" % \
                        URL(c=layer.controller, f=layer.function)

        self.style = style

    # -------------------------------------------------------------------------
    def as_dict(self):
        """

        """

        # Not JSON-serializable
        #return self.style
        style = self.style
        output = Storage()
        if not style:
            return output
        if hasattr(style, "marker"):
            output.marker = style.marker.as_json_dict()
        opacity = style.opacity
        if opacity and opacity not in (1, 1.0):
            output.opacity = style.opacity
        if style.popup_format:
            output.popup_format = style.popup_format
        if style.url_format:
            output.url_format = style.url_format
        cluster_distance = style.cluster_distance
        if cluster_distance is not None and \
           cluster_distance != CLUSTER_DISTANCE:
            output.cluster_distance = cluster_distance
        cluster_threshold = style.cluster_threshold
        if cluster_threshold is not None and \
           cluster_threshold != CLUSTER_THRESHOLD:
            output.cluster_threshold = cluster_threshold
        if style.style:
            if isinstance(style.style, str):
                # Native JSON
                try:
                    style.style = json.loads(style.style)
                except JSONERRORS:
                    current.log.error("Unable to decode Style: %s" % style.style)
                    style.style = None
            output.style = style.style
        return output

# END =========================================================================

