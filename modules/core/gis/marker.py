"""
    Map Markers

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

from gluon import current
from gluon.storage import Storage

from .base import GIS

# =============================================================================
class Marker:
    """
        Represents a Map Marker

        TODO Support Markers in Themes
    """

    def __init__(self,
                 marker=None,
                 marker_id=None,
                 layer_id=None,
                 tablename=None):
        """
            Args:
                marker: Storage object with image/height/width (looked-up in bulk)
                marker_id: id of record in gis_marker
                layer_id: layer_id to lookup marker in gis_style (unused)
                tablename: used to identify whether to provide a default marker as fallback
        """

        no_default = False
        if not marker:
            db = current.db
            s3db = current.s3db
            mtable = s3db.gis_marker
            config = None
            if marker_id:
                # Lookup the Marker details from it's ID
                marker = db(mtable.id == marker_id).select(mtable.image,
                                                           mtable.height,
                                                           mtable.width,
                                                           limitby = (0, 1),
                                                           cache=s3db.cache
                                                           ).first()
            elif layer_id:
                # Check if we have a Marker defined for this Layer
                config = GIS.get_config()
                stable = s3db.gis_style
                query = (stable.layer_id == layer_id) & \
                        ((stable.config_id == config.id) | \
                         (stable.config_id == None)) & \
                        (stable.marker_id == mtable.id) & \
                        (stable.record_id == None)
                marker = db(query).select(mtable.image,
                                          mtable.height,
                                          mtable.width,
                                          limitby = (0, 1)).first()

        if not marker:
            # Check to see if we're a Polygon/LineString
            # (& hence shouldn't use a default marker)
            if tablename == "gis_layer_shapefile":
                table = db.gis_layer_shapefile
                query = (table.layer_id == layer_id)
                layer = db(query).select(table.gis_feature_type,
                                         limitby = (0, 1)).first()
                if layer and layer.gis_feature_type != 1:
                    no_default = True
            #elif tablename == "gis_layer_feature":
            #    table = db.gis_layer_feature
            #    query = (table.layer_id == layer_id)
            #    layer = db(query).select(table.polygons,
            #                             limitby = (0, 1)).first()
            #    if layer and layer.polygons:
            #       no_default = True

        if marker:
            self.image = marker["image"]
            self.height = marker["height"]
            self.width = marker["width"]
        elif no_default:
            self.image = None
        else:
            # Default Marker
            if not config:
                config = GIS.get_config()
            self.image = config.marker_image
            self.height = config.marker_height
            self.width = config.marker_width

    # -------------------------------------------------------------------------
    def add_attributes_to_output(self, output):
        """
            Called by Layer.as_dict()
        """

        if self.image:
            output["marker"] = self.as_json_dict()

    # -------------------------------------------------------------------------
    def as_dict(self):
        """
            Called by gis.get_marker(), feature_resources & methods/profile
        """

        if self.image:
            marker = Storage(image = self.image,
                             height = self.height,
                             width = self.width,
                             )
        else:
            marker = None
        return marker

    # -------------------------------------------------------------------------
    #def as_json(self):
    #    """
    #        Called by nothing
    #    """

    #    output = dict(i = self.image,
    #                  h = self.height,
    #                  w = self.width,
    #                  )
    #    return json.dumps(output, separators=JSONSEPARATORS)

    # -------------------------------------------------------------------------
    def as_json_dict(self):
        """
            Called by Style.as_dict() and add_attributes_to_output()
        """

        if self.image:
            marker = {"i": self.image,
                      "h": self.height,
                      "w": self.width,
                      }
        else:
            marker = None
        return marker

# END =========================================================================
