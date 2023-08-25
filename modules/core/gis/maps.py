"""
    Map Specification

    Copyright: (c) 2022-2022 Sahana Software Foundation

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

DEFAULT = lambda: None

from gluon import current, URL

from .context import MapContext, MapResource, Offering

# =============================================================================
class Map:
    """ A multi-layered map """

    def __init__(self, catalog_layers=True):
        """
            Args:
                catalog_layers: add all enabled catalog layers (True),
                                or just the default base layer (False)
        """

        self.catalog_layers = catalog_layers

        self._layers = None

    # -------------------------------------------------------------------------
    @property
    def config(self):
        """
            The applicable GIS configuration (lazy property)
        """

        # TODO make overridable
        return current.gis.get_config()

    # -------------------------------------------------------------------------
    @property
    def uri(self):
        """
            A universal identifier for this instance
        """

        # TODO handle missing config.ids
        config = self.config
        uri_vars = {"config": ",".join(map(str, config.ids))} if config.ids else {}

        return URL(c="gis", f="context", args=[], vars=uri_vars, host=True)

    # -------------------------------------------------------------------------
    @property
    def layers(self):
        """

        """

        layers = self._layers
        if layers is None:

            layers = self._layers = []

            config = self.config

            db = current.db
            s3db = current.s3db

            ltable = s3db.gis_layer_config
            etable = s3db.gis_layer_entity

            join = [etable.on(etable.layer_id == ltable.layer_id)]
            fields = [etable.instance_type,
                      etable.layer_id,
                      ltable.visible,
                      ltable.base,
                      ltable.dir,
                      ]

            query = (ltable.deleted == False)
            if self.catalog_layers:
                # Add all enabled catalog layers linked to any config.ids
                query &= (ltable.config_id.belongs(config.ids)) & \
                         (ltable.enabled == True)
            else:
                # Add just the base layer for config.id
                query &= (ltable.base == True) & (ltable.config_id == config.id)

            rows = db(query).select(*fields, join=join, orderby=etable.name)
            if not rows:
                # TODO Add the SITE_DEFAULT base layer
                pass

            instances = self.load_layers(rows)
            append = layers.append
            for row in rows:
                entity = row.gis_layer_entity
                layer_type = LAYERS.get(entity.instance_type)
                instance = instances.get(entity.layer_id)
                if layer_type and instance:
                    config = row.gis_layer_config
                    layer = layer_type(**instance)
                    layer.update(active = config.visible,
                                 base_layer = config.base,
                                 folder = config.dir,
                                 )
                    append(layer)

        return layers

    # -------------------------------------------------------------------------
    def add_layer(self, instance_type, **kwargs):
        """
            Adds (appends) a layer to this map

            Args:
                instance_type: the layer instance type (table name)

            Keyword Args:
                name: the layer title
                description: the layer description
                *: other layer parameters

            Returns:
                the newly added Layer
        """

        layer = LAYERS.get(instance_type)(**kwargs)

        self.layers.append(layer)

        return layer

    # -------------------------------------------------------------------------
    @staticmethod
    def load_layers(rows):

        db = current.db
        s3db = current.s3db

        # Group layer_ids by tablename
        types = {}
        for row in rows:
            entity = row.gis_layer_entity
            tablename = entity.instance_type
            if tablename in types:
                types[tablename].append(entity.layer_id)
            else:
                types[tablename] = [entity.layer_id]

        # Load the instance records
        instances = {}
        for tablename, layer_ids in types.items():

            table = s3db.table(tablename)
            if not table:
                continue

            if len(layer_ids) > 1:
                query = table.layer_id.belongs(layer_ids)
            else:
                query = table.layer_id == layer_ids[0]

            # TODO which fields? (don't need meta-fields except id/uuid/layer_id/modified_on)
            fields = [table[fn] for fn in table.fields]

            irows = db(query).select(*fields).as_dict(key="layer_id")
            instances.update(irows)

        return instances

    # -------------------------------------------------------------------------
    def context(self):
        """
            Returns the web services context (MapContext) for this map
        """

        context = MapContext(self.uri)

        for layer in self.layers:

            folder = layer.option("folder")
            if folder:
                folder = folder.lower().replace(" ", "_")
            if layer.option("base_layer"):
                folder = "/base"
            elif folder and folder[0] != "/":
                folder = "/%s" % folder
            elif not folder:
                folder = "/overlays"

            resource = MapResource(layer.uri, layer.name,
                                   active = layer.option("active"),
                                   folder = folder,
                                   )
            resource.modified_on = layer.attr.get("modified_on", context.modified_on)

            offerings = layer.offerings
            if offerings:
                for offering in offerings:
                    resource.add_offering(offering)

            context.append(resource)

        return context

    # -------------------------------------------------------------------------
    def viewer(self):
        """
            Configures and returns a viewer for this map
        """
        # TODO implement
        pass

# =============================================================================
class Layer:
    """ A map layer """

    def __init__(self, **kwargs):
        """
            Args:
                kwargs: the layer parameters
        """

        opts, attr = {}, {}

        for k, v in kwargs.items():
            if k[0] == "_":
                opts[k[1:]] = v
            else:
                attr[k] = v
        self.opts = opts
        self.attr = attr

        # TODO defaults
        self.layer_id = attr.get("layer_id")
        self.name = attr.get("name")

    # -------------------------------------------------------------------------
    def update(self, **options):

        self.opts.update(options)

    # -------------------------------------------------------------------------
    def option(self, key):

        return self.opts.get(key)

    # -------------------------------------------------------------------------
    @property
    def uri(self):
        """
            A universal resource identifier for this layer

            Returns:
                the identifier as str
        """

        # TODO handle missing layer_id
        return URL(c="gis", f="layer_entity", args=[self.layer_id], host=True)

    # -------------------------------------------------------------------------
    @property
    def offerings(self):
        """
            Web service offerings providing layer data

            Returns:
                list of Offering instances
        """
        return None

# =============================================================================
class LayerFeature(Layer):
    """ Internal Feature Layer """

    # TODO implement
    pass

# =============================================================================
class LayerOSM(Layer):
    """ OSM Tile Layer """

    # -------------------------------------------------------------------------
    @property
    def offerings(self):

        attr = self.attr
        offerings = []

        for fn in ("url1", "url2", "url3"):
            url = attr.get(fn)
            if url:
                url = "https:%s" % url.rstrip("?").rstrip("/")
                service = Offering("osm")
                service.add_operation("GetTile", href=url)
                offerings.append(service)

        return offerings

# =============================================================================
class LayerWMS(Layer):
    """ WMS Layer """

    # -------------------------------------------------------------------------
    @property
    def offerings(self):

        attr = self.attr
        offerings = []

        base_url = attr.get("url")
        if base_url:
            url = base_url.rstrip("?").rstrip("/")
            version = attr.get("version", "1.1.1")

            service = Offering("wms")
            service.add_operation("GetCapabilities",
                                  href = "%s?REQUEST=GetCapabilities&SERVICE=WMS&VERSION=%s" % (url, version),
                                  )
            offerings.append(service)

        return offerings

# =============================================================================
class LayerWFS(Layer):
    """ WFS Layer """

    # -------------------------------------------------------------------------
    @property
    def offerings(self):

        attr = self.attr
        offerings = []

        base_url = attr.get("url")
        if base_url:
            url = base_url.rstrip("?").rstrip("/")
            version = attr.get("version", "1.1.0")

            # Layer parameters for feature requests
            layer_arg = "TYPENAME" if version == "1.0.0" else "TYPENAMES"
            layer_name = ":".join(a.strip() for a in map(attr.get, ("featureNS", "featureType")) if a)

            service = Offering("wfs")
            service.add_operation("GetCapabilities",
                                  href = "%s?REQUEST=GetCapabilities&SERVICE=wfs&VERSION=%s" %
                                         (url, version),
                                  )
            service.add_operation("GetFeature",
                                  href = "%s?REQUEST=GetFeature&SERVICE=wfs&VERSION=%s&%s=%s" %
                                         (url, version, layer_arg, layer_name),
                                  )
            offerings.append(service)

        return offerings

# =============================================================================
# Specific layer classes by tablename
#
LAYERS = {"gis_layer_wms": LayerWMS,
          "gis_layer_wfs": LayerWFS,
          "gis_layer_openstreetmap": LayerOSM,
          }

# END =========================================================================
