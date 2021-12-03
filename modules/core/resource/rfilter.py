"""
    Resource Filter

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

__all__ = ("S3ResourceFilter",
           )

from functools import reduce

from gluon import current
from gluon.storage import Storage

from s3dal import Rows

from .query import S3ResourceQuery, S3Joins, S3URLQuery

# =============================================================================
class S3ResourceFilter:
    """ Class representing a resource filter """

    def __init__(self,
                 resource,
                 id = None,
                 uid = None,
                 filter = None,
                 vars = None,
                 extra_filters = None,
                 filter_component = None):
        """
            Args:
                resource: the CRUDResource
                id: the record ID (or list of record IDs)
                uid: the record UID (or list of record UIDs)
                filter: a filter query (S3ResourceQuery or Query)
                vars: the dict of GET vars (URL filters)
                extra_filters: extra filters (to be applied on
                               pre-filtered subsets), as list of
                               tuples (method, expression)
                filter_component: the alias of the component the URL
                                  filters apply for (filters for this
                                  component must be handled separately)
        """

        self.resource = resource

        self.queries = []
        self.filters = []
        self.cqueries = {}
        self.cfilters = {}

        # Extra filters
        self._extra_filter_methods = None
        if extra_filters:
            self.set_extra_filters(extra_filters)
        else:
            self.efilters = []

        self.query = None
        self.rfltr = None
        self.vfltr = None

        self.transformed = None

        self.multiple = True
        self.distinct = False

        # Joins
        self.ijoins = {}
        self.ljoins = {}

        table = resource.table

        # Accessible/available query
        if resource.accessible_query is not None:
            method = []
            if resource._approved:
                method.append("read")
            if resource._unapproved:
                method.append("review")
            mquery = resource.accessible_query(method, table)
        else:
            mquery = (table._id > 0)

        # ID query
        if id is not None:
            if not isinstance(id, (list, tuple)):
                self.multiple = False
                mquery = (table._id == id) & mquery
            else:
                mquery = (table._id.belongs(id)) & mquery

        # UID query
        UID = current.xml.UID
        if uid is not None and UID in table:
            if not isinstance(uid, (list, tuple)):
                self.multiple = False
                mquery = (table[UID] == uid) & mquery
            else:
                mquery = (table[UID].belongs(uid)) & mquery

        # Deletion status
        DELETED = current.xml.DELETED
        if DELETED in table.fields and not resource.include_deleted:
            remaining = (table[DELETED] == False)
            mquery &= remaining

        parent = resource.parent
        if not parent:
            # Standard master query
            self.mquery = mquery

            # URL queries
            if vars:
                resource.vars = Storage(vars)

                if not vars.get("track"):
                    # Apply BBox Filter unless using S3Track to geolocate
                    bbox, joins = self.parse_bbox_query(resource, vars)
                    if bbox is not None:
                        self.queries.append(bbox)
                        if joins:
                            self.ljoins.update(joins)

                # Filters
                add_filter = self.add_filter

                # Current concept:
                # Interpret all URL filters in the context of master
                queries = S3URLQuery.parse(resource, vars)

                # @todo: Alternative concept (inconsistent?):
                # Interpret all URL filters in the context of filter_component:
                #if filter_component:
                #    context = resource.components.get(filter_component)
                #    if not context:
                #        context = resource
                #queries = S3URLQuery.parse(context, vars)

                for alias in queries:
                    if filter_component == alias:
                        for q in queries[alias]:
                            add_filter(q, component=alias, master=False)
                    else:
                        for q in queries[alias]:
                            add_filter(q)
                self.cfilters = queries
        else:
            # Parent filter
            pf = parent.rfilter
            if not pf:
                pf = parent.build_query()

            # Extended master query
            self.mquery = mquery & pf.get_query()

            # Join the master
            self.ijoins[parent._alias] = resource._join(reverse=True)

            # Component/link-table specific filters
            add_filter = self.add_filter
            aliases = [resource.alias]
            if resource.link is not None:
                aliases.append(resource.link.alias)
            elif resource.linked is not None:
                aliases.append(resource.linked.alias)
            for alias in aliases:
                for filter_set in (pf.cqueries, pf.cfilters):
                    if alias in filter_set:
                        for q in filter_set[alias]:
                            add_filter(q)

        # Additional filters
        if filter is not None:
            self.add_filter(filter)

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------
    @property
    def extra_filter_methods(self):
        """
            Getter for extra filter methods, lazy property so methods
            are only imported/initialized when needed

            Returns:
                dict {name: callable} of known named filter methods

            TODO document the expected signature of filter methods
        """

        methods = self._extra_filter_methods
        if methods is None:

            # @todo: implement hooks
            methods = {}

            self._extra_filter_methods = methods

        return methods

    # -------------------------------------------------------------------------
    # Manipulation
    # -------------------------------------------------------------------------
    def add_filter(self, query, component=None, master=True):
        """
            Extend this filter

            Args:
                query: a Query or S3ResourceQuery object
                component: alias of the component the filter shall be
                           added to (None for master)
                master: False to filter only component
        """

        alias = None
        if not master:
            if not component:
                return
            if component != self.resource.alias:
                alias = component

        if isinstance(query, S3ResourceQuery):
            self.transformed = None
            filters = self.filters
            cfilters = self.cfilters
            self.distinct |= query._joins(self.resource)[1]

        else:
            # DAL Query
            filters = self.queries
            cfilters = self.cqueries

        self.query = None
        if alias:
            if alias in self.cfilters:
                cfilters[alias].append(query)
            else:
                cfilters[alias] = [query]
        else:
            filters.append(query)
        return

    # -------------------------------------------------------------------------
    def add_extra_filter(self, method, expression):
        """
            Add an extra filter

            Args:
                method: a name of a known filter method, or a
                        callable filter method
                expression: the filter expression (string)
        """

        efilters = self.efilters
        efilters.append((method, expression))

        return efilters

    # -------------------------------------------------------------------------
    def set_extra_filters(self, filters):
        """
            Replace the current extra filters

            Args:
                filters: list of tuples (method, expression), or None
                         to remove all extra filters
        """

        self.efilters = []
        if filters:
            add = self.add_extra_filter
            for method, expression in filters:
                add(method, expression)

        return self.efilters

    # -------------------------------------------------------------------------
    # Getters
    # -------------------------------------------------------------------------
    def get_query(self):
        """ Get the effective DAL query """

        if self.query is not None:
            return self.query

        resource = self.resource

        query = reduce(lambda x, y: x & y, self.queries, self.mquery)
        if self.filters:
            if self.transformed is None:

                # Combine all filters
                filters = reduce(lambda x, y: x & y, self.filters)

                # Transform with external search engine
                transformed = filters.transform(resource)
                self.transformed = transformed

                # Split DAL and virtual filters
                self.rfltr, self.vfltr = transformed.split(resource)

            # Add to query
            rfltr = self.rfltr
            if isinstance(rfltr, S3ResourceQuery):

                # Resolve query against the resource
                rq = rfltr.query(resource)

                # False indicates that the subquery shall be ignored
                # (e.g. if not supported by platform)
                if rq is not False:
                    query &= rq

            elif rfltr is not None:

                # Combination of virtual field filter and web2py Query
                query &= rfltr

        self.query = query
        return query

    # -------------------------------------------------------------------------
    def get_filter(self):
        """ Get the effective virtual filter """

        if self.query is None:
            self.get_query()
        return self.vfltr

    # -------------------------------------------------------------------------
    def get_extra_filters(self):
        """
            Get the list of extra filters

            Returns:
                list of tuples (method, expression)
        """

        return list(self.efilters)

    # -------------------------------------------------------------------------
    def get_joins(self, left=False, as_list=True):
        """
            Get the joins required for this filter

            Args:
                left: get the left joins
                as_list: return a flat list rather than a nested dict
        """

        if self.query is None:
            self.get_query()

        joins = dict(self.ljoins if left else self.ijoins)

        resource = self.resource
        for q in self.filters:
            subjoins = q._joins(resource, left=left)[0]
            joins.update(subjoins)

        # Cross-component left joins
        parent = resource.parent
        if parent:
            pf = parent.rfilter
            if pf is None:
                pf = parent.build_query()

            parent_left = pf.get_joins(left=True, as_list=False)
            if parent_left:
                tablename = resource._alias
                if left:
                    for tn in parent_left:
                        if tn not in joins and tn != tablename:
                            joins[tn] = parent_left[tn]
                    joins[parent._alias] = resource._join(reverse=True)
                else:
                    joins.pop(parent._alias, None)

        if as_list:
            return [j for tablename in joins for j in joins[tablename]]
        else:
            return joins

    # -------------------------------------------------------------------------
    def get_fields(self):
        """ Get all field selectors in this filter """

        if self.query is None:
            self.get_query()

        if self.vfltr:
            return self.vfltr.fields()
        else:
            return []

    # -------------------------------------------------------------------------
    # Filtering
    # -------------------------------------------------------------------------
    def __call__(self, rows, start=None, limit=None):
        """
            Filter a set of rows by the effective virtual filter

            Args:
                rows: a Rows object
                start: index of the first matching record to select
                limit: maximum number of records to select
        """

        vfltr = self.get_filter()

        if rows is None or vfltr is None:
            return rows
        resource = self.resource
        if start is None:
            start = 0
        first = start
        if limit is not None:
            last = start + limit
            if last < first:
                first, last = last, first
            if first < 0:
                first = 0
            if last < 0:
                last = 0
        else:
            last = None
        i = 0
        result = []
        append = result.append
        for row in rows:
            if last is not None and i >= last:
                break
            success = vfltr(resource, row, virtual=True)
            if success or success is None:
                if i >= first:
                    append(row)
                i += 1
        return Rows(rows.db,
                    result,
                    colnames = rows.colnames,
                    compact = False)

    # -------------------------------------------------------------------------
    def apply_extra_filters(self, ids, start=None, limit=None):
        """
            Apply all extra filters on a list of record ids

            Args:
                ids: the pre-filtered set of record IDs
                limit: the maximum number of matching IDs to establish,
                       None to find all matching IDs

            Returns:
                a sequence of matching IDs
        """

        # Get the resource
        resource = self.resource

        # Get extra filters
        efilters = self.efilters

        # Resolve filter methods
        methods = self.extra_filter_methods
        filters = []
        append = filters.append
        for method, expression in efilters:
            if callable(method):
                append((method, expression))
            else:
                method = methods.get(method)
                if method:
                    append((method, expression))
                else:
                    current.log.warning("Unknown filter method: %s" % method)
        if not filters:
            # No applicable filters
            return ids

        # Clear extra filters so that apply_extra_filters is not
        # called from inside a filter method (e.g. if the method
        # uses resource.select)
        self.efilters = []

        # Initialize subset
        subset = set()
        tail = ids
        limit_ = limit

        while tail:

            if limit:
                head, tail = tail[:limit_], tail[limit_:]
            else:
                head, tail = tail, None

            match = head
            for method, expression in filters:
                # Apply filter
                match = method(resource, match, expression)
                if not match:
                    break

            if match:
                subset |= set(match)

            found = len(subset)

            if limit:
                if found < limit:
                    # Need more
                    limit_ = limit - found
                else:
                    # Found all
                    tail = None

        # Restore order
        subset = [item for item in ids if item in subset]

        # Select start
        if start:
            subset = subset[start:]

        # Restore extra filters
        self.efilters = efilters

        return subset

    # -------------------------------------------------------------------------
    def count(self, left=None, distinct=False):
        """
            Get the total number of matching records

            Args:
                left: left outer joins
                distinct: count only distinct rows
        """

        distinct |= self.distinct

        resource = self.resource
        if resource is None:
            return 0

        table = resource.table

        vfltr = self.get_filter()

        if vfltr is None and not distinct:

            tablename = table._tablename

            ijoins = S3Joins(tablename, self.get_joins(left=False))
            ljoins = S3Joins(tablename, self.get_joins(left=True))
            ljoins.add(left)

            join = ijoins.as_list(prefer=ljoins)
            left = ljoins.as_list()

            cnt = table._id.count()
            row = current.db(self.query).select(cnt,
                                                join=join,
                                                left=left).first()
            if row:
                return row[cnt]
            else:
                return 0

        else:
            data = resource.select([table._id.name],
                                   # We don't really want to retrieve
                                   # any rows but just count, hence:
                                   limit=1,
                                   count=True)
            return data["numrows"]

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------
    def __repr__(self):
        """ String representation of the instance """

        resource = self.resource

        inner_joins = self.get_joins(left=False)
        if inner_joins:
            inner = S3Joins(resource.tablename, inner_joins)
            ijoins = ", ".join([str(j) for j in inner.as_list()])
        else:
            ijoins = None

        left_joins = self.get_joins(left=True)
        if left_joins:
            left = S3Joins(resource.tablename, left_joins)
            ljoins = ", ".join([str(j) for j in left.as_list()])
        else:
            ljoins = None

        vfltr = self.get_filter()
        if vfltr:
            vfltr = vfltr.represent(resource)
        else:
            vfltr = None

        represent = "<S3ResourceFilter %s, " \
                    "query=%s, " \
                    "join=[%s], " \
                    "left=[%s], " \
                    "distinct=%s, " \
                    "filter=%s>" % (resource.tablename,
                                    self.get_query(),
                                    ijoins,
                                    ljoins,
                                    self.distinct,
                                    vfltr,
                                    )

        return represent

    # -------------------------------------------------------------------------
    @staticmethod
    def parse_bbox_query(resource, get_vars):
        """
            Generate a Query from a URL boundary box query; supports multiple
            bboxes, but optimised for the usual case of just 1

            Args:
                resource: the resource
                get_vars: the URL GET vars
        """

        tablenames = ("gis_location",
                      "gis_feature_query",
                      "gis_layer_shapefile",
                      )

        POLYGON = "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))"

        query = None
        joins = {}

        if get_vars:

            table = resource.table
            tablename = resource.tablename
            fields = table.fields

            introspect = tablename not in tablenames
            for k, v in get_vars.items():

                if k[:4] == "bbox":

                    if type(v) is list:
                        v = v[-1]
                    try:
                        minLon, minLat, maxLon, maxLat = v.split(",")
                    except ValueError:
                        # Badly-formed bbox - ignore
                        continue

                    # Identify the location reference
                    field = None
                    rfield = None
                    alias = False

                    if k.find(".") != -1:

                        # Field specified in query
                        fname = k.split(".")[1]
                        if fname not in fields:
                            # Field not found - ignore
                            continue
                        field = table[fname]
                        if query is not None or "bbox" in get_vars:
                            # Need alias
                            alias = True

                    elif introspect:

                        # Location context?
                        context = resource.get_config("context")
                        if context and "location" in context:
                            try:
                                rfield = resource.resolve_selector("(location)$lat")
                            except (SyntaxError, AttributeError):
                                rfield = None
                            else:
                                if not rfield.field or rfield.tname != "gis_location":
                                    # Invalid location context
                                    rfield = None

                        # Fall back to location_id (or site_id as last resort)
                        if rfield is None:
                            fname = None
                            for f in fields:
                                ftype = str(table[f].type)
                                if ftype[:22] == "reference gis_location":
                                    fname = f
                                    break
                                elif not fname and \
                                     ftype[:18] == "reference org_site":
                                    fname = f
                            field = table[fname] if fname else None

                        if not rfield and not field:
                            # No location reference could be identified => skip
                            continue

                    # Construct the join to gis_location
                    gtable = current.s3db.gis_location
                    if rfield:
                        joins.update(rfield.left)

                    elif field:
                        fname = field.name
                        gtable = current.s3db.gis_location
                        if alias:
                            gtable = gtable.with_alias("gis_%s_location" % fname)
                        tname = str(gtable)
                        ftype = str(field.type)
                        if ftype == "reference gis_location":
                            joins[tname] = [gtable.on(gtable.id == field)]
                        elif ftype == "reference org_site":
                            stable = current.s3db.org_site
                            if alias:
                                stable = stable.with_alias("org_%s_site" % fname)
                            joins[tname] = [stable.on(stable.site_id == field),
                                            gtable.on(gtable.id == stable.location_id)]
                        elif introspect:
                            # => not a location or site reference
                            continue

                    elif tablename in ("gis_location", "gis_feature_query"):
                        gtable = table

                    elif tablename == "gis_layer_shapefile":
                        # Find the layer_shapefile_%(layer_id)s component
                        # (added dynamically in gis/layer_shapefile controller)
                        gtable = None
                        hooks = current.s3db.get_hooks("gis_layer_shapefile")[1]
                        for alias in hooks:
                            if alias[:19] == "gis_layer_shapefile":
                                component = resource.components.get(alias)
                                if component:
                                    gtable = component.table
                                    break
                        # Join by layer_id
                        if gtable:
                            joins[str(gtable)] = \
                                [gtable.on(gtable.layer_id == table._id)]
                        else:
                            continue

                    # Construct the bbox filter
                    bbox_filter = None
                    if current.deployment_settings.get_gis_spatialdb():
                        # Use the Spatial Database
                        minLon = float(minLon)
                        maxLon = float(maxLon)
                        minLat = float(minLat)
                        maxLat = float(maxLat)
                        bbox = POLYGON % (minLon, minLat,
                                          minLon, maxLat,
                                          maxLon, maxLat,
                                          maxLon, minLat,
                                          minLon, minLat)
                        try:
                            # Spatial DAL & Database
                            bbox_filter = gtable.the_geom \
                                                .st_intersects(bbox)
                        except:
                            # Old DAL or non-spatial database
                            pass

                    if bbox_filter is None:
                        # Standard Query
                        bbox_filter = (gtable.lon > float(minLon)) & \
                                      (gtable.lon < float(maxLon)) & \
                                      (gtable.lat > float(minLat)) & \
                                      (gtable.lat < float(maxLat))

                    # Add bbox filter to query
                    if query is None:
                        query = bbox_filter
                    else:
                        # Merge with the previous BBOX
                        query = query & bbox_filter

        return query, joins

    # -------------------------------------------------------------------------
    def serialize_url(self):
        """
            Serialize this filter as URL query

            Returns:
                a Storage of URL GET variables
        """

        resource = self.resource
        url_vars = Storage()
        for f in self.filters:
            sub = f.serialize_url(resource=resource)
            url_vars.update(sub)
        return url_vars

# END =========================================================================
