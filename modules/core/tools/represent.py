# -*- coding: utf-8 -*-

""" S3 Extensions for gluon.dal.Field, reusable fields

    @requires: U{B{I{gluon}} <http://web2py.com>}

    @copyright: 2009-2021 (c) Sahana Software Foundation
    @license: MIT

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

__all__ = ("S3Represent",
           "S3RepresentLazy",
           )

import sys
from itertools import chain

from gluon import current, A, TAG, URL
from gluon.storage import Storage
from gluon.languages import lazyT

from .utils import s3_str, S3MarkupStripper

# =============================================================================
class S3Represent(object):
    """
        Scalable universal field representation for option fields and
        foreign keys. Can be subclassed and tailored to the particular
        model where necessary.

        @group Configuration (in the model): __init__
        @group API (to apply the method): __call__,
                                          multiple,
                                          bulk,
                                          render_list
        @group Prototypes (to adapt in subclasses): lookup_rows,
                                                    represent_row,
                                                    link
        @group Internal Methods: _setup,
                                 _lookup
    """

    def __init__(self,
                 lookup = None,
                 key = None,
                 fields = None,
                 labels = None,
                 options = None,
                 translate = False,
                 linkto = None,
                 show_link = False,
                 multiple = False,
                 hierarchy = False,
                 default = None,
                 none = None,
                 field_sep = " "
                 ):
        """
            Constructor

            @param lookup: the name of the lookup table
            @param key: the field name of the primary key of the lookup table,
                        a field name
            @param fields: the fields to extract from the lookup table, a list
                           of field names
            @param labels: string template or callable to represent rows from
                           the lookup table, callables must return a string
            @param options: dictionary of options to lookup the representation
                            of a value, overrides lookup and key
            @param multiple: web2py list-type (all values will be lists)
            @param hierarchy: render a hierarchical representation, either
                              True or a string template like "%s > %s"
            @param translate: translate all representations (using T)
            @param linkto: a URL (as string) to link representations to,
                           with "[id]" as placeholder for the key
            @param show_link: whether to add a URL to representations
            @param default: default representation for unknown options
            @param none: representation for empty fields (None or empty list)
            @param field_sep: separator to use to join fields
        """

        self.tablename = lookup
        self.table = None
        self.key = key
        self.fields = fields
        self.labels = labels
        self.options = options
        self.list_type = multiple
        self.hierarchy = hierarchy
        self.translate = translate
        self.linkto = linkto
        self.show_link = show_link
        self.default = default
        self.none = none
        self.field_sep = field_sep
        self.setup = False
        self.theset = None
        self.queries = 0
        self.lazy = []
        self.lazy_show_link = False

        self.rows = {}

        self.clabels = None
        self.slabels = None
        self.htemplate = None

        # Attributes to simulate being a function for sqlhtml's count_expected_args()
        # Make sure we indicate only 1 position argument
        self.__code__ = Storage(co_argcount = 1)
        self.__defaults__ = None

        # Detect lookup_rows override
        self.custom_lookup = self.lookup_rows.__func__ is not S3Represent.lookup_rows

    # -------------------------------------------------------------------------
    def lookup_rows(self, key, values, fields=None):
        """
            Lookup all rows referenced by values.
            (in foreign key representations)

            @param key: the key Field
            @param values: the values
            @param fields: the fields to retrieve
        """

        if fields is None:
            fields = []
        fields.append(key)

        if len(values) == 1:
            query = (key == values[0])
        else:
            query = key.belongs(values)
        rows = current.db(query).select(*fields)
        self.queries += 1
        return rows

    # -------------------------------------------------------------------------
    def represent_row(self, row, prefix=None):
        """
            Represent the referenced row.
            (in foreign key representations)

            @param row: the row
            @param prefix: prefix for hierarchical representation

            @return: the representation of the Row, or None if there
                     is an error in the Row
        """

        labels = self.labels

        translated = False

        if self.slabels:
            # String Template or lazyT
            try:
                row_dict = row.as_dict()
            except AttributeError:
                # Row just a dict/Storage after all? (e.g. custom lookup)
                row_dict = row

            # Represent None as self.none
            none = self.none
            for k, v in list(row_dict.items()):
                if v is None:
                    row_dict[k] = none

            v = labels % row_dict

        elif self.clabels:
            # External Renderer
            v = labels(row)

        else:
            # Default
            values = [row[f] for f in self.fields if row[f] not in (None, "")]

            if len(values) > 1:
                # Multiple values => concatenate with separator
                if self.translate:
                    # Translate items individually before concatenating
                    T = current.T
                    values = [T(v) if not type(v) is lazyT else v for v in values]
                    translated = True
                sep = self.field_sep
                v = sep.join(s3_str(value) for value in values)
            elif values:
                v = s3_str(values[0])
            else:
                v = self.none

        if not translated and self.translate and not type(v) is lazyT:
            output = current.T(v)
        else:
            output = v

        if prefix and self.hierarchy:
            return self.htemplate % (prefix, output)

        return output

    # -------------------------------------------------------------------------
    def link(self, k, v, row=None):
        """
            Represent a (key, value) as hypertext link.

                - Typically, k is a foreign key value, and v the
                  representation of the referenced record, and the link
                  shall open a read view of the referenced record.

                - In the base class, the linkto-parameter expects a URL (as
                  string) with "[id]" as placeholder for the key.

            @param k: the key
            @param v: the representation of the key
            @param row: the row with this key (unused in the base class)
        """

        if self.linkto:
            k = s3_str(k)
            return A(v, _href=self.linkto.replace("[id]", k) \
                                         .replace("%5Bid%5D", k))
        else:
            return v

    # -------------------------------------------------------------------------
    def __call__(self, value, row=None, show_link=True):
        """
            Represent a single value (standard entry point).

            @param value: the value
            @param row: the referenced row (if value is a foreign key)
            @param show_link: render the representation as link
        """

        self._setup()
        show_link = show_link and self.show_link

        if self.list_type:
            # Is a list-type => use multiple
            return self.multiple(value,
                                 rows=row,
                                 list_type=False,
                                 show_link=show_link)

        # Prefer the row over the value
        if row and self.table:
            value = row[self.key]

        # Lookup the representation
        if value:
            rows = [row] if row is not None else None
            items = self._lookup([value], rows=rows)
            if value in items:
                k, v = value, items[value]
                r = self.link(k, v, row=self.rows.get(k)) \
                    if show_link else items[value]
            else:
                r = self.default
            return r
        return self.none

    # -------------------------------------------------------------------------
    def multiple(self, values, rows=None, list_type=True, show_link=True):
        """
            Represent multiple values as a comma-separated list.

            @param values: list of values
            @param rows: the referenced rows (if values are foreign keys)
            @param show_link: render each representation as link
        """

        self._setup()
        show_link = show_link and self.show_link

        # Get the values
        if rows and self.table:
            key = self.key
            values = [row[key] for row in rows]
        elif self.list_type and list_type:
            try:
                hasnone = None in values
                if hasnone:
                    values = [i for i in values if i != None]
                values = list(set(chain.from_iterable(values)))
                if hasnone:
                    values.append(None)
            except TypeError:
                raise ValueError("List of lists expected, got %s" % values)
        else:
            values = [values] if type(values) is not list else values

        # Lookup the representations
        if values:
            default = self.default
            items = self._lookup(values, rows=rows)
            if show_link:
                link = self.link
                rows = self.rows
                labels = [[link(k, s3_str(items[k]), row=rows.get(k)), ", "]
                          if k in items else [default, ", "]
                          for k in values]
                if labels:
                    return TAG[""](list(chain.from_iterable(labels))[:-1])
                else:
                    return ""
            else:
                labels = [s3_str(items[k])
                          if k in items else default for k in values]
                if labels:
                    return ", ".join(labels)
        return self.none

    # -------------------------------------------------------------------------
    def bulk(self, values, rows=None, list_type=True, show_link=True):
        """
            Represent multiple values as dict {value: representation}

            @param values: list of values
            @param rows: the rows
            @param show_link: render each representation as link

            @return: a dict {value: representation}

            @note: for list-types, the dict keys will be the individual
                   values within all lists - and not the lists (simply
                   because lists can not be dict keys). Thus, the caller
                   would still have to construct the final string/HTML.
        """

        self._setup()
        show_link = show_link and self.show_link

        # Get the values
        if rows and self.table:
            key = self.key
            _rows = self.rows
            values = set()
            add_value = values.add
            for row in rows:
                value = row[key]
                _rows[value] = row
                add_value(value)
            values = list(values)
        elif self.list_type and list_type:
            try:
                hasnone = None in values
                if hasnone:
                    values = [i for i in values if i != None]
                values = list(set(chain.from_iterable(values)))
                if hasnone:
                    values.append(None)
            except TypeError:
                raise ValueError("List of lists expected, got %s" % values)
        else:
            values = [values] if type(values) is not list else values

        # Lookup the representations
        if values:
            labels = self._lookup(values, rows=rows)
            if show_link:
                link = self.link
                rows = self.rows
                labels = {k: link(k, v, rows.get(k)) for k, v in labels.items()}
            for k in values:
                if k not in labels:
                    labels[k] = self.default
        else:
            labels = {}
        labels[None] = self.none
        return labels

    # -------------------------------------------------------------------------
    def render_list(self, value, labels, show_link=True):
        """
            Helper method to render list-type representations from
            bulk()-results.

            @param value: the list
            @param labels: the labels as returned from bulk()
            @param show_link: render references as links, should
                              be the same as used with bulk()
        """

        show_link = show_link and self.show_link
        if show_link:
            labels = [(labels[v], ", ")
                      if v in labels else (self.default, ", ")
                      for v in value]
            if labels:
                return TAG[""](list(chain.from_iterable(labels))[:-1])
            else:
                return ""
        else:
            return ", ".join([s3_str(labels[v])
                              if v in labels else self.default
                              for v in value])

    # -------------------------------------------------------------------------
    def _setup(self):
        """ Lazy initialization of defaults """

        if self.setup:
            return

        self.queries = 0

        # Default representations
        messages = current.messages
        if self.default is None:
            self.default = s3_str(messages.UNKNOWN_OPT)
        if self.none is None:
            self.none = messages["NONE"]

        # Initialize theset
        if self.options is not None:
            if self.translate:
                T = current.T
                self.theset = {opt: T(label) if isinstance(label, str) else label
                               for opt, label in self.options.items()}
            else:
                self.theset = self.options
        else:
            self.theset = {}

        # Lookup table parameters and linkto
        if self.table is None:
            tablename = self.tablename
            if tablename:
                table = current.s3db.table(tablename)
                if table is not None:
                    if self.key is None:
                        self.key = table._id.name
                    if not self.fields:
                        if "name" in table:
                            self.fields = ["name"]
                        else:
                            self.fields = [self.key]
                    self.table = table
                if self.linkto is None and self.show_link:
                    c, f = tablename.split("_", 1)
                    self.linkto = URL(c=c, f=f, args=["[id]"], extension="")

        # What type of renderer do we use?
        labels = self.labels
        # String template?
        self.slabels = isinstance(labels, (str, lazyT))
        # External renderer?
        self.clabels = callable(labels)

        # Hierarchy template
        if isinstance(self.hierarchy, str):
            self.htemplate = self.hierarchy
        else:
            self.htemplate = "%s > %s"

        self.setup = True

    # -------------------------------------------------------------------------
    def _lookup(self, values, rows=None):
        """
            Lazy lookup values.

            @param values: list of values to lookup
            @param rows: rows referenced by values (if values are foreign keys)
                         optional
        """

        theset = self.theset

        keys = {}
        items = {}
        lookup = {}

        # Check whether values are already in theset
        table = self.table
        for _v in values:
            v = _v
            if v is not None and table and isinstance(v, str):
                try:
                    v = int(_v)
                except ValueError:
                    pass
            keys[v] = _v
            if v is None:
                items[_v] = self.none
            elif v in theset:
                items[_v] = theset[v]
            else:
                lookup[v] = True

        if table is None or not lookup:
            return items

        if table and self.hierarchy:
            # Does the lookup table have a hierarchy?
            from ..tools import S3Hierarchy
            h = S3Hierarchy(table._tablename)
            if h.config:
                def lookup_parent(node_id):
                    parent = h.parent(node_id)
                    if parent and \
                       parent not in theset and \
                       parent not in lookup:
                        lookup[parent] = False
                        lookup_parent(parent)
                    return
                for node_id in list(lookup.keys()):
                    lookup_parent(node_id)
            else:
                h = None
        else:
            h = None

        # Get the primary key
        pkey = self.key
        ogetattr = object.__getattribute__
        try:
            key = ogetattr(table, pkey)
        except AttributeError:
            return items

        # Use the given rows to lookup the values
        pop = lookup.pop
        represent_row = self.represent_row
        represent_path = self._represent_path
        if rows and not self.custom_lookup:
            rows_ = dict((row[key], row) for row in rows)
            self.rows.update(rows_)
            for row in rows:
                k = row[key]
                if k not in theset:
                    if h:
                        theset[k] = represent_path(k,
                                                   row,
                                                   rows = rows_,
                                                   hierarchy = h,
                                                   )
                    else:
                        theset[k] = represent_row(row)
                if pop(k, None):
                    items[keys.get(k, k)] = theset[k]

        # Retrieve additional rows as needed
        if lookup:
            if not self.custom_lookup:
                try:
                    # Need for speed: assume all fields are in table
                    fields = [ogetattr(table, f) for f in self.fields]
                except AttributeError:
                    # Ok - they are not: provide debug output and filter fields
                    current.log.error(sys.exc_info()[1])
                    fields = [ogetattr(table, f)
                              for f in self.fields if hasattr(table, f)]
            else:
                fields = []
            rows = self.lookup_rows(key, list(lookup.keys()), fields=fields)
            rows = {row[key]: row for row in rows}
            self.rows.update(rows)
            if h:
                for k, row in rows.items():
                    if lookup.pop(k, None):
                        items[keys.get(k, k)] = represent_path(k,
                                                               row,
                                                               rows = rows,
                                                               hierarchy = h,
                                                               )
            else:
                for k, row in rows.items():
                    lookup.pop(k, None)
                    items[keys.get(k, k)] = theset[k] = represent_row(row)

        # Anything left gets set to default
        if lookup:
            for k in lookup:
                items[keys.get(k, k)] = self.default

        return items

    # -------------------------------------------------------------------------
    def _represent_path(self, value, row, rows=None, hierarchy=None):
        """
            Recursive helper method to represent value as path in
            a hierarchy.

            @param value: the value
            @param row: the row containing the value
            @param rows: all rows from _loopup as dict
            @param hierarchy: the S3Hierarchy instance
        """

        theset = self.theset

        if value in theset:
            return theset[value]

        prefix = None
        parent = hierarchy.parent(value)

        if parent:
            if parent in theset:
                prefix = theset[parent]
            elif parent in rows:
                prefix = self._represent_path(parent,
                                              rows[parent],
                                              rows=rows,
                                              hierarchy=hierarchy)

        result = self.represent_row(row, prefix=prefix)
        theset[value] = result
        return result

# =============================================================================
class S3RepresentLazy(object):
    """
        Lazy Representation of a field value, utilizes the bulk-feature
        of S3Represent-style representation methods
    """

    def __init__(self, value, renderer):
        """
            Constructor

            @param value: the value
            @param renderer: the renderer (S3Represent instance)
        """

        self.value = value
        self.renderer = renderer

        self.multiple = False
        renderer.lazy.append(value)

    # -------------------------------------------------------------------------
    def __repr__(self):

        return s3_str(self.represent())

    # -------------------------------------------------------------------------
    def represent(self):
        """ Represent as string """

        value = self.value
        renderer = self.renderer
        if renderer.lazy:
            labels = renderer.bulk(renderer.lazy, show_link=False)
            renderer.lazy = []
        else:
            labels = renderer.theset
        if renderer.list_type:
            if self.multiple:
                return renderer.multiple(value, show_link=False)
            else:
                return renderer.render_list(value, labels, show_link=False)
        else:
            if self.multiple:
                return renderer.multiple(value, show_link=False)
            else:
                return renderer(value, show_link=False)

    # -------------------------------------------------------------------------
    def render(self):
        """ Render as HTML """

        value = self.value
        renderer = self.renderer
        if renderer.lazy:
            labels = renderer.bulk(renderer.lazy)
            renderer.lazy = []
        else:
            labels = renderer.theset
        if renderer.list_type:
            if not value:
                value = []
            if self.multiple:
                if len(value) and type(value[0]) is not list:
                    value = [value]
                return renderer.multiple(value)
            else:
                return renderer.render_list(value, labels)
        else:
            if self.multiple:
                return renderer.multiple(value)
            else:
                return renderer(value)

    # -------------------------------------------------------------------------
    def render_node(self, element, attributes, name):
        """
            Render as text or attribute of an XML element

            @param element: the element
            @param attributes: the attributes dict of the element
            @param name: the attribute name
        """

        # Render value
        text = s3_str(self.represent())

        # Strip markup + XML-escape
        if text and "<" in text:
            try:
                stripper = S3MarkupStripper()
                stripper.feed(text)
                text = stripper.stripped()
            except:
                pass

        # Add to node
        if text is not None:
            if element is not None:
                element.text = text
            else:
                attributes[name] = text
            return

# END =========================================================================
