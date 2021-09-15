# -*- coding: utf-8 -*-

"""
    Data Card List

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

__all__ = ("S3DataList",
           "S3DataListLayout",
           )

from itertools import islice

from gluon import current, A, DIV, INPUT, LABEL, SPAN, TAG

# =============================================================================
class S3DataList(object):
    """
        Class representing a list of data cards
        - client-side implementation in static/scripts/S3/s3.dataLists.js
    """

    # -------------------------------------------------------------------------
    # Standard API
    # -------------------------------------------------------------------------
    def __init__(self,
                 resource,
                 list_fields,
                 records,
                 start = None,
                 limit = None,
                 total = None,
                 list_id = None,
                 layout = None,
                 row_layout = None,
                 ):
        """
            Constructor

            @param resource: the S3Resource
            @param list_fields: the list fields
                                (list of field selector strings)
            @param records: the records
            @param start: index of the first item
            @param limit: maximum number of items
            @param total: total number of available items
            @param list_id: the HTML ID for this list
            @param layout: item renderer (optional) as function
                           (list_id, item_id, resource, rfields, record)
            @param row_layout: row renderer (optional) as
                               function(list_id, resource, rowsize, items)
        """

        self.resource = resource
        self.list_fields = list_fields
        self.records = records

        if list_id is None:
            self.list_id = "datalist"
        else:
            self.list_id = list_id

        if layout is not None:
            self.layout = layout
        else:
            self.layout = S3DataListLayout()
        self.row_layout = row_layout

        self.start = start if start else 0
        self.limit = limit if limit else 0
        self.total = total if total else 0

    # ---------------------------------------------------------------------
    def html(self,
             start=None,
             limit=None,
             pagesize=None,
             rowsize=None,
             ajaxurl=None,
             empty=None,
             popup_url=None,
             popup_title=None,
             ):
        """
            Render list data as HTML (nested DIVs)

            @param start: index of the first item (in this page)
            @param limit: total number of available items
            @param pagesize: maximum number of items per page
            @param rowsize: number of items per row
            @param ajaxurl: the URL to Ajax-update the datalist
            @param empty: message to display if the list is empty
            @param popup_url: the URL for the modal used for the 'more'
                              button (=> we deactivate InfiniteScroll)
            @param popup_title: the title for the modal
        """

        T = current.T
        resource = self.resource
        list_fields = self.list_fields
        rfields = resource.resolve_selectors(list_fields)[0]

        list_id = self.list_id
        render = self.layout
        render_row = self.row_layout

        if not rowsize:
            rowsize = 1

        pkey = str(resource._id)

        records = self.records
        if records is not None:

            # Call prep if present
            if hasattr(render, "prep"):
                render.prep(resource, records)

            if current.response.s3.dl_no_header:
                items = []
            else:
                items = [DIV(T("Total Records: %(numrows)s") % \
                                {"numrows": self.total},
                             _class = "dl-header",
                             _id = "%s-header" % list_id,
                             )
                         ]

            if empty is None:
                empty = resource.crud.crud_string(resource.tablename,
                                                  "msg_no_match")
            empty = DIV(empty, _class="dl-empty")
            if self.total > 0:
                empty.update(_style="display:none")
            items.append(empty)

            row_idx = int(self.start / rowsize) + 1
            for group in self.groups(records, rowsize):
                row = []
                col_idx = 0
                for record in group:

                    if pkey in record:
                        item_id = "%s-%s" % (list_id, record[pkey])
                    else:
                        # template
                        item_id = "%s-[id]" % list_id

                    item = render(list_id,
                                  item_id,
                                  resource,
                                  rfields,
                                  record)
                    if hasattr(item, "add_class"):
                        _class = "dl-item dl-%s-cols dl-col-%s" % (rowsize, col_idx)
                        item.add_class(_class)
                    row.append(item)
                    col_idx += 1

                _class = "dl-row %s" % ((row_idx % 2) and "even" or "odd")
                if render_row:
                    row = render_row(list_id,
                                     resource,
                                     rowsize,
                                     row)
                    if hasattr(row, "add_class"):
                        row.add_class(_class)
                else:
                    row = DIV(row, _class=_class)

                items.append(row)
                row_idx += 1
        else:
            # template
            raise NotImplementedError

        dl = DIV(items,
                 _class="dl",
                 _id=list_id,
                 )

        dl_data = {"startindex": start,
                   "maxitems": limit,
                   "totalitems": self.total,
                   "pagesize": pagesize,
                   "rowsize": rowsize,
                   "ajaxurl": ajaxurl,
                   }
        if popup_url:
            input_class = "dl-pagination"
            a_class = "s3_modal dl-more"
            #dl_data["popup_url"] = popup_url
            #dl_data["popup_title"] = popup_title
        else:
            input_class = "dl-pagination dl-scroll"
            a_class = "dl-more"
        from gluon.serializers import json as jsons
        dl_data = jsons(dl_data)
        dl.append(DIV(INPUT(_type = "hidden",
                            _class = input_class,
                            _value = dl_data,
                            ),
                      A(T("more..."),
                        _href = popup_url or ajaxurl,
                        _class = a_class,
                        _title = popup_title,
                        ),
                      _class = "dl-navigation",
                      ))

        return dl

    # ---------------------------------------------------------------------
    @staticmethod
    def groups(iterable, length):
        """
            Iterator to group data list items into rows

            @param iterable: the items iterable
            @param length: the number of items per row
        """

        iterable = iter(iterable)
        group = list(islice(iterable, length))
        while group:
            yield group
            group = list(islice(iterable, length))
        return

# =============================================================================
class S3DataListLayout(object):
    """ DataList default layout """

    item_class = "thumbnail"

    # ---------------------------------------------------------------------
    def __init__(self, profile=None):
        """
            Constructor

            @param profile: table name of the master resource of the
                            profile page (if used for a profile), can be
                            used in popup URLs to indicate the master
                            resource
        """

        self.profile = profile

    # ---------------------------------------------------------------------
    def __call__(self, list_id, item_id, resource, rfields, record):
        """
            Wrapper for render_item.

            @param list_id: the HTML ID of the list
            @param item_id: the HTML ID of the item
            @param resource: the S3Resource to render
            @param rfields: the S3ResourceFields to render
            @param record: the record as dict
        """

        # Render the item
        item = DIV(_id=item_id, _class=self.item_class)

        header = self.render_header(list_id,
                                    item_id,
                                    resource,
                                    rfields,
                                    record)
        if header is not None:
            item.append(header)

        body = self.render_body(list_id,
                                item_id,
                                resource,
                                rfields,
                                record)
        if body is not None:
            item.append(body)

        return item

    # ---------------------------------------------------------------------
    def render_header(self, list_id, item_id, resource, rfields, record):
        """
            @todo: Render the card header

            @param list_id: the HTML ID of the list
            @param item_id: the HTML ID of the item
            @param resource: the S3Resource to render
            @param rfields: the S3ResourceFields to render
            @param record: the record as dict
        """

        #DIV(
            #ICON("icon"),
            #SPAN(" %s" % title, _class="card-title"),
            #toolbox,
            #_class="card-header",
        #),
        return None

    # ---------------------------------------------------------------------
    def render_body(self, list_id, item_id, resource, rfields, record):
        """
            Render the card body

            @param list_id: the HTML ID of the list
            @param item_id: the HTML ID of the item
            @param resource: the S3Resource to render
            @param rfields: the S3ResourceFields to render
            @param record: the record as dict
        """

        pkey = str(resource._id)
        body = DIV(_class="media-body")

        render_column = self.render_column
        for rfield in rfields:

            if not rfield.show or rfield.colname == pkey:
                continue

            column = render_column(item_id, rfield, record)
            if column is not None:
                table_class = "dl-table-%s" % rfield.tname
                field_class = "dl-field-%s" % rfield.fname
                body.append(DIV(column,
                                _class = "dl-field %s %s" % (table_class,
                                                             field_class)))

        return DIV(body, _class="media")

    # ---------------------------------------------------------------------
    def render_icon(self, list_id, resource):
        """
            @todo: Render a body icon

            @param list_id: the HTML ID of the list
            @param resource: the S3Resource to render
        """

        return None

    # ---------------------------------------------------------------------
    def render_toolbox(self, list_id, resource, record):
        """
            @todo: Render the toolbox

            @param list_id: the HTML ID of the list
            @param resource: the S3Resource to render
            @param record: the record as dict
        """

        return None

    # ---------------------------------------------------------------------
    def render_column(self, item_id, rfield, record):
        """
            Render a data column.

            @param item_id: the HTML element ID of the item
            @param rfield: the S3ResourceField for the column
            @param record: the record (from S3Resource.select)
        """

        colname = rfield.colname
        if colname not in record:
            return None

        value = record[colname]
        value_id = "%s-%s" % (item_id, rfield.colname.replace(".", "_"))

        label = LABEL("%s:" % rfield.label,
                      _for = value_id,
                      _class = "dl-field-label")

        value = SPAN(value,
                     _id = value_id,
                     _class = "dl-field-value")

        return TAG[""](label, value)

# END =========================================================================
