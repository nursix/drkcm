"""
    Hierarchy Toolkit

    Copyright: 2013-2021 (c) Sahana Software Foundation

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

__all__ = ("S3HierarchyCRUD",
           )

import json

from gluon import current, DIV, FORM

from ..tools import JSONSEPARATORS, S3Hierarchy, get_crud_string

from .base import CRUDMethod

# =============================================================================
class S3HierarchyCRUD(CRUDMethod):
    """ Method handler for hierarchical CRUD """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Entry point for REST interface

            Args:
                r: the CRUDRequest
                attr: controller attributes
        """

        if r.http == "GET":
            if r.representation == "html":
                output = self.tree(r, **attr)
            elif r.representation == "json" and "node" in r.get_vars:
                output = self.node_json(r, **attr)
            elif r.representation == "xls":
                output = self.export_xls(r, **attr)
            else:
                r.error(415, current.ERROR.BAD_FORMAT)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def tree(self, r, **attr):
        """
            Page load

            Args:
                r: the CRUDRequest
                attr: controller attributes
        """

        output = {}

        resource = self.resource
        tablename = resource.tablename

        # Widget ID
        widget_id = "%s-hierarchy" % tablename

        # Render the tree
        try:
            tree = self.render_tree(widget_id, record=r.record)
        except SyntaxError:
            r.error(405, "No hierarchy configured for %s" % tablename)

        # Page title
        if r.record:
            title = get_crud_string(tablename, "title_display")
        else:
            title = get_crud_string(tablename, "title_list")
        output["title"] = title

        # Build the form
        form = FORM(DIV(tree,
                        _class="s3-hierarchy-tree",
                        ),
                    _id = widget_id,
                    )
        output["form"] = form

        # Widget options and scripts
        T = current.T
        crud_string = lambda name: get_crud_string(tablename, name)

        widget_opts = {
            "widgetID": widget_id,

            "openLabel": str(T("Open")),
            "openURL": r.url(method="read", id="[id]"),
            "ajaxURL": r.url(id=None, representation="json"),

            "editLabel": str(T("Edit")),
            "editTitle": str(crud_string("title_update")),

            "addLabel": str(T("Add")),
            "addTitle": str(crud_string("label_create")),

            "deleteLabel": str(T("Delete")),
            "deleteRoot": False if r.record else True
        }

        # Check permissions and add CRUD URLs
        resource_config = resource.get_config
        has_permission = current.auth.s3_has_permission
        if resource_config("editable", True) and \
           has_permission("update", tablename):
            widget_opts["editURL"] = r.url(method = "update",
                                           id = "[id]",
                                           representation = "popup",
                                           )

        if resource_config("deletable", True) and \
           has_permission("delete", tablename):
            widget_opts["deleteURL"] = r.url(method = "delete",
                                             id = "[id]",
                                             representation = "json",
                                             )

        if resource_config("insertable", True) and \
           has_permission("create", tablename):
            widget_opts["addURL"] = r.url(method = "create",
                                          representation = "popup",
                                          )

        # Theme options
        theme = current.deployment_settings.get_ui_hierarchy_theme()
        icons = theme.get("icons", False)
        if icons:
            # Only include non-default options
            widget_opts["icons"] = icons
        stripes = theme.get("stripes", True)
        if not stripes:
            # Only include non-default options
            widget_opts["stripes"] = stripes
        self.include_scripts(widget_id, widget_opts)

        # View
        current.response.view = self._view(r, "hierarchy.html")

        return output

    # -------------------------------------------------------------------------
    def node_json(self, r, **attr):
        """
            Return a single node as JSON (id, parent and label)

            Args:
                r: the CRUDRequest
                attr: controller attributes
        """

        resource = self.resource
        tablename = resource.tablename

        h = S3Hierarchy(tablename = tablename)
        if not h.config:
            r.error(405, "No hierarchy configured for %s" % tablename)

        data = {}
        node_id = r.get_vars["node"]
        if node_id:
            try:
                node_id = int(node_id)
            except ValueError:
                pass
            else:
                data["node"] = node_id
                label = h.label(node_id)
                data["label"] = label if label else None
                data["parent"] = h.parent(node_id)

                children = h.children(node_id)
                if children:
                    nodes = []
                    h._represent(node_ids=children)
                    for child_id in children:
                        label = h.label(child_id)
                        # @todo: include CRUD permissions?
                        nodes.append({"node": child_id,
                                      "label": label if label else None,
                                      })
                    data["children"] = nodes

        current.response.headers["Content-Type"] = "application/json"
        return json.dumps(data, separators = JSONSEPARATORS)

    # -------------------------------------------------------------------------
    def render_tree(self, widget_id, record=None):
        """
            Render the tree

            Args:
                widget_id: the widget ID
                record: the root record (if requested)
        """

        resource = self.resource
        tablename = resource.tablename

        h = S3Hierarchy(tablename = tablename)
        if not h.config:
            raise SyntaxError()

        root = None
        if record:
            try:
                root = record[h.pkey]
            except AttributeError as e:
                # Hierarchy misconfigured? Or has r.record been tampered with?
                msg = "S3Hierarchy: key %s not found in record" % h.pkey
                e.args = tuple([msg] + list(e.args[1:]))
                raise

        # @todo: apply all resource filters?
        return h.html("%s-tree" % widget_id, root=root)

    # -------------------------------------------------------------------------
    @staticmethod
    def include_scripts(widget_id, widget_opts):
        """ Include JS & CSS needed for hierarchical CRUD """

        s3 = current.response.s3
        scripts = s3.scripts
        theme = current.deployment_settings.get_ui_hierarchy_theme()

        # Include static scripts & stylesheets
        script_dir = "/%s/static/scripts" % current.request.application
        if s3.debug:
            script = "%s/jstree.js" % script_dir
            if script not in scripts:
                scripts.append(script)
            script = "%s/S3/s3.ui.hierarchicalcrud.js" % script_dir
            if script not in scripts:
                scripts.append(script)
            style = "%s/jstree.css" % theme.get("css", "plugins")
            if style not in s3.stylesheets:
                s3.stylesheets.append(style)
        else:
            script = "%s/S3/s3.jstree.min.js" % script_dir
            if script not in scripts:
                scripts.append(script)
            style = "%s/jstree.min.css" % theme.get("css", "plugins")
            if style not in s3.stylesheets:
                s3.stylesheets.append(style)

        # Apply the widget JS
        script = '''$('#%(widget_id)s').hierarchicalcrud(%(widget_opts)s)''' % \
                 {"widget_id": widget_id,
                  "widget_opts": json.dumps(widget_opts, separators=JSONSEPARATORS),
                  }
        s3.jquery_ready.append(script)

        return

    # -------------------------------------------------------------------------
    def export_xls(self, r, **attr):
        """
            Export nodes in the hierarchy in XLS format, including
            ancestor references.

            This is controlled by the hierarchy_export setting, which is
            a dict like:
            {
                "field": "name",        - the field name for the ancestor reference
                "root": "Organisation"  - the label for the root level
                "branch": "Branch"      - the label for the branch levels
                "prefix": "Sub"         - the prefix for the branch label
            }

            With this configuration, the ancestor columns would appear like:

            Organisation, Branch, SubBranch, SubSubBranch, SubSubSubBranch,...

            All parts of the setting can be omitted, the defaults are as follows:
            - "field" defaults to "name"
            - "root" is automatically generated from the resource name
            - "branch" defaults to prefix+root
            - "prefix" defaults to "Sub"

            @status: experimental
        """

        resource = self.resource
        tablename = resource.tablename

        # Get the hierarchy
        h = S3Hierarchy(tablename=tablename)
        if not h.config:
            r.error(405, "No hierarchy configured for %s" % tablename)

        # Intepret the hierarchy_export setting for the resource
        setting = resource.get_config("hierarchy_export", {})
        field = setting.get("field")
        if not field:
            field = "name" if "name" in resource.fields else resource._id.name
        prefix = setting.get("prefix", "Sub")
        root = setting.get("root")
        if not root:
            root = "".join(s.capitalize() for s in resource.name.split("_"))
        branch = setting.get("branch")
        if not branch:
            branch = "%s%s" % (prefix, root)

        rfield = resource.resolve_selector(field)

        # Get the list fields
        list_fields = resource.list_fields("export_fields", id_column=False)
        rfields = resource.resolve_selectors(list_fields,
                                             extra_fields=False,
                                             )[0]

        # Selectors = the fields to extract
        selectors = [h.pkey.name, rfield.selector]

        # Columns = the keys for the XLS Codec to access the rows
        # Types = the data types of the columns (in same order!)
        columns = []
        types = []

        # Generate the headers and type list for XLS Codec
        headers = {}
        for rf in rfields:
            selectors.append(rf.selector)
            if rf.colname == rfield.colname:
                continue
            columns.append(rf.colname)
            headers[rf.colname] = rf.label
            if rf.ftype == "virtual":
                types.append("string")
            else:
                types.append(rf.ftype)

        # Get the root nodes
        if self.record_id:
            if r.component and h.pkey.name != resource._id.name:
                query = resource.table._id == self.record_id
                row = current.db(query).select(h.pkey, limitby=(0, 1)).first()
                if not row:
                    r.error(404, current.ERROR.BAD_RECORD)
                roots = {row[h.pkey]}
            else:
                roots = {self.record_id}
        else:
            roots = h.roots

        # Find all child nodes
        all_nodes = h.findall(roots, inclusive=True)

        # ...and extract their data from a clone of the resource
        from ..resource import FS
        query = FS(h.pkey.name).belongs(all_nodes)
        clone = current.s3db.resource(resource, filter=query)
        data = clone.select(selectors, represent=True, raw_data=True)

        # Convert into dict {hierarchy key: row}
        hkey = str(h.pkey)
        data_dict = dict((row._row[hkey], row) for row in data.rows)

        # Add hierarchy headers and types
        depth = max(h.depth(node_id) for node_id in roots)
        htypes = []
        hcolumns = []
        colprefix = "HIERARCHY"
        htype = "string" if rfield.ftype == "virtual" else rfield.ftype
        for level in range(depth+1):
            col = "%s.%s" % (colprefix, level)
            if level == 0:
                headers[col] = root
            elif level == 1:
                headers[col] = branch
            else:
                headers[col] = "%s%s" % ("".join([prefix] * (level -1)), branch)
            hcolumns.append(col)
            htypes.append(htype)


        # Generate the output for XLS Codec
        output = [headers, htypes + types]
        for node_id in roots:
            rows = h.export_node(node_id,
                                 prefix = colprefix,
                                 depth=depth,
                                 hcol=rfield.colname,
                                 columns = columns,
                                 data = data_dict,
                                 )
            output.extend(rows)

        # Encode in XLS format
        from ..resource import S3Codec
        codec = S3Codec.get_codec("xls")
        result = codec.encode(output,
                              title = resource.name,
                              list_fields = hcolumns+columns,
                              )

        # Reponse headers and file name are set in codec
        return result

# END =========================================================================
