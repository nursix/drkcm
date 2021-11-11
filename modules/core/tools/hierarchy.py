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

__all__ = ("S3Hierarchy",
           )

from gluon import current, LI, UL
from gluon.storage import Storage
from gluon.tools import callback

from .represent import S3Represent
from .convert import s3_str

DEFAULT = lambda: None

# =============================================================================
class S3Hierarchy:
    """ Class representing an object hierarchy """

    # -------------------------------------------------------------------------
    def __init__(self,
                 tablename = None,
                 hierarchy = None,
                 represent = None,
                 filter = None,
                 leafonly = True,
                 ):
        """
            Args:
                tablename: the tablename
                hierarchy: the hierarchy setting for the table
                           (replaces the current setting)
                represent: a representation method for the node IDs
                filter: additional filter query for the table to
                        select the relevant subset
                leafonly: filter strictly for leaf nodes
        """

        self.tablename = tablename
        if hierarchy:
            current.s3db.configure(tablename, hierarchy=hierarchy)
        self.represent = represent

        self.filter = filter
        self.leafonly = leafonly

        self.__theset = None
        self.__flags = None

        self.__nodes = None
        self.__roots = None

        self.__pkey = None
        self.__fkey = None
        self.__ckey = None

        self.__link = DEFAULT
        self.__lkey = DEFAULT
        self.__left = DEFAULT

    # -------------------------------------------------------------------------
    @property
    def theset(self):
        """
            The raw nodes dict like:

                {<node_id>: {"p": <parent_id>,
                             "c": <category>,
                             "s": set(child nodes)
                }}
        """

        if self.__theset is None:
            self.__connect()
        if self.__status("dirty"):
            self.read()
        return self.__theset

    # -------------------------------------------------------------------------
    @property
    def flags(self):
        """ Dict of status flags """

        if self.__flags is None:
            theset = self.theset
        return self.__flags

    # -------------------------------------------------------------------------
    @property
    def config(self):
        """ Hierarchy configuration of the target table """

        tablename = self.tablename
        if tablename:
            s3db = current.s3db
            if tablename in current.db or \
               s3db.table(tablename, db_only=True):
                return s3db.get_config(tablename, "hierarchy")
        return None

    # -------------------------------------------------------------------------
    @property
    def nodes(self):
        """ The nodes in the subset """

        theset = self.theset
        if self.__nodes is None:
            self.__subset()
        return self.__nodes

    # -------------------------------------------------------------------------
    @property
    def roots(self):
        """ The IDs of the root nodes in the subset """

        nodes = self.nodes
        return self.__roots

    # -------------------------------------------------------------------------
    @property
    def pkey(self):
        """ The parent key """

        if self.__pkey is None:
            self.__keys()
        return self.__pkey

    # -------------------------------------------------------------------------
    @property
    def fkey(self):
        """ The foreign key referencing the parent key """

        if self.__fkey is None:
            self.__keys()
        return self.__fkey

    # -------------------------------------------------------------------------
    @property
    def link(self):
        """
            The name of the link table containing the foreign key, or
            None if the foreign key is in the hierarchical table itself
        """

        if self.__link is DEFAULT:
            self.__keys()
        return self.__link

    # -------------------------------------------------------------------------
    @property
    def lkey(self):
        """ The key in the link table referencing the child """

        if self.__lkey is DEFAULT:
            self.__keys()
        return self.__lkey

    # -------------------------------------------------------------------------
    @property
    def left(self):
        """ The left join with the link table containing the foreign key """

        if self.__left is DEFAULT:
            self.__keys()
        return self.__left

    # -------------------------------------------------------------------------
    @property
    def ckey(self):
        """ The category field """

        if self.__ckey is None:
            self.__keys()
        return self.__ckey

    # -------------------------------------------------------------------------
    def __connect(self):
        """ Connect this instance to the hierarchy """

        tablename = self.tablename
        if tablename :
            hierarchies = current.model["hierarchies"]
            if tablename in hierarchies:
                hierarchy = hierarchies[tablename]
                self.__theset = hierarchy["nodes"]
                self.__flags = hierarchy["flags"]
            else:
                self.__theset = dict()
                self.__flags = dict()
                self.load()
                hierarchy = {"nodes": self.__theset,
                             "flags": self.__flags}
                hierarchies[tablename] = hierarchy
        else:
            self.__theset = dict()
            self.__flags = dict()
        return

    # -------------------------------------------------------------------------
    def __status(self, flag=None, default=None, **attr):
        """
            Check or update status flags

            Args:
                flag: the name of the status flag to return
                default: the default value if the flag is not set
                attr: key-value pairs for flags to set

            Returns:
                the value of the requested flag, or all flags as dict if
                no flag was specified
        """

        flags = self.flags
        for k, v in attr.items():
            if v is not None:
                flags[k] = v
            else:
                flags.pop(k, None)
        if flag is not None:
            return flags.get(flag, default)
        return flags

    # -------------------------------------------------------------------------
    def load(self):
        """ Try loading the hierarchy from s3_hierarchy """

        if not self.config:
            return
        tablename = self.tablename

        if not self.__status("dbstatus", True):
            # Cancel attempt if DB is known to be dirty
            self.__status(dirty=True)
            return

        htable = current.s3db.s3_hierarchy
        query = (htable.tablename == tablename)
        row = current.db(query).select(htable.dirty,
                                       htable.hierarchy,
                                       limitby = (0, 1)
                                       ).first()
        if row and not row.dirty:
            data = row.hierarchy
            theset = self.__theset
            theset.clear()
            for node_id, item in data["nodes"].items():
                theset[int(node_id)] = {"p": item["p"],
                                        "c": item["c"],
                                        "s": set(item["s"]) \
                                             if item["s"] else set()}
            self.__status(dirty = False,
                          dbupdate = None,
                          dbstatus = True)
            return
        else:
            self.__status(dirty = True,
                          dbupdate = None,
                          dbstatus = False if row else None)
        return

    # -------------------------------------------------------------------------
    def save(self):
        """ Save this hierarchy in s3_hierarchy """

        if not self.config:
            return
        tablename = self.tablename

        theset = self.theset
        if not self.__status("dbupdate"):
            return

        # Serialize the theset
        nodes_dict = {}
        for node_id, node in theset.items():
            nodes_dict[node_id] = {"p": node["p"],
                                   "c": node["c"],
                                   "s": list(node["s"]) \
                                        if node["s"] else []}

        # Generate record
        data = {"tablename": tablename,
                "dirty": False,
                "hierarchy": {"nodes": nodes_dict}
                }

        # Get current entry
        htable = current.s3db.s3_hierarchy
        query = (htable.tablename == tablename)
        row = current.db(query).select(htable.id,
                                       limitby = (0, 1)
                                       ).first()

        if row:
            # Update record
            row.update_record(**data)
        else:
            # Create new record
            htable.insert(**data)

        # Update status
        self.__status(dirty=False, dbupdate=None, dbstatus=True)
        return

    # -------------------------------------------------------------------------
    @classmethod
    def dirty(cls, tablename):
        """
            Mark this hierarchy as dirty. To be called when the target
            table gets updated (can be called repeatedly).

            Args:
                tablename: the tablename
        """

        s3db = current.s3db

        if not tablename:
            return
        config = s3db.get_config(tablename, "hierarchy")
        if not config:
            return

        hierarchies = current.model["hierarchies"]
        if tablename in hierarchies:
            hierarchy = hierarchies[tablename]
            flags = hierarchy["flags"]
        else:
            flags = {}
            hierarchies[tablename] = {"nodes": dict(),
                                      "flags": flags}
        flags["dirty"] = True

        dbstatus = flags.get("dbstatus", True)
        if dbstatus:
            htable = current.s3db.s3_hierarchy
            query = (htable.tablename == tablename)
            row = current.db(query).select(htable.id,
                                           htable.dirty,
                                           limitby=(0, 1)).first()
            if not row:
                htable.insert(tablename=tablename, dirty=True)
            elif not row.dirty:
                row.update_record(dirty=True)
            flags["dbstatus"] = False
        return

    # -------------------------------------------------------------------------
    def read(self):
        """ Rebuild this hierarchy from the target table """

        tablename = self.tablename
        if not tablename:
            return

        s3db = current.s3db
        table = s3db[tablename]

        pkey = self.pkey
        fkey = self.fkey
        ckey = self.ckey

        fields = [pkey, fkey]
        if ckey is not None:
            fields.append(table[ckey])

        if "deleted" in table:
            query = (table.deleted == False)
        else:
            query = (table.id > 0)
        rows = current.db(query).select(left = self.left, *fields)

        self.__theset.clear()

        add = self.add
        cfield = table[ckey]
        for row in rows:
            n = row[pkey]
            p = row[fkey]
            if ckey:
                c = row[cfield]
            else:
                c = None
            add(n, parent_id=p, category=c)

        # Update status: memory is clean, db needs update
        self.__status(dirty=False, dbupdate=True)

        # Remove subset
        self.__roots = None
        self.__nodes = None

        return

    # -------------------------------------------------------------------------
    def __keys(self):
        """ Introspect the key fields in the hierarchical table """

        tablename = self.tablename
        if not tablename:
            return

        s3db = current.s3db
        table = s3db[tablename]

        config = s3db.get_config(tablename, "hierarchy")
        if not config:
            return

        if isinstance(config, tuple):
            parent, self.__ckey = config[:2]
        else:
            parent, self.__ckey = config, None

        pkey = None
        fkey = None
        if parent is None:

            # Assume self-reference
            pkey = table._id

            for field in table:
                ftype = str(field.type)
                if ftype[:9] == "reference":
                    key = ftype[10:].split(".")
                    if key[0] == tablename and \
                       (len(key) == 1 or key[1] == pkey.name):
                        parent = field.name
                        fkey = field
                        break
        else:
            resource = s3db.resource(tablename)
            master = resource.tablename

            rfield = resource.resolve_selector(parent)
            ltname = rfield.tname

            if ltname == master:
                # Self-reference
                fkey = rfield.field
                self.__link = None
                self.__lkey = None
                self.__left = None
            else:
                # Link table

                # Use the parent selector to find the link resource
                alias = parent.split(".%s" % rfield.fname)[0]
                calias = s3db.get_alias(master, alias)
                if not calias:
                    # Fall back to link table name
                    alias = ltname.split("_", 1)[1]
                    calias = s3db.get_alias(master, alias)

                # Load the component and get the link parameters
                if calias:
                    component = resource.components.get(calias)
                    link = component.link
                    if link:
                        fkey = rfield.field
                        self.__link = ltname
                        self.__lkey = link.fkey
                        self.__left = rfield.left.get(ltname)

        if not fkey:
            # No parent field found
            raise AttributeError("parent link not found")

        if pkey is None:
            ftype = str(fkey.type)
            if ftype[:9] != "reference":
                # Invalid parent field (not a foreign key)
                raise SyntaxError("Invalid parent field: "
                                  "%s is not a foreign key" % fkey)
            key = ftype[10:].split(".")
            if key[0] == tablename:
                # Self-reference
                pkey = table._id
            else:
                # Super-entity?
                ktable = s3db[key[0]]
                skey = ktable._id.name
                if skey != "id" and "instance_type" in ktable:
                    try:
                        pkey = table[skey]
                    except AttributeError:
                        raise SyntaxError("%s is not an instance type of %s" %
                                          (tablename, ktable._tablename))

        self.__pkey = pkey
        self.__fkey = fkey
        return

    # -------------------------------------------------------------------------
    def preprocess_create_node(self, r, parent_id):
        """
            Pre-process a CRUD request to create a new node

            Args:
                r: the request
                table: the hierarchical table
                parent_id: the parent ID
        """

        # Make sure the parent record exists
        table = current.s3db.table(self.tablename)
        query = (table[self.pkey.name] == parent_id)
        DELETED = current.xml.DELETED
        if DELETED in table.fields:
            query &= table[DELETED] != True
        parent = current.db(query).select(table._id).first()
        if not parent:
            raise KeyError("Parent record not found")

        link = self.link
        fkey = self.fkey
        if self.link is None:
            # Parent field in table
            fkey.default = fkey.update = parent_id
            fkey.comment = None
            if r.http == "POST":
                r.post_vars[fkey.name] = parent_id
            fkey.readable = fkey.writable = False
            link = None
        else:
            # Parent field in link table
            link = {"linktable": self.link,
                    "rkey": fkey.name,
                    "lkey": self.lkey,
                    "parent_id": parent_id,
                    }
        return link

    # -------------------------------------------------------------------------
    def postprocess_create_node(self, link, node):
        """
            Create a link table entry for a new node

            Args:
                link: the link information (as returned from
                      preprocess_create_node)
                node: the new node
        """

        try:
            node_id = node[self.pkey.name]
        except (AttributeError, KeyError):
            return

        s3db = current.s3db
        tablename = link["linktable"]
        linktable = s3db.table(tablename)
        if not linktable:
            return

        lkey = link["lkey"]
        rkey = link["rkey"]
        data = {rkey: link["parent_id"],
                lkey: node_id,
                }

        # Create the link if it does not already exist
        query = ((linktable[lkey] == data[lkey]) &
                 (linktable[rkey] == data[rkey]))
        row = current.db(query).select(linktable._id, limitby=(0, 1)).first()
        if not row:
            onaccept = s3db.get_config(tablename, "create_onaccept")
            if onaccept is None:
                onaccept = s3db.get_config(tablename, "onaccept")
            link_id = linktable.insert(**data)
            data[linktable._id.name] = link_id
            s3db.update_super(linktable, data)
            if link_id and onaccept:
                callback(onaccept, Storage(vars=Storage(data)))
        return

    # -------------------------------------------------------------------------
    def delete(self, node_ids, cascade=False):
        """
            Recursive deletion of hierarchy branches

            Args:
                node_ids: the parent node IDs of the branches to be deleted
                cascade: cascade call, do not commit (internal use)

            Returns:
                number of deleted nodes, or None if cascade failed
        """

        if not self.config:
            return None

        tablename = self.tablename

        total = 0
        for node_id in node_ids:

            # Recursively delete child nodes
            children = self.children(node_id)
            if children:
                result = self.delete(children, cascade=True)
                if result is None:
                    if not cascade:
                        current.db.rollback()
                    return None
                else:
                    total += result

            # Delete node
            from ..resource import FS
            query = (FS(self.pkey.name) == node_id)
            resource = current.s3db.resource(tablename, filter=query)
            success = resource.delete(cascade=True)
            if success:
                self.remove(node_id)
                total += 1
            else:
                if not cascade:
                    current.db.rollback()
                return None

        if not cascade and total:
            self.dirty(tablename)

        return total

    # -------------------------------------------------------------------------
    def add(self, node_id, parent_id=None, category=None):
        """
            Add a new node to the hierarchy

            Args:
                node_id: the node ID
                parent_id: the parent node ID
                category: the category
        """

        theset = self.__theset

        if node_id in theset:
            node = theset[node_id]
            if category is not None:
                node["c"] = category
        elif node_id:
            node = {"s": set(), "c": category}
        else:
            raise SyntaxError

        if parent_id:
            if parent_id not in theset:
                parent = self.add(parent_id, None, None)
            else:
                parent = theset[parent_id]
            parent["s"].add(node_id)
        node["p"] = parent_id

        theset[node_id] = node
        return node

    # -------------------------------------------------------------------------
    def remove(self, node_id):
        """
            Remove a node from the hierarchy

            Args:
                node_id: the node ID
        """

        theset = self.__theset

        if node_id in theset:
            node = theset[node_id]
        else:
            return False

        parent_id = node["p"]
        if parent_id:
            parent = theset[parent_id]
            parent["s"].discard(node_id)
        del theset[node_id]
        return True

    # -------------------------------------------------------------------------
    def __subset(self):
        """ Generate the subset of accessible nodes which match the filter """

        theset = self.theset

        roots = set()
        subset = {}

        resource = current.s3db.resource(self.tablename,
                                         filter = self.filter)

        pkey = self.pkey
        rows = resource.select([pkey.name], as_rows = True)

        if rows:
            key = str(pkey)
            if self.leafonly:
                # Select matching leaf nodes
                ids = set()
                for row in rows:
                    node_id = row[key]
                    if node_id in theset and not theset[node_id]["s"]:
                        ids.add(node_id)
            else:
                # Select all matching nodes
                ids = set(row[key] for row in rows)

            # Resolve the paths
            while ids:
                node_id = ids.pop()
                if node_id in subset:
                    continue
                node = theset.get(node_id)
                if not node:
                    continue
                parent_id = node["p"]
                if parent_id and parent_id not in subset:
                    ids.add(parent_id)
                elif not parent_id:
                    roots.add(node_id)
                subset[node_id] = dict(node)

            # Update the descendants
            for node in subset.values():
                node["s"] = set(node_id for node_id in node["s"]
                                        if node_id in subset)

        self.__roots = roots
        self.__nodes = subset
        return

    # -------------------------------------------------------------------------
    def category(self, node_id):
        """
            Get the category of a node

            Args:
                node_id: the node ID

            Returns:
                the node category
        """

        node = self.nodes.get(node_id)
        if not node:
            return None
        else:
            return node["c"]

    # -------------------------------------------------------------------------
    def parent(self, node_id, classify=False):
        """
            Get the parent node of a node

            Args:
                node_id: the node ID
                classify: return the root node as tuple (id, category)
                          instead of just id

            Returns
                the root node ID (or tuple (id, category), respectively)
        """

        nodes = self.nodes

        default = (None, None) if classify else None

        node = nodes.get(node_id)
        if not node:
            return default

        parent_id = node["p"]
        if not parent_id:
            return default

        parent_node = nodes.get(parent_id)
        if not parent_node:
            return default

        parent_category = parent_node["c"]
        return (parent_id, parent_category) if classify else parent_id

    # -------------------------------------------------------------------------
    def children(self, node_id, category=DEFAULT, classify=False):
        """
            Get child nodes of a node

            Args:
                node_id: the node ID
                category: return only children of this category
                classify: return each node as tuple (id, category) instead
                          of just ids

            Returns:
                the child nodes as Python set
        """

        nodes = self.nodes
        default = set()

        node = nodes.get(node_id)
        if not node:
            return default

        child_ids = node["s"]
        if not child_ids:
            return default

        children = set()
        for child_id in child_ids:
            child_node = nodes.get(child_id)
            if not child_node:
                continue
            child_category = child_node["c"]
            child = (child_id, child_category) if classify else child_id
            if category is DEFAULT or category == child_category:
                children.add(child)
        return children

    # -------------------------------------------------------------------------
    def path(self, node_id, category=DEFAULT, classify=False):
        """
            Return the ancestor path of a node

            Args:
                node_id: the node ID
                category: start with this category rather than with root
                classify: return each node as tuple (id, category) instead
                          of just ids

            Returns:
                the path as list, starting at the root node
        """

        nodes = self.nodes

        node = nodes.get(node_id)
        if not node:
            return []
        this = (node_id, node["c"]) if classify else node_id
        if category is not DEFAULT and node["c"] == category:
            return [this]
        parent_id = node["p"]
        if not parent_id:
            return [this]
        path = self.path(parent_id, category=category, classify=classify)
        path.append(this)
        return path

    # -------------------------------------------------------------------------
    def root(self, node_id, category=DEFAULT, classify=False):
        """
            Get the root node for a node. Returns the node if it is the
            root node itself.

            Args:
                node_id: the node ID
                category: find the closest node of this category rather
                          than the absolute root
                classify: return the root node as tuple (id, category)
                          instead of just id

            Returns:
                the root node ID (or tuple (id, category), respectively)
        """

        nodes = self.nodes
        default = (None, None) if classify else None

        node = nodes.get(node_id)
        if not node:
            return default
        this = (node_id, node["c"]) if classify else node_id
        if category is not DEFAULT and node["c"] == category:
            return this
        parent_id = node["p"]
        if not parent_id:
            return this if category is DEFAULT else default
        return self.root(parent_id, category=category, classify=classify)

    # -------------------------------------------------------------------------
    def depth(self, node_id, level=0):
        """
            Determine the depth of a hierarchy

            Args:
                node_id: the start node (default to all root nodes)
        """

        nodes = self.nodes
        depth = self.depth

        node = nodes.get(node_id)
        if not node:
            roots = self.roots
            result = max(depth(root) for root in roots)
        else:
            children = node["s"]
            if children:
                result = max(depth(n, level=level+1) for n in children)
            else:
                result = level
        return result

    # -------------------------------------------------------------------------
    def siblings(self,
                 node_id,
                 category=DEFAULT,
                 classify=False,
                 inclusive=False):
        """
            Get the sibling nodes of a node. If the node is a root node,
            this method returns all root nodes.

            Args:
                node_id: the node ID
                category: return only nodes of this category
                classify: return each node as tuple (id, category)
                          instead of just id
                inclusive: include the start node

            Returns:
                a set of node IDs (or tuples (id, category), respectively)
        """

        result = set()
        nodes = self.nodes

        node = nodes.get(node_id)
        if not node:
            return result

        parent_id = node["p"]
        if not parent_id:
            siblings = [(k, nodes[k]) for k in self.roots]
        else:
            parent = nodes[parent_id]
            if parent["s"]:
                siblings = [(k, nodes[k]) for k in parent["s"]]
            else:
                siblings = []

        add = result.add
        for sibling_id, sibling_node in siblings:
            if not inclusive and sibling_id == node_id:
                continue
            if category is DEFAULT or category == sibling_node["c"]:
                sibling = (sibling_id, sibling_node["c"]) \
                          if classify else sibling_id
                add(sibling)
        return result

    # -------------------------------------------------------------------------
    def findall(self,
                node_id,
                category=DEFAULT,
                classify=False,
                inclusive=False):
        """
            Find descendant nodes of a node

            Args:
                node_id: the node ID (can be an iterable of node IDs)
                category: find nodes of this category
                classify: return each node as tuple (id, category) instead
                          of just ids
                inclusive: include the start node(s) if they match

            Returns:
                a set of node IDs (or tuples (id, category), respectively)
        """

        result = set()
        findall = self.findall
        if isinstance(node_id, (set, list, tuple)):
            for n in node_id:
                if n is None:
                    continue
                result |= findall(n,
                                  category=category,
                                  classify=classify,
                                  inclusive=inclusive)
            return result
        nodes = self.nodes
        node = nodes.get(node_id)
        if not node:
            return result
        if node["s"]:
            result |= findall(node["s"],
                              category=category,
                              classify=classify,
                              inclusive=True)
        if inclusive:
            this = (node_id, node["c"]) if classify else node_id
            if category is DEFAULT or category == node["c"]:
                result.add(this)
        return result

    # -------------------------------------------------------------------------
    def _represent(self, node_ids=None, renderer=None):
        """
            Represent nodes as labels, the labels are stored in the
            nodes as attribute "l".

            Args:
                node_ids: the node IDs (None for all nodes)
                renderer: the representation method (falls back to the "name"
                          field in the target table if present)
        """

        theset = self.theset

        if node_ids is None:
            node_ids = self.nodes.keys()

        pending = set()
        for node_id in node_ids:
            node = theset.get(node_id)
            if not node:
                continue
            if "l" not in node:
                pending.add(node_id)

        if renderer is None:
            renderer = self.represent
        if renderer is None:
            tablename = self.tablename
            table = current.s3db.table(tablename) if tablename else None
            if table and "name" in table.fields:
                self.represent = renderer = S3Represent(lookup = tablename,
                                                        key = self.pkey.name)
            else:
                renderer = s3_str
        if hasattr(renderer, "bulk"):
            labels = renderer.bulk(list(pending), list_type = False)
            for node_id, label in labels.items():
                if node_id in theset:
                    theset[node_id]["l"] = label
        else:
            for node_id in pending:
                try:
                    label = renderer(node_id)
                except:
                    label = s3_str(node_id)
                theset[node_id]["l"] = label
        return

    # -------------------------------------------------------------------------
    def label(self, node_id, represent=None):
        """
            Get a label for a node

            Args:
                node_id: the node ID
                represent: the node ID representation method
        """

        theset = self.theset
        node = theset.get(node_id)
        if node:
            if "l" in node:
                label = node["l"]
            else:
                self._represent(node_ids=[node_id], renderer=represent)
            if "l" in node:
                label = node["l"]
            if type(label) is str:
                label = s3_str(label)
            return label
        return None

    # -------------------------------------------------------------------------
    def repr_expand(self, node_ids, levels=None, represent=None):
        """
            Helper function to represent a set of nodes as lists
            of their respective ancestors starting by the root node

            Args:
                node_ids: the node_ids (iterable)
                levels: the number of levels to include (counting from root)
                represent: a representation function for each ancestor

            Returns:
                a dict {node_id: ["Label", "Label", ...]}
                - each label list is padded with "-" to reach the
                  requested number of levels (TODO make this configurable)
        """

        paths = {}
        all_parents = set()
        for node_id in node_ids:
            paths[node_id] = path = self.path(node_id)
            all_parents |= set(path)

        self._represent(all_parents, renderer=represent)

        theset = self.theset
        result = {}
        for node_id, path in paths.items():
            p = (path + [None] * levels)[:levels]
            l = [theset[parent]["l"] if parent else "-" for parent in p]
            result[node_id] = l

        return result

    # -------------------------------------------------------------------------
    def _json(self, node_id, represent=None, depth=0, max_depth=None):
        """
            Represent a node as JSON-serializable array

            Args:
                node_id: the node ID
                represent: the representation method
                depth: the current recursion depth
                max_depth: the maximum recursion depth

            Returns:
                the node as [label, category, subnodes], with subnodes as:
                    - False: if there are no subnodes
                    - True: if there are subnodes beyond max_depth
                    - otherwise: {node_id: [label, category, subnodes], ...}
        """

        node = self.nodes.get(node_id)
        if not node:
            return None

        label = self.label(node_id, represent=represent)
        if label is None:
            label = node_id

        category = node["c"]
        subnode_ids = node["s"]
        if subnode_ids:
            if max_depth and depth == max_depth:
                subnodes = True
            else:
                subnodes = {}
                for subnode_id in subnode_ids:
                    item = self._json(subnode_id,
                                      represent = represent,
                                      depth = depth + 1,
                                      max_depth = max_depth,
                                      )
                    if item:
                        subnodes[subnode_id] = item
        else:
            subnodes = False

        return [s3_str(label), category, subnodes]

    # -------------------------------------------------------------------------
    def json(self, root=None, represent=None, max_depth=None):
        """
            Represent the hierarchy as JSON-serializable dict

            Args:
                root: the root node ID (or array of root node IDs)
                represent: the representation method
                max_depth: maximum recursion depth

            Returns:
                the hierarchy as dict:
                    {node_id: [label, category, subnodes], ...}
        """

        self._represent(renderer=represent)

        roots = [root] if root else self.roots

        output = {}
        for node_id in roots:
            item = self._json(node_id,
                              represent = represent,
                              max_depth = max_depth,
                              )
            if item:
                output[node_id] = item

        return output

    # -------------------------------------------------------------------------
    def html(self,
             widget_id,
             root=None,
             represent=None,
             hidden=True,
             none=None,
             _class=None):
        """
            Render this hierarchy as nested unsorted list

            Args:
                widget_id: a unique ID for the HTML widget
                root: node ID of the start node (defaults to all
                      available root nodes)
                represent: the representation method for the node IDs
                hidden: render with style display:none
                _class: the HTML class for the outermost list

            Returns:
                the list (UL)
        """

        self._represent(renderer=represent)

        roots = [root] if root else self.roots

        html = self._html
        output = UL([html(node_id, widget_id, represent=represent)
                    for node_id in roots],
                    _id=widget_id,
                    _style="display:none" if hidden else None)
        if _class:
            output.add_class(_class)
        if none:
            if none is True:
                none = current.messages["NONE"]
            output.insert(0, LI(none,
                                _id="%s-None" % widget_id,
                                _rel = "none",
                                _class = "s3-hierarchy-node s3-hierarchy-none",
                                ))
        return output

    # -------------------------------------------------------------------------
    def _html(self, node_id, widget_id, represent=None):
        """
            Recursively render a node as list item (with subnodes
            as unsorted list inside the item)

            Args:
                node_id: the node ID
                widget_id: the unique ID for the outermost list
                represent: the node ID representation method

            Returns:
                the list item (LI)

            TODO option to add CRUD permissions
        """

        node = self.nodes.get(node_id)
        if not node:
            return None

        label = self.label(node_id, represent=represent)
        if label is None:
            label = s3_str(node_id)

        subnodes = node["s"]
        item = LI(label,
                  _id = "%s-%s" % (widget_id, node_id),
                  _rel = "parent" if subnodes else "leaf",
                  _class = "s3-hierarchy-node",
                  )

        html = self._html
        if subnodes:
            sublist = UL([html(n, widget_id, represent=represent)
                         for n in subnodes])
            item.append(sublist)
        return item

    # -------------------------------------------------------------------------
    def export_node(self,
                    node_id,
                    prefix = "_hierarchy",
                    depth=None,
                    level=0,
                    path=None,
                    hcol=None,
                    columns = None,
                    data = None,
                    node_list=None):
        """
            Export the hierarchy beneath a node

            Args:
                node_id: the root node
                prefix: prefix for the hierarchy column in the output
                depth: the maximum depth to export
                level: the current recursion level (internal)
                path: the path dict for this node (internal)
                hcol: the hierarchy column in the input data
                columns: the list of columns to export
                data: the input data dict {node_id: row}
                node_list: the output data list (will be appended to)

            Returns:
                the output data list

            TODO pass the input data as list and retain the original
                 order when recursing into child nodes?
        """

        if node_list is None:
            node_list = []

        # Do not recurse deeper than depth levels below the root node
        if depth is not None and level > depth:
            return node_list

        # Get the current node
        node = self.nodes.get(node_id)
        if not node:
            return node_list

        # Get the node data
        if data:
            if node_id not in data:
                return node_list
            node_data = data.get(node_id)
        else:
            node_data = {}

        # Generate the path dict if it doesn't exist yet
        if path is None:
            if depth is None:
                depth = self.depth(node_id)
            path = dict(("%s.%s" % (prefix, l), "") for l in range(depth+1))

        # Set the hierarchy column
        label = node_data.get(hcol) if hcol else node_id
        path["%s.%s" % (prefix, level)] = label

        # Add remaining columns to the record dict
        record = dict(path)
        if columns:
            for column in columns:
                if columns == hcol:
                    continue
                record[column] = node_data.get(column)

        # Append the record to the node list
        node_list.append(record)

        # Recurse into child nodes
        children = node["s"]
        for child in children:
            self.export_node(child,
                             prefix = prefix,
                             depth=depth,
                             level=level+1,
                             path=dict(path),
                             hcol=hcol,
                             columns=columns,
                             data=data,
                             node_list=node_list,
                             )
        return node_list

# END =========================================================================
