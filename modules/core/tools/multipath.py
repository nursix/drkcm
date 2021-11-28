"""
    Tool for Multi-Ancestor Hypergraphs

    Copyright: (c) 2010-2021 Sahana Software Foundation

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

__all__ = ("S3MultiPath",
           )

# =============================================================================
class S3MultiPath:
    """
        Simplified path toolkit for managing multi-ancestor-hypergraphs
        in a relational database.

        MultiPaths allow single-query searches for all ancestors and
        descendants of a node, as well as single-query affiliation
        testing - whereas they require multiple writes on update (one
        per each descendant node), so they should only be used for
        hypergraphs which rarely change.

        Every node of the hypergraph contains a path attribute, with the
        following MultiPath-syntax:

        MultiPath: <SimplePath>,<SimplePath>,...
        SimplePath: [|<Node>|<Node>|...|]
        Node: ID of the ancestor node

        SimplePaths contain only ancestors, not the node itself.

        SimplePaths contain the ancestors in reverse order, i.e. the nearest
        ancestor first (this is important because removing a vertex from the
        path will cut off the tail, not the head)

        A path like A<-B<-C can be constructed like:

            path = S3MultiPath([["C", "B", "A"]])
            [|C|B|A|]

        Extending this path by a vertex E<-B will result in a multipath like:

            path.extend("B", "E")
            [|C|B|A|],[|C|B|E|]

        Cutting the vertex A<-B reduces the multipath to:

            path.cut("B", "A")
            [|C|B|E|]

        Note the reverse notation (nearest ancestor first)!

        MultiPaths will be normalized automatically, i.e.:

            path = S3MultiPath([["C", "B", "A", "D", "F", "B", "E", "G"]])
            [|C|B|A|D|F|],[|C|B|E|G|]
    """

    # -------------------------------------------------------------------------
    # Construction
    #
    def __init__(self, paths=None):

        self.paths = []
        if isinstance(paths, S3MultiPath):
            self.paths = list(paths.paths)
        else:
            if paths is None:
                paths = []
            elif type(paths) is str:
                paths = self.__parse(paths)
            elif not isinstance(paths, (list, tuple)):
                paths = [paths]
            append = self.append
            for p in paths:
                append(p)

    # -------------------------------------------------------------------------
    def append(self, path):
        """
            Append a new ancestor path to this multi-path

            Args:
                path: the ancestor path
        """

        Path = self.Path

        if isinstance(path, Path):
            path = path.nodes
        else:
            path = Path(path).nodes
        multipath = None

        # Normalize any recurrent paths
        paths = self.__normalize(path)

        append = self.paths.append
        for p in paths:
            p = Path(p)
            if not self & p:
                append(p)
                multipath = self
        return multipath

    # -------------------------------------------------------------------------
    def extend(self, head, ancestors=None, cut=None):
        """
            Extend this multi-path with a new vertex ancestors<-head

            Args:
                head: the head node
                ancestors: the ancestor (multi-)path of the head node
        """

        # If ancestors is a multi-path, extend recursively with all paths
        if isinstance(ancestors, S3MultiPath):
            extend = self.extend
            for p in ancestors.paths:
                extend(head, p, cut=cut)
            return self

        # Split-extend all paths which contain the head node
        extensions = []
        Path = self.Path
        append = extensions.append
        for p in self.paths:
            if cut:
                pos = p.find(cut)
                if pos > 0:
                    p.nodes = p.nodes[:pos-1]
            i = p.find(head)
            if i > 0:
                path = Path(p.nodes[:i]).extend(head, ancestors)
                detour = None
                for tail in self.paths:
                    j = tail.find(path.last())
                    if j > 0:
                        # append original tail
                        detour = Path(path)
                        detour.extend(path.last(), tail[j:])
                        append(detour)
                if not detour:
                    append(path)
        self.paths.extend(extensions)

        # Finally, cleanup for duplicate and empty paths
        return self.clean()

    # -------------------------------------------------------------------------
    def cut(self, head, ancestor=None):
        """
            Cut off the vertex ancestor<-head in this multi-path

            Args:
                head: the head node
                ancestor: the ancestor node to cut off
        """

        for p in self.paths:
            p.cut(head, ancestor)
        # Must cleanup for duplicates
        return self.clean()

    # -------------------------------------------------------------------------
    def clean(self):
        """
            Remove any duplicate and empty paths from this multi-path
        """

        mp = S3MultiPath(self)
        pop = mp.paths.pop
        self.paths = []
        append = self.paths.append
        while len(mp):
            item = pop(0)
            if len(item) and not mp & item and not self & item:
                append(item)
        return self

    # -------------------------------------------------------------------------
    # Serialization/Deserialization
    #
    def __parse(self, value):
        """ Parse a multi-path-string into nodes """

        return value.split(",")

    def __repr__(self):
        """ Serialize this multi-path as string """

        return ",".join([str(p) for p in self.paths])

    def as_list(self):
        """ Return this multi-path as list of node lists """

        return [p.as_list() for p in self.paths if len(p)]

    # -------------------------------------------------------------------------
    # Introspection
    #
    def __len__(self):
        """ The number of paths in this multi-path """

        return len(self.paths)

    # -------------------------------------------------------------------------
    def __and__(self, sequence):
        """
            Check whether sequence is the start sequence of any of
            the paths in this multi-path (for de-duplication)

            Args:
                sequence: sequence of node IDs (or path)
        """

        for p in self.paths:
            if p.startswith(sequence):
                return 1
        return 0

    # -------------------------------------------------------------------------
    def __contains__(self, sequence):
        """
            Check whether sequence is contained in any of the paths (can
            also be used to check whether this multi-path contains a path
            to a particular node)

            Args:
                sequence: the sequence (or node ID)
        """

        for p in self.paths:
            if sequence in p:
                return 1
        return 0

    # -------------------------------------------------------------------------
    def nodes(self):
        """ Get all nodes from this path """

        nodes = []
        for p in self.paths:
            n = [i for i in p.nodes if i not in nodes]
            nodes.extend(n)
        return nodes

    # -------------------------------------------------------------------------
    @staticmethod
    def all_nodes(paths):
        """
            Get all nodes from all paths

            Args:
                paths: list of multi-paths
        """

        nodes = []
        for p in paths:
            n = [i for i in p.nodes() if i not in nodes]
            nodes.extend(n)
        return nodes

    # -------------------------------------------------------------------------
    # Normalization
    #
    @staticmethod
    def __normalize(path):
        """
            Normalize a path into a sequence of non-recurrent paths

            Args:
                path: the path as a list of node IDs
        """

        seq = [str(item) for item in path]
        if len(seq) < 2:
            return [path]
        seq = S3MultiPath.__resolve(seq)
        pop = seq.pop
        paths = []
        append = paths.append
        while len(seq):
            p = pop(0)
            s = paths + seq
            contained = False
            lp = len(p)
            for i in s:
                if i[:lp] == p:
                    contained = True
                    break
            if not contained:
                append(p)
        return paths

    # -------------------------------------------------------------------------
    @staticmethod
    def __resolve(seq):
        """
            Resolve a sequence of vertices (=pairs of node IDs) into a
            sequence of non-recurrent paths

            Args:
                seq: the vertex sequence
        """

        resolve = S3MultiPath.__resolve
        if seq:
            head = seq[0]
            tail = seq[1:]
            tails = []
            index = tail.index
            append = tails.append
            while head in tail:
                pos = index(head)
                append(tail[:pos])
                tail = tail[pos + 1:]
            append(tail)
            r = []
            append = r.append
            for tail in tails:
                nt = resolve(tail)
                for t in nt:
                    append([head] + t)
            return r
        else:
            return [seq]

    # -------------------------------------------------------------------------
    # Helper class for simple ancestor paths
    #
    class Path:

        # ---------------------------------------------------------------------
        # Construction methods
        #
        def __init__(self, nodes=None):

            self.nodes = []
            if isinstance(nodes, S3MultiPath.Path):
                self.nodes = list(nodes.nodes)
            else:
                if nodes is None:
                    nodes = []
                elif type(nodes) is str:
                    nodes = self.__parse(nodes)
                elif not isinstance(nodes, (list, tuple)):
                    nodes = [nodes]
                append = self.append
                for n in nodes:
                    if not append(n):
                        break

        # ---------------------------------------------------------------------
        def append(self, node=None):
            """
                Append a node to this path

                Args:
                    node: the node
            """

            if node is None:
                return True
            n = str(node)
            if not n:
                return True
            if n not in self.nodes:
                self.nodes.append(n)
                return True
            return False

        # ---------------------------------------------------------------------
        def extend(self, head, ancestors=None):
            """
                Extend this path with a new vertex ancestors<-head, if this
                path ends at the head node

                Args:
                    head: the head node
                    ancestors: the ancestor sequence
            """

            if ancestors is None:
                # If no head node is specified, use the first ancestor node
                path = S3MultiPath.Path(head)
                head = path.first()
                ancestors = path.nodes[1:]
            last = self.last()
            if last is None or last == str(head):
                append = self.append
                path = S3MultiPath.Path(ancestors)
                for i in path.nodes:
                    if not append(i):
                        break
                return self
            else:
                return None

        # ---------------------------------------------------------------------
        def cut(self, head, ancestor=None):
            """
                Cut off the ancestor<-head vertex from this path, retaining
                the head node

                Args:
                    head: the head node
                    ancestor: the ancestor node
            """

            if ancestor is not None:
                sequence = [str(head), str(ancestor)]
                pos = self.find(sequence)
                if pos > 0:
                    self.nodes = self.nodes[:pos]
            else:
                # if ancestor is None and the path starts with head,
                # then remove the entire path
                if str(head) == self.first():
                    self.nodes = []
            return self

        # ---------------------------------------------------------------------
        # Serialize/Deserialize
        #
        def __repr__(self):
            """ Represent this path as a string """

            return "[|%s|]" % "|".join(self.nodes)

        def __parse(self, value):
            """ Parse a string into nodes """

            return value.strip().strip("[").strip("]").strip("|").split("|")

        def as_list(self):
            """ Return the list of nodes """

            return list(self.nodes)

        # ---------------------------------------------------------------------
        # Item access
        #
        def __getitem__(self, i):
            """ Get the node at position i """

            try:
                return self.nodes.__getitem__(i)
            except IndexError:
                return None

        # ---------------------------------------------------------------------
        def first(self):
            """ Get the first node in this path (the nearest ancestor) """

            return self[0]

        # ---------------------------------------------------------------------
        def last(self):
            """ Get the last node in this path (the most distant ancestor) """

            return self[-1]

        # ---------------------------------------------------------------------
        # Tests
        #
        def __contains__(self, sequence):
            """
                Check whether this path contains sequence

                Args:
                    sequence: sequence of node IDs
            """

            if self.find(sequence) != -1:
                return 1
            else:
                return 0

        # ---------------------------------------------------------------------
        def __len__(self):
            """
                Get the number of nodes in this path
            """

            return len(self.nodes)

        # ---------------------------------------------------------------------
        def find(self, sequence):
            """
                Find a sequence of node IDs in this path

                Args:
                    sequence: sequence of node IDs (or path)

                Returns:
                    position of the sequence (index+1), 0 if the path
                    is empty, -1 if the sequence wasn't found
            """

            path = S3MultiPath.Path(sequence)
            sequence = path.nodes
            nodes = self.nodes
            if not sequence:
                return -1
            if not nodes:
                return 0
            head, tail = sequence[0], sequence[1:]
            pos = 0
            l = len(tail)
            index = nodes.index
            while head in nodes[pos:]:
                pos = index(head, pos) + 1
                if not tail or nodes[pos:pos+l] == tail:
                    return pos
            return -1

        # ---------------------------------------------------------------------
        def startswith(self, sequence):
            """
                Check whether this path starts with sequence

                Args:
                    sequence: sequence of node IDs (or path)
            """

            sequence = S3MultiPath.Path(sequence).nodes
            if self.nodes[0:len(sequence)] == sequence:
                return True
            else:
                return False

# END =========================================================================
