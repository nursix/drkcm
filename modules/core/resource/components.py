"""
    Lazy Components Loader

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

__all__ = ("S3Components",
           )

import sys

from gluon import current

from .query import FS

DEFAULT = lambda: None

# =============================================================================
class S3Components:
    """
        Lazy component loader
    """

    def __init__(self, master, expose=None):
        """
            Args:
                master: the master resource (CRUDResource)
                expose: aliases of components to expose, defaults to
                        all configured components
        """

        self.master = master

        if expose is None:
            hooks = current.s3db.get_hooks(master.tablename)[1]
            if hooks:
                self.exposed_aliases = set(hooks.keys())
            else:
                self.exposed_aliases = set()
        else:
            self.exposed_aliases = set(expose)

        self._components = {}
        self._exposed = {}

        self.links = {}

    # -------------------------------------------------------------------------
    def get(self, alias, default=None):
        """
            Access a component resource by its alias; will load the
            component if not loaded yet

            Args:
                alias: the component alias
                default: default to return if the alias is not defined

            Returns:
                the component resource (CRUDResource)
        """

        components = self._components

        component = components.get(alias)
        if not component:
            self.__load((alias,))
            return components.get(alias, default)
        else:
            db = current.db
            table_alias = component._alias
            if not getattr(db, table_alias, None):
                setattr(db._aliased_tables, table_alias, component.table)
            return component

    # -------------------------------------------------------------------------
    def __getitem__(self, alias):
        """
            Access a component by its alias in key notation; will load the
            component if not loaded yet

            Args:
                alias: the component alias

            Returns:
                the component resource (CRUDResource)

            Raises:
                KeyError: if the component is not defined
        """

        component = self.get(alias)
        if component is None:
            raise KeyError
        else:
            return component

    # -------------------------------------------------------------------------
    def __contains__(self, alias):
        """
            Check if a component is defined for this resource

            Args:
                alias: the alias to check

            Returns:
                True|False whether the component is defined
        """

        return bool(self.get(alias))

    # -------------------------------------------------------------------------
    @property
    def loaded(self):
        """
            Get all currently loaded components

            Returns:
                dict {alias: resource} with loaded components
        """
        return self._components

    # -------------------------------------------------------------------------
    @property
    def exposed(self):
        """
            Get all exposed components (=> will thus load them all)

            Returns:
                dict {alias: resource} with exposed components
        """

        loaded = self._components
        exposed = self._exposed

        missing = set()
        for alias in self.exposed_aliases:
            if alias not in exposed:
                if alias in loaded:
                    exposed[alias] = loaded[alias]
                else:
                    missing.add(alias)

        if missing:
            self.__load(missing)

        return exposed

    # -------------------------------------------------------------------------
    # Methods kept for backwards-compatibility
    # - to be deprecated
    # - use-cases should explicitly address either .loaded or .exposed
    #
    def keys(self):
        """
            Get the aliases of all exposed components ([alias])
        """
        return list(self.exposed.keys())

    def values(self):
        """
            Get all exposed components ([resource])
        """
        return list(self.exposed.values())

    def items(self):
        """
            Get all exposed components ([(alias, resource)])
        """
        return list(self.exposed.items())

    # -------------------------------------------------------------------------
    def __load(self, aliases, force=False):
        """
            Instantiate component resources

            Args:
                aliases: iterable of aliases of components to instantiate
                force: forced reload of components

            Returns:
                dict of loaded components {alias: resource}
        """

        s3db = current.s3db

        master = self.master

        components = self._components
        exposed = self._exposed
        exposed_aliases = self.exposed_aliases

        links = self.links

        if aliases:
            if force:
                # Forced reload
                new = aliases
            else:
                new = [alias for alias in aliases if alias not in components]
        else:
            new = None

        hooks = s3db.get_components(master.table, names=new)
        if not hooks:
            return {}

        for alias, hook in hooks.items():

            filterby = hook.filterby
            if alias is not None and filterby is not None:
                table_alias = "%s_%s_%s" % (hook.prefix,
                                            hook.alias,
                                            hook.name,
                                            )
                table = s3db.get_aliased(hook.table, table_alias)
                hook.table = table
            else:
                table_alias = None
                table = hook.table

            # Instantiate component resource
            from .resource import CRUDResource
            component = CRUDResource(table,
                                     parent = master,
                                     alias = alias,
                                     linktable = hook.linktable,
                                     include_deleted = master.include_deleted,
                                     approved = master._approved,
                                     unapproved = master._unapproved,
                                     )

            if table_alias:
                component.tablename = hook.tablename
                component._alias = table_alias

            # Copy hook properties to the component resource
            component.pkey = hook.pkey
            component.fkey = hook.fkey

            component.linktable = hook.linktable
            component.lkey = hook.lkey
            component.rkey = hook.rkey
            component.actuate = hook.actuate
            component.autodelete = hook.autodelete
            component.autocomplete = hook.autocomplete

            #component.alias = alias
            component.multiple = hook.multiple
            component.defaults = hook.defaults

            # Component filter
            if not filterby:
                # Can use filterby=False to enforce table aliasing yet
                # suppress component filtering, useful e.g. if the same
                # table is declared as component more than once for the
                # same master table (using different foreign keys)
                component.filter = None

            else:
                if callable(filterby):
                    # Callable to construct complex join filter
                    # => pass the (potentially aliased) component table
                    query = filterby(table)
                elif isinstance(filterby, dict):
                    # Filter by multiple criteria
                    query = None
                    for k, v in filterby.items():
                        if isinstance(v, FS):
                            # Match a field in the master table
                            # => identify the field
                            try:
                                rfield = v.resolve(master)
                            except (AttributeError, SyntaxError):
                                if current.response.s3.debug:
                                    raise
                                else:
                                    current.log.error(sys.exc_info()[1])
                                    continue
                            # => must be a real field in the master table
                            field = rfield.field
                            if not field or field.table != master.table:
                                current.log.error("Component filter for %s<=%s: "
                                                  "invalid lookup field '%s'" %
                                                  (master.tablename, alias, v.name))
                                continue
                            subquery = (table[k] == field)
                        else:
                            is_list = isinstance(v, (tuple, list))
                            if is_list and len(v) == 1:
                                filterfor = v[0]
                                is_list = False
                            else:
                                filterfor = v
                            if not is_list:
                                subquery = (table[k] == filterfor)
                            elif filterfor:
                                subquery = (table[k].belongs(set(filterfor)))
                            else:
                                continue
                        if subquery:
                            if query is None:
                                query = subquery
                            else:
                                query &= subquery
                if query:
                    component.filter = query

            # Copy component properties to the link resource
            link = component.link
            if link is not None:

                link.pkey = component.pkey
                link.fkey = component.lkey

                link.multiple = component.multiple

                link.actuate = component.actuate
                link.autodelete = component.autodelete

                # Register the link table
                links[link.name] = links[link.alias] = link

            # Register the component
            components[alias] = component

            if alias in exposed_aliases:
                exposed[alias] = component

        return components

    # -------------------------------------------------------------------------
    def reset(self, aliases=None, expose=DEFAULT):
        """
            Detach currently loaded components, e.g. to force a reload

            Args:
                aliases: aliases to remove, None for all
                expose: aliases of components to expose (default:
                        keep previously exposed aliases), None for
                        all configured components
        """

        if expose is not DEFAULT:
            if expose is None:
                hooks = current.s3db.get_hooks(self.master.tablename)[1]
                if hooks:
                    self.exposed_aliases = set(hooks.keys())
                else:
                    self.exposed_aliases = set()
            else:
                self.exposed_aliases = set(expose)

        if aliases:

            loaded = self._components
            links = self.links
            exposed = self._exposed

            for alias in aliases:
                component = loaded.pop(alias, None)
                if component:
                    link = component.link
                    for k, v in list(links.items()):
                        if v is link:
                            links.pop(k)
                    exposed.pop(alias, None)
        else:
            self._components = {}
            self._exposed = {}

            self.links.clear()

# END =========================================================================
