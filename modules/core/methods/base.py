"""
    CRUD Methods

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

__all__ = ("CRUDMethod",
           )

import os
import re

from gluon import current
from gluon.storage import Storage

# =============================================================================
class CRUDMethod:
    """ CRUD Access Method """

    def __init__(self):

        self.request = None
        self.method = None

        self.download_url = None
        self.hide_filter = False


        self.prefix = None
        self.name = None
        self.resource = None

        self.tablename = None
        self.table = None
        self.record_id = None

        self.next = None

    # -------------------------------------------------------------------------
    def __call__(self, r, method=None, widget_id=None, **attr):
        """
            Entry point for the CRUD controller

            Args:
                r: the CRUDRequest
                method: the method established by the CRUD controller
                widget_id: widget ID
                attr: dict of parameters for the method handler

            Keyword Args:
                hide_filter: whether to hide filter forms
                    - None  = show filters on master, hide for components
                    - False = show all filters (on all tabs)
                    - True  = hide all filters (on all tabs)
                    - {alias=setting} = setting per component,
                                        alias None means master resource,
                                        alias "_default" to specify an
                                        alternative default

            Returns:
                output object to send to the view
        """

        # Environment of the request
        self.request = r

        # Settings
        response = current.response
        self.download_url = response.s3.download_url

        # Override request method
        self.method = method if method else r.method

        # Find the target resource and record
        if r.component:
            component = r.component
            resource = component
            self.record_id = self._record_id(r)
            if not self.method:
                if component.multiple and not r.component_id:
                    self.method = "list"
                else:
                    self.method = "read"
            if component.link:
                actuate_link = r.actuate_link()
                if not actuate_link:
                    resource = component.link
        else:
            self.record_id = r.id
            resource = r.resource
            if not self.method:
                if r.id or r.method in ("read", "display"):
                    self.method = "read"
                else:
                    self.method = "list"

        self.prefix = resource.prefix
        self.name = resource.name
        self.tablename = resource.tablename
        self.table = resource.table
        self.resource = resource

        if r.interactive:
            hide_filter = attr.get("hide_filter")
            if isinstance(hide_filter, dict):
                component_name = r.component_name
                if component_name in hide_filter:
                    hide_filter = hide_filter[component_name]
                elif "_default" in hide_filter:
                    hide_filter = hide_filter["_default"]
                else:
                    hide_filter = None
            if hide_filter is None:
                hide_filter = r.component is not None
            self.hide_filter = hide_filter
        else:
            self.hide_filter = True

        # Apply method
        if widget_id and hasattr(self, "widget"):
            output = self.widget(r,
                                 method = self.method,
                                 widget_id = widget_id,
                                 **attr)
        else:
            output = self.apply_method(r, **attr)

            # Redirection
            if self.next and resource.lastid:
                self.next = str(self.next)
                placeholder = "%5Bid%5D"
                self.next = self.next.replace(placeholder, resource.lastid)
                placeholder = "[id]"
                self.next = self.next.replace(placeholder, resource.lastid)
            if not response.error:
                r.next = self.next

            # Add additional view variables (e.g. rheader)
            self._extend_view(output, r, **attr)

        return output

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Stub, to be implemented in subclass. This method is used
            to get the results as a standalone page.

            Args:
                r: the CRUDRequest
                attr: dictionary of parameters for the method handler

            Returns:
                output object to send to the view
        """

        output = {}
        return output

    # -------------------------------------------------------------------------
    def widget(self, r, method=None, widget_id=None, visible=True, **attr):
        """
            Stub, to be implemented in subclass. This method is used
            by other method handlers to embed this method as widget.

            Args:
                r: the CRUDRequest
                method: the URL method
                widget_id: the widget ID
                visible: whether the widget is initially visible
                attr: dictionary of parameters for the method handler

            Returns:
                output, see below

            Notes:
                For "html" format, the widget method must return an XML
                component that can be embedded in a DIV. If a dict is
                returned, it will be rendered against the view template
                of the calling method - the view template selected by
                the widget method will be ignored.

                For other formats, the data returned by the widget method
                will be rendered against the view template selected by
                the widget method. If no view template is set, the data
                will be returned as-is.

                The widget must use the widget_id as HTML id for the element
                providing the Ajax-update hook and this element must be
                visible together with the widget.

                The widget must include the widget_id as ?w=<widget_id> in
                the URL query of the Ajax-update call, and Ajax-calls should
                not use "html" format.

                If visible==False, then the widget will initially be hidden,
                so it can be rendered empty and Ajax-load its data layer
                upon a separate refresh call. Otherwise, the widget should
                receive its data layer immediately. Widgets can ignore this
                parameter if delayed loading of the data layer is not
                all([possible, useful, supported]).
        """

        return None

    # -------------------------------------------------------------------------
    # Utility functions
    # -------------------------------------------------------------------------
    def _permitted(self, method=None):
        """
            Check permission for the requested resource

            Args:
                method: method to check, defaults to the actually
                        requested method

            Returns:
                bool: whether the action is allowed for the target resource
        """

        auth = current.auth
        has_permission = auth.s3_has_permission

        r = self.request

        if not method:
            method = self.method

        if r.component is None:
            table = r.table
            record_id = r.id
        else:
            table = r.component.table
            record_id = r.component_id

            if method == "create":
                # Is creating a new component record allowed without
                # permission to update the master record?
                writable = current.s3db.get_config(r.tablename,
                                                   "ignore_master_access",
                                                   )
                if not isinstance(writable, (tuple, list)) or \
                   r.component_name not in writable:
                    master_access = has_permission("update",
                                                   r.table,
                                                   record_id = r.id,
                                                   )
                    if not master_access:
                        return False

        return has_permission(method, table, record_id=record_id)

    # -------------------------------------------------------------------------
    @staticmethod
    def _record_id(r):
        """
            Get the ID of the target record of a CRUDRequest

            Args:
                r: the CRUDRequest

            Returns:
                the target record ID
        """

        master_id = r.id

        if r.component:

            component = r.component
            component_id = r.component_id
            link = r.link

            if not component.multiple and not component_id:
                # Enforce first component record
                table = component.table
                pkey = table._id.name
                component.load(start=0, limit=1)
                if len(component):
                    component_id = component.records().first()[pkey]
                    if link and master_id:
                        r.link_id = link.link_id(master_id, component_id)
                    r.component_id = component_id
                    component.add_filter(table._id == component_id)

            if not link or r.actuate_link():
                return component_id
            else:
                return r.link_id
        else:
            return master_id

        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def _view(r, default):
        """
            Get the path to the view template

            Args:
                r: the CRUDRequest
                default: name of the default view template

            Returns:
                path to view
        """

        folder = r.folder
        prefix = r.controller

        exists = os.path.exists
        join = os.path.join

        settings = current.deployment_settings
        theme = settings.get_theme()
        theme_layouts = settings.get_theme_layouts()

        if theme != "default":
            # See if there is a Custom View for this Theme
            view = join(folder, "modules", "templates", theme_layouts, "views",
                        "%s_%s_%s" % (prefix, r.name, default))
            if exists(view):
                # There is a view specific to this page
                # NB This should normally include {{extend layout.html}}
                # Pass view as file not str to work in compiled mode
                return open(view, "rb")
            else:
                if "/" in default:
                    subfolder, default_ = default.split("/", 1)
                else:
                    subfolder = ""
                    default_ = default
                if exists(join(folder, "modules", "templates", theme_layouts, "views",
                               subfolder, "_%s" % default_)):
                    # There is a general view for this page type
                    # NB This should not include {{extend layout.html}}
                    if subfolder:
                        subfolder = "%s/" % subfolder
                    # Pass this mapping to the View
                    current.response.s3.views[default] = \
                        "../modules/templates/%s/views/%s_%s" % (theme_layouts,
                                                                 subfolder,
                                                                 default_,
                                                                 )

        if r.component:
            view = "%s_%s_%s" % (r.name, r.component_name, default)
            path = join(folder, "views", prefix, view)
            if exists(path):
                return "%s/%s" % (prefix, view)
            else:
                view = "%s_%s" % (r.name, default)
                path = join(folder, "views", prefix, view)
        else:
            view = "%s_%s" % (r.name, default)
            path = join(folder, "views", prefix, view)

        if exists(path):
            return "%s/%s" % (prefix, view)
        else:
            return default

    # -------------------------------------------------------------------------
    @staticmethod
    def _extend_view(output, r, **attr):
        """
            Add additional view variables (invokes all callables)

            Args:
                output: the output dict
                r: the CRUDRequest
                attr: the view variables (e.g. 'rheader')

            Note:
                Overload this method in subclasses if you don't want
                additional view variables to be added automatically
        """

        if r.interactive and isinstance(output, dict):
            for key in attr:
                handler = attr[key]
                if callable(handler):
                    resolve = True
                    try:
                        display = handler(r)
                    except TypeError:
                        # Argument list failure
                        # => pass callable to the view as-is
                        display = handler
                        continue
                    except:
                        # Propagate all other errors to the caller
                        raise
                else:
                    resolve = False
                    display = handler
                if isinstance(display, dict) and resolve:
                    output.update(**display)
                elif display is not None:
                    output[key] = display
                elif key in output and callable(handler):
                    del output[key]

    # -------------------------------------------------------------------------
    @staticmethod
    def _remove_filters(get_vars):
        """
            Remove all filters from URL vars

            Args:
                get_vars: the URL vars as dict

            Returns:
                the filtered URL vars (Storage)
        """

        regex_filter = re.compile(r".+\..+|.*\(.+\).*")

        return Storage((k, v) for k, v in get_vars.items()
                              if not regex_filter.match(k))

    # -------------------------------------------------------------------------
    @staticmethod
    def _limits(get_vars, default_limit=0):
        """
            Extract page limits (start and limit) from GET vars

            Args:
                get_vars: the GET vars
                default_limit: the default limit, explicit value or:
                                  0 => response.s3.ROWSPERPAGE
                                  None => no default limit

            Returns:
                a tuple (start, limit)
        """

        start = get_vars.get("start", None)
        limit = get_vars.get("limit", default_limit)

        # Deal with overrides (pagination limits come last)
        if isinstance(start, list):
            start = start[-1]
        if isinstance(limit, list):
            limit = limit[-1]

        if limit:
            # Ability to override default limit to "Show All"
            if isinstance(limit, str) and limit.lower() == "none":
                #start = None # needed?
                limit = None
            else:
                try:
                    start = int(start) if start is not None else None
                    limit = int(limit)
                except (ValueError, TypeError):
                    # Fall back to defaults
                    start, limit = None, default_limit

        else:
            # Use defaults, assume sspag because this is a
            # pagination request by definition
            start = None
            limit = default_limit

        return start, limit

# END =========================================================================

