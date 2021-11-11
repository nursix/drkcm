# -*- coding: utf-8 -*-

""" Sahana Eden GUI Layouts (HTML Renderers)

    @copyright: 2012-2021 (c) Sahana Software Foundation
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

    @todo: - complete layout implementations
           - render "selected" (flag in item)
"""

__all__ = ("S3MainMenuDefaultLayout",
           "MM",
           "S3PersonalMenuDefaultLayout",
           "MP",
           "S3AboutMenuDefaultLayout",
           "MA",
           "S3LanguageMenuDefaultLayout",
           "ML",
           "S3OrgMenuDefaultLayout",
           "OM",
           "S3OptionsMenuDefaultLayout",
           "M",
           "S3OAuthMenuDefaultLayout",
           "MOA",
           "S3MenuSeparatorDefaultLayout",
           "SEP",
           "S3BreadcrumbsLayout",
           "S3PopupLink",
           "S3AddResourceLink",
           "homepage",
           )

from gluon import current, URL, \
                  A, DIV, FORM, H3, IMG, INPUT, LABEL, LI, OPTION, SELECT, SPAN, TAG, UL

from core import S3NavigationItem, ICON, get_crud_string
from s3theme import NAV, SECTION

# =============================================================================
class S3MainMenuDefaultLayout(S3NavigationItem):
    """ Application Main Menu Layout """

    # Use the layout method of this class in templates/<theme>/layouts.py
    # if it is available at runtime (otherwise fallback to this layout):
    OVERRIDE = "S3MainMenuLayout"

    @staticmethod
    def layout(item):
        """ Custom Layout Method """

        # Manage flags: hide any disabled/unauthorized items
        if not item.authorized:
            item.enabled = False
            item.visible = False
        elif item.enabled is None or item.enabled:
            item.enabled = True
            item.visible = True

        if not item.enabled or not item.visible:
            return None

        items = item.render_components()
        if item.parent is not None:

            classes = []

            if item.parent.parent is None:
                # Item at the top-level?
                toplevel = True
                if item.opts.right:
                    classes.append("menu-right")
            else:
                toplevel = False

            if item.components:
                classes.append("has-dropdown not-click")
                if item.selected:
                    classes.append("active")
                _class = " ".join(classes)
                # Menu item with Dropdown
                if item.get_first(enabled=True, link=True):
                    _href = item.url()
                    return LI(A(item.label,
                                _href = _href,
                                _id = item.attr._id
                                ),
                                UL(items,
                                    _class = "dropdown"
                                    ),
                                _class = _class,
                                )
                else:
                    # No active items in drop-down
                    # => hide the entire entry
                    return None
            else:
                # Menu item without Drop-Down
                item_url = item.url()
                label = item.label
                if toplevel:
                    # Top-level item
                    if item_url == URL(c="default", f="index"):
                        classes.append("menu-home")
                    if item.selected:
                        classes.append("active")
                    _class = " ".join(classes)
                else:
                    # Submenu item
                    if isinstance(label, dict):
                        if "name" in label:
                            label = label["name"]
                        else:
                            return None
                    _class = None
                link_class = "s3_modal" if item.opts.modal else None
                return LI(A(label,
                            _class = link_class,
                            _href = item_url,
                            _id = item.attr._id,
                            ),
                            _class = _class,
                            )
        else:
            # Main menu
            right = []
            left = []
            for child in items:
                if "menu-right" in child["_class"]:
                    child.remove_class("menu-right")
                    right.append(child)
                else:
                    left.append(child)
            right.reverse()
            if current.response.s3.rtl:
                right, left = left, right

            T = current.T
            data_options = {"back": T("Back"),
                            }

            return NAV(UL(LI(A(" ",
                                _href = URL(c="default", f="index"),
                                ),
                                _class = "name"
                                ),
                            LI(A(SPAN(current.T("Menu"))),
                                _class = "toggle-topbar menu-icon",
                                ),
                            _class = "title-area",
                            ),
                        SECTION(UL(right, _class="right"),
                                UL(left, _class="left"),
                                _class = "top-bar-section",
                                ),
                        _class = "top-bar",
                        data = {"topbar": " ",
                                "options": "back_text:%(back)s" % data_options,
                                },
                        )

    # ---------------------------------------------------------------------
    @staticmethod
    def checkbox_item(item):
        """ Render special active items """

        name = item.label
        link = item.url()
        _id = name["id"]
        if "name" in name:
            _name = name["name"]
        else:
            _name = ""
        if "value" in name:
            _value = name["value"]
        else:
            _value = False
        if "request_type" in name:
            _request_type = name["request_type"]
        else:
            _request_type = "ajax"
        if link:
            if _request_type == "ajax":
                _onchange='''var val=$('#%s:checked').length;$.getS3('%s'+'?val='+val,null,false,null,false,false)''' % \
                          (_id, link)
            else:
                # Just load the page. Use this if the changed menu
                # item should alter the contents of the page, and
                # it's simpler just to load it.
                _onchange="location.href='%s'" % link
        else:
            _onchange=None
        return LI(A(INPUT(_type="checkbox",
                          _id=_id,
                          _onchange=_onchange,
                          value=_value,
                          ),
                    "%s" % _name,
                    _nowrap="nowrap",
                    ),
                  _class="menu-toggle",
                  )

# =============================================================================
class S3PersonalMenuDefaultLayout(S3NavigationItem):

    OVERRIDE = "S3PersonalMenuLayout"

    @staticmethod
    def layout(item):

        if item.parent is None:
            # The menu
            items = item.render_components()
            if items:
                return TAG["ul"](items, _class="sub-nav personal-menu")
            else:
                return "" # menu is empty
        else:
            # A menu item
            if item.enabled and item.authorized:
                return TAG["li"](A(item.label, _href=item.url()))
            else:
                return None

# -----------------------------------------------------------------------------
# Shortcut
MP = S3PersonalMenuDefaultLayout

# =============================================================================
class S3AboutMenuDefaultLayout(S3NavigationItem):

    OVERRIDE = "S3AboutMenuLayout"

    @staticmethod
    def layout(item):

        if item.parent is None:
            # The menu
            items = item.render_components()
            if items:
                return TAG["ul"](items, _class="sub-nav about-menu")
            else:
                return "" # menu is empty
        else:
            # A menu item
            if item.enabled and item.authorized:
                return TAG["li"](A(item.label, _href=item.url()))
            else:
                return None

# -----------------------------------------------------------------------------
# Shortcut
MA = S3AboutMenuDefaultLayout

# =============================================================================
class S3LanguageMenuDefaultLayout(S3NavigationItem):

    OVERRIDE = "S3LanguageMenuLayout"

    @staticmethod
    def layout(item):
        """ Language menu layout

            options for each entry:
                - lang_code: the language code
                - lang_name: the language name
            option for the menu
                - current_language: code of the current language
        """

        if item.enabled:
            if item.components:
                # The language menu itself
                current_language = current.T.accepted_language
                items = item.render_components()
                select = SELECT(items,
                                value = current_language,
                                _name = "_language",
                                # @ToDo T:
                                _title = "Language Selection",
                                _onchange = "S3.reloadWithQueryStringVars({'_language':$(this).val()});",
                                )
                form = FORM(select,
                            _class = "language-selector",
                            _name = "_language",
                            _action = "",
                            _method = "get",
                            )
                return form
            else:
                # A language entry
                return OPTION(item.opts.lang_name,
                              _value = item.opts.lang_code,
                              )
        else:
            return None

    # -------------------------------------------------------------------------
    def check_enabled(self):
        """ Check whether the language menu is enabled """

        return bool(current.deployment_settings.get_L10n_display_toolbar())

# -----------------------------------------------------------------------------
# Shortcut
ML = S3LanguageMenuDefaultLayout

# =============================================================================
class S3OrgMenuDefaultLayout(S3NavigationItem):
    """ Layout for the organisation-specific menu """

    OVERRIDE = "S3OrgMenuLayout"

    @staticmethod
    def layout(item):

        name = "Humanitarian Management System"

        current_user = current.auth.user
        if current_user:
            user_org_id = current_user.organisation_id
            if user_org_id:
                otable = current.s3db.org_organisation
                query = (otable.id == user_org_id) & \
                        (otable.deleted == False)
                row = current.db(query).select(otable.name,
                                               limitby = (0, 1),
                                               ).first()
                if row:
                    name = row.name

        logo = IMG(_src = "/%s/static/img/eden_asp_large.png" %
                          current.request.application,
                   _alt = name,
                   _width=38,
                   )

        # Note: render using current.menu.org.render()[0] + current.menu.org.render()[1]
        return (name, logo)

# -----------------------------------------------------------------------------
# Shortcut
OM = S3OrgMenuDefaultLayout

# =============================================================================
class S3OptionsMenuDefaultLayout(S3NavigationItem):
    """ Controller Options Menu Layout """

    # Use the layout method of this class in templates/<theme>/layouts.py
    # if it is available at runtime (otherwise fallback to this layout):
    OVERRIDE = "S3OptionsMenuLayout"

    @staticmethod
    def layout(item):
        """ Layout Method (Item Renderer) """

        # Manage flags: hide any disabled/unauthorized items
        if not item.authorized:
            enabled = False
            visible = False
        elif item.enabled is None or item.enabled:
            enabled = True
            visible = True

        if enabled and visible:
            if item.parent is not None:
                if item.enabled and item.authorized:

                    attr = {"_id": item.attr._id}
                    if item.attr._onclick:
                        attr["_onclick"] = item.attr._onclick
                    else:
                        attr["_href"] = item.url()

                    if item.components:
                        # Submenu
                        items = item.render_components()

                        # Hide submenus which have no active links
                        if not items and not item.link:
                            return None

                        _class = ""
                        if item.parent.parent is None and item.selected:
                            _class = "active"

                        section = [LI(A(item.label,
                                        **attr
                                        ),
                                      _class="heading %s" % _class,
                                      ),
                                   ]

                        if items:
                            section.append(UL(items))
                        return section

                    else:
                        # Submenu item
                        if item.parent.parent is None:
                            _class = "heading"
                        else:
                            _class = ""

                        return LI(A(item.label,
                                    **attr
                                    ),
                                  _class=_class,
                                  )
            else:
                # Main menu
                items = item.render_components()
                return DIV(NAV(UL(items, _id="main-sub-menu", _class="side-nav")), _class="sidebar")

        else:
            return None

# =============================================================================
class S3OAuthMenuDefaultLayout(S3NavigationItem):
    """ OAuth Menu Layout """

    # Use the layout method of this class in templates/<theme>/layouts.py
    # if it is available at runtime (otherwise fallback to this layout):
    OVERRIDE = "S3OAuthMenuLayout"

    @staticmethod
    def layout(item):
        """ Layout Method (Item Renderer) """

        if item.enabled:
            if item.parent is not None:
                output = A(SPAN(item.label),
                        _class = "zocial %s" % item.opts.api,
                        _href = item.url(),
                        _title = item.opts.get("title", item.label),
                        )
            else:
                items = item.render_components()
                if items:
                    output = DIV(items, _class="zocial-login")
                else:
                    # Hide if empty
                    output = None
        else:
            # Hide if disabled
            output = None

        return output

# =============================================================================
class S3MenuSeparatorDefaultLayout(S3NavigationItem):
    """ Simple menu separator """

    # Use the layout method of this class in templates/<theme>/layouts.py
    # if it is available at runtime (otherwise fallback to this layout):
    OVERRIDE = "S3MenuSeparatorLayout"

    @staticmethod
    def layout(item):
        """ Layout Method (Item Renderer) """

        if item.parent is not None:
            return LI(_class="divider hide-for-small")
        else:
            return None

# =============================================================================
# Import menu layouts from template (if present)
#
MM = S3MainMenuDefaultLayout
M = S3OptionsMenuDefaultLayout
MOA = S3OAuthMenuDefaultLayout
SEP = S3MenuSeparatorDefaultLayout

# =============================================================================
class S3BreadcrumbsLayout(S3NavigationItem):
    """ Breadcrumbs layout """

    @staticmethod
    def layout(item):

        if item.parent is None:
            items = item.render_components()
            return DIV(UL(items), _class='breadcrumbs')
        else:
            if item.is_last():
                _class = "highlight"
            else:
                _class = "ancestor"
            return LI(A(item.label, _href=item.url(), _class=_class))

# =============================================================================
class S3HomepageMenuLayout(S3NavigationItem):
    """
        Layout for homepage menus
    """

    @staticmethod
    def layout(item):
        """ Layout Method (Item Renderer) """

        # Manage flags: hide any disabled/unauthorized items
        if not item.authorized and not item.opts.always_display:
            item.enabled = False
            item.visible = False
        elif item.enabled is None or item.enabled:
            item.enabled = True
            item.visible = True

        if item.enabled and item.visible:
            items = item.render_components()

            if item.parent is None:
                # The menu itself

                number_of_links = 0

                components = []
                append = components.append
                for submenu in items:
                    append(submenu)
                    number_of_links += len(submenu.elements("a"))

                # Hide the entire menu if it doesn't contain any links
                if not number_of_links:
                    return None

                title = H3(item.label) if item.label else ""
                menu = DIV(title,
                           DIV(TAG[""](components),
                               _class = "icon-bar four-up",
                               ),
                           _id = item.attr._id,
                           _class = item.attr._class,
                           )

                return menu

            else:
                # A menu item
                _class = item.attr._class
                if _class:
                    _class = "%s item" % _class
                else:
                    _class = "item"
                _id = item.attr._id

                icon = item.opts.icon
                if icon:
                    label = LABEL(ICON(icon), item.label)
                else:
                    label = LABEL(item.label)
                return A(label,
                         _class = _class,
                         _href = item.url(),
                         _id = _id,
                         )
        else:
            return None

# =============================================================================
class S3PopupLink(S3NavigationItem):
    """
        Links in form fields comments to show a form for adding
        a new foreign key record.
    """

    def __init__(self,
                 label = None,
                 c = None,
                 f = None,
                 t = None,
                 m = "create",
                 args = None,
                 vars = None,
                 info = None,
                 title = None,
                 tooltip = None,
                 ):
        """
            Constructor

            @param c: the target controller
            @param f: the target function
            @param t: the target table (defaults to c_f)
            @param m: the URL method (will be appended to args)
            @param args: the argument list
            @param vars: the request vars (format="popup" will be added automatically)
            @param label: the link label (falls back to label_create)
            @param info: hover-title for the label
            @param title: the tooltip title
            @param tooltip: the tooltip text
        """

        if label is None:
            label = title
        if info is None:
            info = title

        if c is None:
            # Fall back to current controller
            c = current.request.controller

        if label is None:
            if t is None:
                t = "%s_%s" % (c, f)
            if m == "create":
                # Fall back to label_create
                label = get_crud_string(t, "label_create")
            elif m == "update":
                # Fall back to label_update
                label = get_crud_string(t, "label_update")

        super(S3PopupLink, self).__init__(label,
                                          c=c, f=f, t=t,
                                          m=m,
                                          args=args,
                                          vars=vars,
                                          info=info,
                                          title=title,
                                          tooltip=tooltip,
                                          mandatory=True)

    # -------------------------------------------------------------------------
    @staticmethod
    def layout(item):
        """ Layout for popup link """

        if not item.authorized:
            return None

        if current.deployment_settings.get_ui_use_button_icons():
            label = (ICON("add"), item.label)
        else:
            label = item.label

        popup_link = A(label,
                       _href = item.url(format="popup"),
                       _class = "s3_add_resource_link",
                       _id = "%s_add" % item.function,
                       _target = "top",
                       _title = item.opts.info,
                       )

        tooltip = item.opts.tooltip
        if tooltip is not None:
            ttip = DIV(_class = "tooltip",
                       _title = "%s|%s" % (item.opts.title, tooltip))
        else:
            ttip = ""

        return TAG[""](popup_link, ttip)

    # -------------------------------------------------------------------------
    @staticmethod
    def inline(item):
        """ Render this link for an inline component """

        if not item.authorized:
            return None

        popup_link = A(item.label,
                       _href = item.url(format="popup"),
                       _class = "s3_add_resource_link action-lnk",
                       _id = "%s_%s_add" % (item.vars["caller"], item.function),
                       _target = "top",
                       _title = item.opts.info,
                       )

        return DIV(popup_link, _class="s3_inline_add_resource_link")

# =============================================================================
# Maintained for backward compatibility
#
S3AddResourceLink = S3PopupLink

# =============================================================================
def homepage(module=None, *match, **attr):
    """
        Shortcut for module homepage menu items using the MM layout,
        retrieves the module's nice name.

        @param module: the module's prefix (controller)
        @param match: additional prefixes
        @param attr: attributes for the navigation item
    """

    settings = current.deployment_settings
    all_modules = settings.modules

    layout = S3MainMenuDefaultLayout
    c = [module] + list(match)

    if "name" in attr:
        name = attr["name"]
        attr.pop("name")
    else:
        if module is None:
            module = "default"
        if module in all_modules:
            m = all_modules[module]
            name = m.name_nice
        else:
            name = module

    if "f" in attr:
        f = attr["f"]
        del attr["f"]
    else:
        f = "index"

    return layout(name, c=c, f=f, **attr)

# END =========================================================================
