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

__all__ = ("FieldS3",
           "S3ReusableField",
           "S3MetaFields",
           "s3_fieldmethod",
           "s3_meta_fields",
           "s3_all_meta_field_names",
           "s3_role_required",
           "s3_roles_permitted",
           "s3_comments",
           "s3_currency",
           "s3_language",
           "s3_date",
           "s3_datetime",
           "s3_time",
           )

import datetime
from uuid import uuid4

from gluon import current, DIV, Field, IS_EMPTY_OR, IS_IN_SET, IS_TIME, TAG, XML
from gluon.sqlhtml import TimeWidget
from gluon.storage import Storage

from s3dal import SQLCustomType

from ..tools import S3DateTime, s3_str, IS_ISO639_2_LANGUAGE_CODE, \
                    IS_ONE_OF, IS_UTC_DATE, IS_UTC_DATETIME, S3Represent
from ..ui import S3ScriptItem, S3CalendarWidget, S3DateWidget

# =============================================================================
class FieldS3(Field):
    """
        S3 extensions of the gluon.sql.Field class
            - add "sortby" attribute (used by IS_ONE_OF)

        @ToDo: Deprecate now that Field supports this natively via **others
    """

    def __init__(self, fieldname,
                 type = "string",
                 length = None,
                 default = None,
                 required = False,
                 requires = "<default>",
                 ondelete = "CASCADE",
                 notnull = False,
                 unique = False,
                 uploadfield = True,
                 widget = None,
                 label = None,
                 comment = None,
                 writable = True,
                 readable = True,
                 update = None,
                 authorize = None,
                 autodelete = False,
                 represent = None,
                 uploadfolder = None,
                 compute = None,
                 sortby = None):

        self.sortby = sortby

        Field.__init__(self,
                       fieldname,
                       type = type,
                       length = length,
                       default = default,
                       required = required,
                       requires = requires,
                       ondelete = ondelete,
                       notnull = notnull,
                       unique = unique,
                       uploadfield = uploadfield,
                       widget = widget,
                       label = label,
                       comment = comment,
                       writable = writable,
                       readable = readable,
                       update = update,
                       authorize = authorize,
                       autodelete = autodelete,
                       represent = represent,
                       uploadfolder = uploadfolder,
                       compute = compute,
                       )

# =============================================================================
def s3_fieldmethod(name, f, represent=None, search_field=None):
    """
        Helper to attach a representation method to a Field.Method.

        @param name: the field name
        @param f: the field method
        @param represent: the representation function
        @param search_field: the field to use for searches
               - only used by datatable_filter currently
               - can only be a single field in the same table currently
    """

    if represent is None and search_field is None:
        fieldmethod = Field.Method(name, f)

    else:
        class Handler(object):
            def __init__(self, method, row):
                self.method=method
                self.row=row
            def __call__(self, *args, **kwargs):
                return self.method(self.row, *args, **kwargs)

        if represent is not None:
            if hasattr(represent, "bulk"):
                Handler.represent = represent
            else:
                Handler.represent = staticmethod(represent)

        if search_field is not None:
            Handler.search_field = search_field

        fieldmethod = Field.Method(name, f, handler=Handler)

    return fieldmethod

# =============================================================================
class S3ReusableField(object):
    """
        DRY Helper for reusable fields:

        This creates neither a Table nor a Field, but just
        an argument store. The field is created with the __call__
        method, which is faster than copying an existing field.
    """

    def __init__(self, name, type="string", **attr):

        self.name = name
        self.__type = type
        self.attr = Storage(attr)

    # -------------------------------------------------------------------------
    def __call__(self, name=None, **attr):

        if not name:
            name = self.name

        ia = dict(self.attr)

        DEFAULT = "default"
        widgets = ia.pop("widgets", {})

        if attr:
            empty = attr.pop("empty", True)
            if not empty:
                requires = ia.get("requires")
                if requires:
                    if not isinstance(requires, (list, tuple)):
                        requires = [requires]
                    if requires:
                        r = requires[0]
                        if isinstance(r, IS_EMPTY_OR):
                            requires = r.other
                            ia["requires"] = requires
            widget = attr.pop("widget", DEFAULT)
            ia.update(**attr)
        else:
            widget = DEFAULT

        if isinstance(widget, str):
            if widget == DEFAULT and "widget" in ia:
                widget = ia["widget"]
            else:
                if not isinstance(widgets, dict):
                    widgets = {DEFAULT: widgets}
                if widget != DEFAULT and widget not in widgets:
                    raise NameError("Undefined widget: %s" % widget)
                else:
                    widget = widgets.get(widget)
        ia["widget"] = widget

        script = ia.pop("script", None)
        if script:
            comment = ia.get("comment")
            if comment:
                ia["comment"] = TAG[""](comment,
                                        S3ScriptItem(script=script),
                                        )
            else:
                ia["comment"] = S3ScriptItem(script=script)

        if ia.get("sortby") is not None:
            return FieldS3(name, self.__type, **ia)
        else:
            return Field(name, self.__type, **ia)

    # -------------------------------------------------------------------------
    @staticmethod
    def dummy(fname="dummy_id", ftype="integer"):
        """
            Provide a dummy reusable field; for safe defaults in models

            @param fname: the dummy field name
            @param ftype: the dummy field type

            @returns: a lambda with the same signature as a reusable field
        """

        return lambda name=fname, **attr: Field(name,
                                                ftype,
                                                readable = False,
                                                writable = False,
                                                )

# =============================================================================
# Meta-fields
#
# Use URNs according to http://tools.ietf.org/html/rfc4122
s3uuid = SQLCustomType(type = "string",
                       native = "VARCHAR(128)",
                       encoder = lambda x: \
                                 "%s" % (uuid4().urn if x == "" else s3_str(x)),
                       decoder = lambda x: x,
                       )

# Representation of user roles (auth_group)
auth_group_represent = S3Represent(lookup="auth_group", fields=["role"])

ALL_META_FIELD_NAMES = ("uuid",
                        "mci",
                        "deleted",
                        "deleted_fk",
                        "deleted_rb",
                        "created_on",
                        "created_by",
                        "modified_on",
                        "modified_by",
                        "approved_by",
                        "owned_by_user",
                        "owned_by_group",
                        "realm_entity",
                        )

# -----------------------------------------------------------------------------
class S3MetaFields(object):
    """ Class to standardize meta-fields """

    # -------------------------------------------------------------------------
    @staticmethod
    def uuid():
        """
            Universally unique record identifier according to RFC4122, as URN
            (e.g. "urn:uuid:fd8f97ab-1252-4d62-9982-8e3f3025307f"); uuids are
            mandatory for synchronization (incl. EdenMobile)
        """

        return Field("uuid", type=s3uuid,
                     default = "",
                     length = 128,
                     notnull = True,
                     unique = True,
                     readable = False,
                     writable = False,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def mci():
        """
            Master-Copy-Index - whether this record has been created locally
            or imported ("copied") from another source:
                - mci=0 means "created here"
                - mci>0 means "copied n times"
        """

        return Field("mci", "integer",
                     default = 0,
                     readable = False,
                     writable = False,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def deleted():
        """
            Deletion status (True=record is deleted)
        """

        return Field("deleted", "boolean",
                     default = False,
                     readable = False,
                     writable = False,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def deleted_fk():
        """
            Foreign key values of this record before deletion (foreign keys
            are set to None during deletion to derestrict constraints)
        """

        return Field("deleted_fk", #"text",
                     readable = False,
                     writable = False,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def deleted_rb():
        """
            De-duplication: ID of the record that has replaced this record
        """

        return Field("deleted_rb", "integer",
                     readable = False,
                     writable = False,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def created_on():
        """
            Date/time when the record was created
        """

        return Field("created_on", "datetime",
                     readable = False,
                     writable = False,
                     default = datetime.datetime.utcnow,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def modified_on():
        """
            Date/time when the record was last modified
        """

        return Field("modified_on", "datetime",
                     readable = False,
                     writable = False,
                     default = datetime.datetime.utcnow,
                     update = datetime.datetime.utcnow,
                     )

    # -------------------------------------------------------------------------
    @classmethod
    def created_by(cls):
        """
            Auth_user ID of the user who created the record
        """

        return Field("created_by", current.auth.settings.table_user,
                     readable = False,
                     writable = False,
                     requires = None,
                     default = cls._current_user(),
                     represent = cls._represent_user(),
                     ondelete = "RESTRICT",
                     )

    # -------------------------------------------------------------------------
    @classmethod
    def modified_by(cls):
        """
            Auth_user ID of the last user who modified the record
        """

        current_user = cls._current_user()
        return Field("modified_by", current.auth.settings.table_user,
                     readable = False,
                     writable = False,
                     requires = None,
                     default = current_user,
                     update = current_user,
                     represent = cls._represent_user(),
                     ondelete = "RESTRICT",
                     )

    # -------------------------------------------------------------------------
    @classmethod
    def approved_by(cls):
        """
            Auth_user ID of the user who has approved the record:
                - None means unapproved
                - 0 means auto-approved
        """

        return Field("approved_by", "integer",
                     readable = False,
                     writable = False,
                     requires = None,
                     represent = cls._represent_user(),
                     )

    # -------------------------------------------------------------------------
    @classmethod
    def owned_by_user(cls):
        """
            Auth_user ID of the user owning the record
        """

        return Field("owned_by_user", current.auth.settings.table_user,
                     readable = False,
                     writable = False,
                     requires = None,
                     default = cls._current_user(),
                     represent = cls._represent_user(),
                     ondelete = "RESTRICT",
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def owned_by_group():
        """
            Auth_group ID of the user role owning the record
        """

        return Field("owned_by_group", "integer",
                     default = None,
                     readable = False,
                     writable = False,
                     requires = None,
                     represent = auth_group_represent,
                     )

    # -------------------------------------------------------------------------
    @staticmethod
    def realm_entity():
        """
            PE ID of the entity managing the record
        """

        return Field("realm_entity", "integer",
                     default = None,
                     readable = False,
                     writable = False,
                     requires = None,
                     # using a lambda here as we don't want the model
                     # to be loaded yet:
                     represent = lambda pe_id: \
                                 current.s3db.pr_pentity_represent(pe_id),
                     )

    # -------------------------------------------------------------------------
    @classmethod
    def all_meta_fields(cls):
        """
            Standard meta fields for all tables

            @return: tuple of Fields
        """

        return (cls.uuid(),
                cls.mci(),
                cls.deleted(),
                cls.deleted_fk(),
                cls.deleted_rb(),
                cls.created_on(),
                cls.created_by(),
                cls.modified_on(),
                cls.modified_by(),
                cls.approved_by(),
                cls.owned_by_user(),
                cls.owned_by_group(),
                cls.realm_entity(),
                )

    # -------------------------------------------------------------------------
    @classmethod
    def sync_meta_fields(cls):
        """
            Meta-fields required for sync

            @return: tuple of Fields
        """

        return (cls.uuid(),
                cls.mci(),
                cls.deleted(),
                cls.deleted_fk(),
                cls.deleted_rb(),
                cls.created_on(),
                cls.modified_on(),
                )

    # -------------------------------------------------------------------------
    @classmethod
    def owner_meta_fields(cls):
        """
            Record ownership meta-fields

            @return: tuple of Fields
        """

        return (cls.owned_by_user(),
                cls.owned_by_group(),
                cls.realm_entity(),
                )

    # -------------------------------------------------------------------------
    @classmethod
    def timestamps(cls):
        """
            Timestamp meta-fields

            @return: tuple of Fields
        """

        return (cls.created_on(),
                cls.modified_on(),
                )

    # -------------------------------------------------------------------------
    @staticmethod
    def _current_user():
        """
            Get the user ID of the currently logged-in user

            @return: auth_user ID
        """

        if current.auth.is_logged_in():
            # Not current.auth.user to support impersonation
            return current.session.auth.user.id
        else:
            return None

    # -------------------------------------------------------------------------
    @staticmethod
    def _represent_user():
        """
            Representation method for auth_user IDs

            @return: representation function
        """

        return current.auth.user_represent

# -----------------------------------------------------------------------------
def s3_meta_fields():
    """
        Shortcut commonly used in table definitions: *s3_meta_fields()

        @return: tuple of Field instances
    """

    return S3MetaFields.all_meta_fields()

def s3_all_meta_field_names():
    """
        Shortcut commonly used to include/exclude meta fields

        @return: tuple of field names
    """

    return ALL_META_FIELD_NAMES

# =============================================================================
# Reusable roles fields

def s3_role_required():
    """
        Role Required to access a resource
        - used by GIS for map layer permissions management
    """

    T = current.T
    gtable = current.auth.settings.table_group
    represent = S3Represent(lookup="auth_group", fields=["role"])
    return FieldS3("role_required", gtable,
                   sortby="role",
                   requires = IS_EMPTY_OR(
                                IS_ONE_OF(current.db, "auth_group.id",
                                          represent,
                                          zero=T("Public"))),
                   #widget = S3AutocompleteWidget("admin",
                   #                              "group",
                   #                              fieldname="role"),
                   represent = represent,
                   label = T("Role Required"),
                   comment = DIV(_class="tooltip",
                                 _title="%s|%s" % (T("Role Required"),
                                                   T("If this record should be restricted then select which role is required to access the record here."),
                                                   ),
                                 ),
                   ondelete = "RESTRICT",
                   )

# -----------------------------------------------------------------------------
def s3_roles_permitted(name="roles_permitted", **attr):
    """
        List of Roles Permitted to access a resource
        - used by CMS
    """

    T = current.T
    represent = S3Represent(lookup="auth_group", fields=["role"])
    if "label" not in attr:
        attr["label"] = T("Roles Permitted")
    if "sortby" not in attr:
        attr["sortby"] = "role"
    if "represent" not in attr:
        attr["represent"] = represent
    if "requires" not in attr:
        attr["requires"] = IS_EMPTY_OR(IS_ONE_OF(current.db,
                                                 "auth_group.id",
                                                 represent,
                                                 multiple=True))
    if "comment" not in attr:
        attr["comment"] = DIV(_class="tooltip",
                              _title="%s|%s" % (T("Roles Permitted"),
                                                T("If this record should be restricted then select which role(s) are permitted to access the record here.")))
    if "ondelete" not in attr:
        attr["ondelete"] = "RESTRICT"

    return FieldS3(name, "list:reference auth_group", **attr)

# =============================================================================
def s3_comments(name="comments", **attr):
    """
        Return a standard Comments field
    """

    T = current.T
    if "label" not in attr:
        attr["label"] = T("Comments")
    if "represent" not in attr:
        # Support HTML markup
        attr["represent"] = lambda comments: \
            XML(comments) if comments else current.messages["NONE"]
    if "widget" not in attr:
        from ..ui import s3_comments_widget
        _placeholder = attr.pop("_placeholder", None)
        if _placeholder:
            attr["widget"] = lambda f, v: \
                s3_comments_widget(f, v, _placeholder=_placeholder)
        else:
            attr["widget"] = s3_comments_widget
    if "comment" not in attr:
        attr["comment"] = DIV(_class="tooltip",
                              _title="%s|%s" % \
            (T("Comments"),
             T("Please use this field to record any additional information, including a history of the record if it is updated.")))

    return Field(name, "text", **attr)

# =============================================================================
def s3_currency(name="currency", **attr):
    """
        Return a standard Currency field

        @ToDo: Move to a Finance module?
    """

    settings = current.deployment_settings

    if "label" not in attr:
        attr["label"] = current.T("Currency")
    if "default" not in attr:
        attr["default"] = settings.get_fin_currency_default()
    if "requires" not in attr:
        currency_opts = settings.get_fin_currencies()
        attr["requires"] = IS_IN_SET(list(currency_opts.keys()),
                                     zero=None)
    if "writable" not in attr:
        attr["writable"] = settings.get_fin_currency_writable()

    return Field(name, length=3, **attr)

# =============================================================================
def s3_language(name="language", **attr):
    """
        Return a standard Language field

        @param name: the Field name
        @param attr: Field parameters, as well as keywords:
            @keyword empty: allow the field to remain empty:
                            None: accept empty, don't show empty-option (default)
                            True: accept empty, show empty-option
                            False: reject empty, don't show empty-option
                            (keyword ignored if a "requires" is passed)
            @keyword translate: translate the language names into
                                current UI language (not recommended for
                                selector to choose that UI language)

            @keyword select: which languages to show in the selector:
                             - a dict of {lang_code: lang_name}
                             - None to expose all languages
                             - False (or omit) to use L10n_languages setting (default)
    """

    if "label" not in attr:
        attr["label"] = current.T("Language")
    if "default" not in attr:
        attr["default"] = current.deployment_settings.get_L10n_default_language()

    empty = attr.pop("empty", None)
    zero = "" if empty else None

    translate = attr.pop("translate", True)

    if "select" in attr:
        # If select is present => pass as-is
        requires = IS_ISO639_2_LANGUAGE_CODE(select = attr.pop("select"),
                                             sort = True,
                                             translate = translate,
                                             zero = zero,
                                             )
    else:
        # Use L10n_languages deployment setting
        requires = IS_ISO639_2_LANGUAGE_CODE(sort = True,
                                             translate = translate,
                                             zero = zero,
                                             )

    if "requires" not in attr:
        # Value required only if empty is explicitly False
        if empty is False:
            attr["requires"] = requires
        else:
            attr["requires"] = IS_EMPTY_OR(requires)

    if "represent" not in attr:
        attr["represent"] = requires.represent

    return Field(name, length=8, **attr)

# =============================================================================
def s3_date(name="date", **attr):
    """
        Return a standard date-field

        @param name: the field name

        @keyword default: the field default, can be specified as "now" for
                          current date, or as Python date
        @keyword past: number of selectable past months
        @keyword future: number of selectable future months
        @keyword widget: the form widget for the field, can be specified
                         as "date" for S3DateWidget, "calendar" for
                         S3CalendarWidget, or as a web2py FormWidget,
                         defaults to "calendar"
        @keyword calendar: the calendar to use for this widget, defaults
                           to current.calendar
        @keyword start_field: CSS selector for the start field for interval
                              selection
        @keyword default_interval: the default interval
        @keyword default_explicit: whether the user must click the field
                                   to set the default, or whether it will
                                   automatically be set when the value for
                                   start_field is set
        @keyword set_min: CSS selector for another date/time widget to
                          dynamically set the minimum selectable date/time to
                          the value selected in this widget
        @keyword set_max: CSS selector for another date/time widget to
                          dynamically set the maximum selectable date/time to
                          the value selected in this widget
        @keyword month_selector: allow direct selection of month

        @note: other S3ReusableField keywords are also supported (in addition
               to the above)

        @note: calendar-option requires widget="calendar" (default), otherwise
               Gregorian calendar is enforced for the field

        @note: set_min/set_max only supported for widget="calendar" (default)

        @note: interval options currently not supported by S3CalendarWidget,
               only available with widget="date"
        @note: start_field and default_interval should be given together

        @note: sets a default field label "Date" => use label-keyword to
               override if necessary
        @note: sets a default validator IS_UTC_DATE => use requires-keyword
               to override if necessary
        @note: sets a default representation S3DateTime.date_represent => use
               represent-keyword to override if necessary

        @ToDo: Different default field name in case we need to start supporting
               Oracle, where 'date' is a reserved word
    """

    attributes = dict(attr)

    # Calendar
    calendar = attributes.pop("calendar", None)

    # Past and future options
    past = attributes.pop("past", None)
    future = attributes.pop("future", None)

    # Label
    if "label" not in attributes:
        attributes["label"] = current.T("Date")

    # Widget-specific options (=not intended for S3ReusableField)
    WIDGET_OPTIONS = ("start_field",
                      "default_interval",
                      "default_explicit",
                      "set_min",
                      "set_max",
                      "month_selector",
                      )

    # Widget
    widget = attributes.get("widget", "calendar")
    widget_options = {}
    if widget == "date":
        # Legacy: S3DateWidget
        # @todo: deprecate (once S3CalendarWidget supports all legacy options)

        # Must use Gregorian calendar
        calendar = "Gregorian"

        # Past/future options
        if past is not None:
            widget_options["past"] = past
        if future is not None:
            widget_options["future"] = future

        # Supported additional widget options
        SUPPORTED_OPTIONS = ("start_field",
                             "default_interval",
                             "default_explicit",
                             )
        for option in WIDGET_OPTIONS:
            if option in attributes:
                if option in SUPPORTED_OPTIONS:
                    widget_options[option] = attributes[option]
                del attributes[option]

        widget = S3DateWidget(**widget_options)

    elif widget == "calendar":

        # Default: calendar widget
        widget_options["calendar"] = calendar

        # Past/future options
        if past is not None:
            widget_options["past_months"] = past
        if future is not None:
            widget_options["future_months"] = future

        # Supported additional widget options
        SUPPORTED_OPTIONS = ("set_min",
                             "set_max",
                             "month_selector",
                             )
        for option in WIDGET_OPTIONS:
            if option in attributes:
                if option in SUPPORTED_OPTIONS:
                    widget_options[option] = attributes[option]
                del attributes[option]

        widget = S3CalendarWidget(**widget_options)

    else:
        # Drop all widget options
        for option in WIDGET_OPTIONS:
            attributes.pop(option, None)

    attributes["widget"] = widget

    # Default value
    now = current.request.utcnow.date()
    if attributes.get("default") == "now":
        attributes["default"] = now

    # Representation
    if "represent" not in attributes:
        attributes["represent"] = lambda dt: \
                                  S3DateTime.date_represent(dt,
                                                            utc=True,
                                                            calendar=calendar,
                                                            )

    # Validator
    if "requires" not in attributes:

        if past is None and future is None:
            requires = IS_UTC_DATE(calendar=calendar)
        else:
            from dateutil.relativedelta import relativedelta
            minimum = maximum = None
            if past is not None:
                minimum = now - relativedelta(months = past)
            if future is not None:
                maximum = now + relativedelta(months = future)
            requires = IS_UTC_DATE(calendar=calendar,
                                   minimum=minimum,
                                   maximum=maximum,
                                   )

        empty = attributes.pop("empty", None)
        if empty is False:
            attributes["requires"] = requires
        else:
            # Default
            attributes["requires"] = IS_EMPTY_OR(requires)

    return Field(name, "date", **attributes)

# =============================================================================
def s3_datetime(name="date", **attr):
    """
        Return a standard datetime field

        @param name: the field name

        @keyword default: the field default, can be specified as "now" for
                          current date/time, or as Python date

        @keyword past: number of selectable past hours
        @keyword future: number of selectable future hours

        @keyword widget: form widget option, can be specified as "date"
                         for date-only, or "datetime" for date+time (default),
                         or as a web2py FormWidget
        @keyword calendar: the calendar to use for this field, defaults
                           to current.calendar
        @keyword set_min: CSS selector for another date/time widget to
                          dynamically set the minimum selectable date/time to
                          the value selected in this widget
        @keyword set_max: CSS selector for another date/time widget to
                          dynamically set the maximum selectable date/time to
                          the value selected in this widget

        @note: other S3ReusableField keywords are also supported (in addition
               to the above)

        @note: sets a default field label "Date" => use label-keyword to
               override if necessary
        @note: sets a default validator IS_UTC_DATE/IS_UTC_DATETIME => use
               requires-keyword to override if necessary
        @note: sets a default representation S3DateTime.date_represent or
               S3DateTime.datetime_represent respectively => use the
               represent-keyword to override if necessary

        @ToDo: Different default field name in case we need to start supporting
               Oracle, where 'date' is a reserved word
    """

    attributes = dict(attr)

    # Calendar
    calendar = attributes.pop("calendar", None)

    # Limits
    limits = {}
    for keyword in ("past", "future", "min", "max"):
        if keyword in attributes:
            limits[keyword] = attributes[keyword]
            del attributes[keyword]

    # Compute earliest/latest
    widget = attributes.pop("widget", None)
    now = current.request.utcnow
    if widget == "date":
        # Helper function to convert past/future hours into
        # earliest/latest datetime, retaining day of month and
        # time of day
        def limit(delta):
            current_month = now.month
            years, hours = divmod(-delta, 8760)
            months = divmod(hours, 744)[0]
            if months > current_month:
                years += 1
            month = divmod((current_month - months) + 12, 12)[1]
            year = now.year - years
            return now.replace(month=month, year=year)

        earliest = limits.get("min")
        if not earliest:
            past = limits.get("past")
            if past is not None:
                earliest = limit(-past)
        latest = limits.get("max")
        if not latest:
            future = limits.get("future")
            if future is not None:
                latest = limit(future)
    else:
        # Compute earliest/latest
        earliest = limits.get("min")
        if not earliest:
            past = limits.get("past")
            if past is not None:
                earliest = now - datetime.timedelta(hours=past)
        latest = limits.get("max")
        if not latest:
            future = limits.get("future")
            if future is not None:
                latest = now + datetime.timedelta(hours=future)

    # Label
    if "label" not in attributes:
        attributes["label"] = current.T("Date")

    # Widget
    set_min = attributes.pop("set_min", None)
    set_max = attributes.pop("set_max", None)
    date_only = False
    if widget == "date":
        date_only = True
        widget = S3CalendarWidget(calendar = calendar,
                                  timepicker = False,
                                  minimum = earliest,
                                  maximum = latest,
                                  set_min = set_min,
                                  set_max = set_max,
                                  )
    elif widget is None or widget == "datetime":
        widget = S3CalendarWidget(calendar = calendar,
                                  timepicker = True,
                                  minimum = earliest,
                                  maximum = latest,
                                  set_min = set_min,
                                  set_max = set_max,
                                  )
    attributes["widget"] = widget

    # Default value
    if attributes.get("default") == "now":
        attributes["default"] = now

    # Representation
    represent = attributes.pop("represent", None)
    represent_method = None
    if represent == "date" or represent is None and date_only:
        represent_method = S3DateTime.date_represent
    elif represent is None:
        represent_method = S3DateTime.datetime_represent
    if represent_method:
        represent = lambda dt: represent_method(dt,
                                                utc=True,
                                                calendar=calendar,
                                                )
    attributes["represent"] = represent

    # Validator and empty-option
    if "requires" not in attributes:
        if date_only:
            validator = IS_UTC_DATE
        else:
            validator = IS_UTC_DATETIME
        requires = validator(calendar=calendar,
                             minimum=earliest,
                             maximum=latest,
                             )
        empty = attributes.pop("empty", None)
        if empty is False:
            attributes["requires"] = requires
        else:
            attributes["requires"] = IS_EMPTY_OR(requires)

    return Field(name, "datetime", **attributes)

# =============================================================================
def s3_time(name="time_of_day", **attr):
    """
        Return a standard time field

        @param name: the field name

        @ToDo: Support minTime/maxTime options for fgtimepicker
    """

    attributes = dict(attr)

    if "widget" not in attributes:
        # adds .time class which launches fgtimepicker from s3.datepicker.js
        attributes["widget"] = TimeWidget.widget

    if "requires" not in attributes:
        requires = IS_TIME()
        empty = attributes.pop("empty", None)
        if empty is False:
            attributes["requires"] = requires
        else:
            attributes["requires"] = IS_EMPTY_OR(requires)

    return Field(name, "time", **attributes)

# END =========================================================================
