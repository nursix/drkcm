"""
    Autocomplete Widgets

    Copyright: 2009-2023 (c) Sahana Software Foundation

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

__all__ = ("S3AutocompleteWidget",
           "S3HumanResourceAutocompleteWidget",
           "S3LocationAutocompleteWidget",
           "S3OrganisationAutocompleteWidget",
           "S3PersonAutocompleteWidget",
           "S3PentityAutocompleteWidget",
           "S3SiteAutocompleteWidget",
           "search_ac",
           )

import json

from gluon import current, DIV, HTTP, INPUT, TAG

from gluon.sqlhtml import FormWidget, StringWidget

from ..tools import JSONSEPARATORS, s3_str, s3_strip_markup

# =============================================================================
class S3AutocompleteWidget(FormWidget):
    """
        Renders a SELECT as an INPUT field with AJAX Autocomplete
    """

    def __init__(self,
                 module,
                 resourcename,
                 fieldname = "name",
                 filter = "",       # REST filter
                 link_filter = "",
                 post_process = "",
                 ):

        self.module = module
        self.resourcename = resourcename
        self.fieldname = fieldname
        self.filter = filter
        self.link_filter = link_filter
        self.post_process = post_process

        # @ToDo: Refreshes all dropdowns as-necessary
        self.post_process = post_process or ""

    def __call__(self, field, value, **attributes):

        s3 = current.response.s3
        settings = current.deployment_settings

        default = {"_type": "text",
                   "value": str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = attr["_class"] + " hide"

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")
        dummy_input = "dummy_%s" % real_input

        # JS Function defined in static/scripts/S3/S3.js
        script = '''S3.autocomplete.normal('%s','%s','%s','%s','%s',"%s"''' % \
            (self.fieldname,
             self.module,
             self.resourcename,
             real_input,
             self.filter,
             self.link_filter,
             )

        options = ""
        post_process = self.post_process
        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()
        if min_length != 2:
            options = ''',"%(postprocess)s",%(delay)s,%(min_length)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 "min_length": min_length,
                 }
        elif delay != 800:
            options = ''',"%(postprocess)s",%(delay)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 }
        elif post_process:
            options = ''',"%(postprocess)s"''' % \
                {"postprocess": post_process,
                 }

        script = '''%s%s)''' % (script, options)
        s3.jquery_ready.append(script)

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        s3.js_global.append('''i18n.none_of_the_above="%s"''' % current.T("None of the above"))

        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(**attr),
                       )

# =============================================================================
class S3HumanResourceAutocompleteWidget(FormWidget):
    """
        Renders an hrm_human_resource SELECT as an INPUT field with
        AJAX Autocomplete.

        Differs from the S3AutocompleteWidget in that it uses:
            3 name fields
            Organisation
            Job Role
   """

    def __init__(self,
                 post_process = "",
                 group = "",    # Filter to staff/volunteers/deployables
                 ):

        self.post_process = post_process
        self.group = group

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        settings = current.deployment_settings

        group = self.group
        if not group and current.request.controller == "deploy":
            group = "deploy"

        default = {"_type": "text",
                   "value": str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = "%s hide" % attr["_class"]

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")
        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        script = '''S3.autocomplete.hrm('%(group)s','%(input)s',"%(postprocess)s"''' % \
            {"group": group,
             "input": real_input,
             "postprocess": self.post_process,
             }
        if delay != 800:
            script = "%s,%s" % (script, delay)
            if min_length != 2:
                script = "%s,%s" % (script, min_length)
        elif min_length != 2:
            script = "%s,,%s" % (script, min_length)
        script = "%s)" % script

        current.response.s3.jquery_ready.append(script)

        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             _value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(**attr),
                       )

# =============================================================================
class S3LocationAutocompleteWidget(FormWidget):
    """
        Renders a gis_location SELECT as an INPUT field with AJAX Autocomplete

        Appropriate when the location has been previously created (as is the
        case for location groups or other specialized locations that need
        the location create form).
        LocationSelector is generally more appropriate for specific locations.

        Currently used for selecting the region location in gis_config
        and for project/location.
    """

    def __init__(self,
                 level = "",
                 post_process = "",
                 ):

        self.level = level
        self.post_process = post_process

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        settings = current.deployment_settings

        level = self.level
        if isinstance(level, list):
            levels = ""
            counter = 0
            for _level in level:
                levels += _level
                if counter < len(level):
                    levels += "|"
                counter += 1

        default = {"_type": "text",
                   "value": s3_str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = attr["_class"] + " hide"

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")

        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        # Mandatory part
        script = '''S3.autocomplete.location("%s"''' % real_input
        # Optional parts
        if self.post_process:
            # We need all
            script = '''%s,'%s',%s,%s,"%s"''' % (script, level, min_length, delay, self.post_process)
        elif delay != 800:
            script = '''%s,"%s",%s,%s''' % (script, level, min_length, delay)
        elif min_length != 2:
            script = '''%s,"%s",%s''' % (script, level, min_length)
        elif level:
            script = '''%s,"%s"''' % (script, level)
        # Close
        script = "%s)" % script
        current.response.s3.jquery_ready.append(script)
        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(**attr),
                       )

# =============================================================================
class S3OrganisationAutocompleteWidget(FormWidget):
    """
        Renders an org_organisation SELECT as an INPUT field with AJAX
        Autocomplete. Differs from the S3AutocompleteWidget in that it
        can default to the setting in the profile.

        TODO Add an option to hide the widget completely when using the
             Org from the Profile (i.e. prevent user overrides)
    """

    def __init__(self,
                 post_process = "",
                 default_from_profile = False,
                 ):

        self.post_process = post_process
        self.tablename = "org_organisation"
        self.default_from_profile = default_from_profile

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        def transform_value(value):
            if not value and self.default_from_profile:
                auth = current.session.auth
                if auth and auth.user:
                    value = auth.user.organisation_id
            return value

        settings = current.deployment_settings
        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        return self.autocomplete_template(self.post_process,
                                          delay,
                                          min_length,
                                          field,
                                          value,
                                          attributes,
                                          transform_value = transform_value,
                                          )

    # -------------------------------------------------------------------------
    @staticmethod
    def autocomplete_template(post_process,
                              delay,
                              min_length,
                              field,
                              value,
                              attributes,
                              source = None,
                              transform_value = lambda value: value,
                              ):
        """
            Renders a SELECT as an INPUT field with AJAX Autocomplete
        """

        value = transform_value(value)

        default = {"_type": "text",
                   "value": (value is not None and s3_str(value)) or "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = attr["_class"] + " hide"

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")

        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        script = \
'''S3.autocomplete.org('%(input)s',"%(postprocess)s",%(delay)s,%(min_length)s)''' % \
            {"input": real_input,
             "postprocess": post_process,
             "delay": delay,
             "min_length": min_length,
             }

        current.response.s3.jquery_ready.append(script)
        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide"),
                       INPUT(**attr),
                       )

# =============================================================================
class S3PersonAutocompleteWidget(FormWidget):
    """
        Renders a pr_person SELECT as an INPUT field with AJAX Autocomplete.
        Differs from the S3AutocompleteWidget in that it uses 3 name fields

        To make this widget use the HR table, set the controller to "hrm"
    """

    def __init__(self,
                 controller = "pr",
                 function = "person_search",
                 post_process = "",
                 hideerror = False,
                 ajax_filter = "",
                 ):

        self.post_process = post_process
        self.c = controller
        self.f = function
        self.hideerror = hideerror
        self.ajax_filter = ajax_filter

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        default = {"_type": "text",
                   "value": str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = "%s hide" % attr["_class"]

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")

        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        script = '''S3.autocomplete.person('%(controller)s','%(fn)s',"%(input)s"''' % \
            {"controller": self.c,
             "fn": self.f,
             "input": real_input,
             }
        options = ""
        post_process = self.post_process

        settings = current.deployment_settings
        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        if self.ajax_filter:
            options = ''',"%(ajax_filter)s"''' % \
                {"ajax_filter": self.ajax_filter}

        if min_length != 2:
            options += ''',"%(postprocess)s",%(delay)s,%(min_length)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 "min_length": min_length,
                 }
        elif delay != 800:
            options += ''',"%(postprocess)s",%(delay)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 }
        elif post_process:
            options += ''',"%(postprocess)s"''' % \
                {"postprocess": post_process}

        script = '''%s%s)''' % (script, options)
        current.response.s3.jquery_ready.append(script)

        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             _value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(hideerror = self.hideerror, **attr),
                       )

# =============================================================================
class S3PentityAutocompleteWidget(FormWidget):
    """
        Renders a pr_pentity SELECT as an INPUT field with AJAX Autocomplete.
        Differs from the S3AutocompleteWidget in that it can filter by type &
        also represents results with the type
    """

    def __init__(self,
                 controller = "pr",
                 function = "pentity",
                 types = None,
                 post_process = "",
                 hideerror = False,
                 ):

        self.post_process = post_process
        self.c = controller
        self.f = function
        self.types = types
        self.hideerror = hideerror

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        default = {"_type": "text",
                   "value": str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = "%s hide" % attr["_class"]

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")

        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            text = s3_str(field.represent(value))
            if "<" in text:
                text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        T = current.T
        s3 = current.response.s3
        script = \
'''i18n.person="%s"\ni18n.group="%s"\ni18n.none_of_the_above="%s"''' % \
            (T("Person"), T("Group"), T("None of the above"))
        s3.js_global.append(script)

        if self.types:
            # Something other than default: ("pr_person", "pr_group")
            types = json.dumps(self.types, separators=JSONSEPARATORS)
        else:
            types = ""

        script = '''S3.autocomplete.pentity('%(controller)s','%(fn)s',"%(input)s"''' % \
            {"controller": self.c,
             "fn": self.f,
             "input": real_input,
             }

        options = ""
        post_process = self.post_process

        settings = current.deployment_settings
        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        if types:
            options = ''',"%(postprocess)s",%(delay)s,%(min_length)s,%(types)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 "min_length": min_length,
                 "types": types,
                 }
        elif min_length != 2:
            options = ''',"%(postprocess)s",%(delay)s,%(min_length)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 "min_length": min_length,
                 }
        elif delay != 800:
            options = ''',"%(postprocess)s",%(delay)s''' % \
                {"postprocess": post_process,
                 "delay": delay,
                 }
        elif post_process:
            options = ''',"%(postprocess)s"''' % \
                {"postprocess": post_process,
                 }

        script = '''%s%s)''' % (script, options)
        s3.jquery_ready.append(script)
        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             _value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(hideerror = self.hideerror, **attr),
                       )

# =============================================================================
class S3SiteAutocompleteWidget(FormWidget):
    """
        Renders an org_site SELECT as an INPUT field with AJAX Autocomplete.
        Differs from the S3AutocompleteWidget in that it uses name & type fields
        in the represent
    """

    def __init__(self,
                 post_process = "",
                 ):

        self.auth = current.auth
        self.post_process = post_process

    # -------------------------------------------------------------------------
    def __call__(self, field, value, **attributes):

        default = {"_type": "text",
                   "value": str(value) if value is not None else "",
                   }
        attr = StringWidget._attributes(field, default, **attributes)

        # Hide the real field
        attr["_class"] = "%s hide" % attr["_class"]

        if "_id" in attr:
            real_input = attr["_id"]
        else:
            real_input = str(field).replace(".", "_")
        dummy_input = "dummy_%s" % real_input

        if value:
            try:
                value = int(value)
            except ValueError:
                pass
            # Provide the representation for the current/default Value
            represent = field.represent
            if hasattr(represent, "link"):
                # S3Represent, so don't generate HTML
                text = s3_str(represent(value, show_link=False))
            else:
                # Custom represent, so filter out HTML later
                text = s3_str(represent(value))
                if "<" in text:
                    text = s3_strip_markup(text)
            represent = s3_str(text)
        else:
            represent = ""

        s3 = current.response.s3
        site_types = current.auth.org_site_types
        for instance_type in site_types:
            # Change from T()
            site_types[instance_type] = s3_str(site_types[instance_type])
        site_types = '''S3.org_site_types=%s''' % json.dumps(site_types, separators=JSONSEPARATORS)

        settings = current.deployment_settings
        delay = settings.get_ui_autocomplete_delay()
        min_length = settings.get_ui_autocomplete_min_chars()

        js_global = s3.js_global
        if site_types not in js_global:
            js_global.append(site_types)
        script = '''S3.autocomplete.site('%(input)s',"%(postprocess)s"''' % \
            {"input": real_input,
             "postprocess": self.post_process,
             }
        if delay != 800:
            script = "%s,%s" % (script, delay)
            if min_length != 2:
                script = "%s,%s" % (script, min_length)
        elif min_length != 2:
            script = "%s,,%s" % (script, min_length)
        script = "%s)" % script

        s3.jquery_ready.append(script)

        return TAG[""](INPUT(_id = dummy_input,
                             # Required to retain label on error:
                             _name = dummy_input,
                             _class = "string",
                             _value = represent,
                             ),
                       DIV(_id = "%s_throbber" % dummy_input,
                           _class = "throbber input_throbber hide",
                           ),
                       INPUT(**attr),
                       )

# =============================================================================
def search_ac(r, **attr):
    """
        JSON search method for S3AutocompleteWidget

        Args:
            r: the CRUDRequest
            attr: request attributes
    """

    _vars = current.request.get_vars

    # JQueryUI Autocomplete uses "term" instead of "value"
    # (old JQuery Autocomplete uses "q" instead of "value")
    value = _vars.term or _vars.value or _vars.q or None

    # We want to do case-insensitive searches
    # (default anyway on MySQL/SQLite, but not PostgreSQL)
    value = value.lower().strip()

    fieldname = _vars.get("field", "name")
    fieldname = str.lower(fieldname)
    filter = _vars.get("filter", "~")

    resource = r.resource
    table = resource.table

    limit = int(_vars.limit or 0)

    from ..resource import FS
    field = FS(fieldname)

    # Default fields to return
    fields = ["id", fieldname]
    # Now using custom method
    #if resource.tablename == "org_site":
    #    # Simpler to provide an exception case than write a whole new class
    #    fields.append("instance_type")

    if filter == "~":
        # Normal single-field Autocomplete
        query = (field.lower().like(value + "%"))

    elif filter == "=":
        if field.type.split(" ")[0] in \
            ["reference", "id", "float", "integer"]:
            # Numeric, e.g. Organizations' offices_by_org
            query = (field == value)
        else:
            # Text
            query = (field.lower() == value)

    elif filter == "<":
        query = (field < value)

    elif filter == ">":
        query = (field > value)

    else:
        output = current.xml.json_message(False, 400,
                    "Unsupported filter! Supported filters: ~, =, <, >")
        raise HTTP(400, body=output)

    if "link" in _vars:
        from .widgets import S3EmbeddedComponentWidget
        link_filter = S3EmbeddedComponentWidget.link_filter_query(table,
                                                                  _vars.link,
                                                                  )
        if link_filter:
            query &= link_filter

    # Select only or exclude template records:
    # to only select templates:
    #           ?template=<fieldname>.<value>,
    #      e.g. ?template=template.true
    # to exclude templates:
    #           ?template=~<fieldname>.<value>
    #      e.g. ?template=~template.true
    if "template" in _vars:
        try:
            flag, val = _vars.template.split(".", 1)
            if flag[0] == "~":
                exclude = True
                flag = flag[1:]
            else:
                exclude = False
            ffield = table[flag]
        except:
            pass # ignore
        else:
            if str(ffield.type) == "boolean":
                if val.lower() == "true":
                    val = True
                else:
                    val = False
            if exclude:
                templates = (ffield != val)
            else:
                templates = (ffield == val)
            resource.add_filter(templates)

    resource.add_filter(query)

    output = None
    if filter == "~":
        MAX_SEARCH_RESULTS = current.deployment_settings.get_search_max_results()
        if (not limit or limit > MAX_SEARCH_RESULTS) and \
           resource.count() > MAX_SEARCH_RESULTS:
            output = [
                {"label": str(current.T("There are more than %(max)s results, please input more characters.") % \
                    {"max": MAX_SEARCH_RESULTS})
                 }
                ]

    if output is None:
        rows = resource.select(fields,
                               start=0,
                               limit=limit,
                               orderby=field,
                               as_rows=True)
        output = []
        append = output.append
        for row in rows:
            record = {"id": row.id,
                      fieldname: row[fieldname],
                      }
            append(record)

    current.response.headers["Content-Type"] = "application/json"
    return json.dumps(output, separators=JSONSEPARATORS)

