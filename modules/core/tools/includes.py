"""
    Common JS/CSS includes

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

import os

from gluon import current, HTTP, URL, XML

# =============================================================================
def s3_include_debug_css():
    """
        Generates html to include the css listed in
            /modules/templates/<theme>/css.cfg
    """

    request = current.request

    location = current.response.s3.theme_config
    filename = "%s/modules/templates/%s/css.cfg" % (request.folder, location)
    if not os.path.isfile(filename):
        raise HTTP(500, "Theme configuration file missing: modules/templates/%s/css.cfg" % location)

    link_template = '<link href="/%s/static/styles/%%s" rel="stylesheet" type="text/css" />' % \
                    request.application
    links = ""

    with open(filename, "r") as css_cfg:
        links = "\n".join(link_template % cssname.rstrip()
                          for cssname in css_cfg if cssname[0] != "#")

    return XML(links)

# =============================================================================
def s3_include_debug_js():
    """
        Generates html to include the js scripts listed in
            /static/scripts/tools/sahana.js.cfg
    """

    request = current.request

    scripts_dir = os.path.join(request.folder, "static", "scripts")

    import mergejsmf

    config_dict = {
        ".": scripts_dir,
        "ui": scripts_dir,
        "web2py": scripts_dir,
        "S3":     scripts_dir
    }
    config_filename = "%s/tools/sahana.js.cfg"  % scripts_dir
    files = mergejsmf.getFiles(config_dict, config_filename)[1]

    script_template = '<script src="/%s/static/scripts/%%s"></script>' % \
                      request.application

    scripts = "\n".join(script_template % scriptname for scriptname in files)
    return XML(scripts)

# =============================================================================
def s3_include_ext():
    """
        Add ExtJS CSS & JS into a page for a Map
        - since this is normally run from MAP.xml() it is too late to insert into
          s3.[external_]stylesheets, so must inject sheets into correct order
    """

    s3 = current.response.s3
    if s3.ext_included:
        # Ext already included
        return
    request = current.request
    appname = request.application

    xtheme = current.deployment_settings.get_base_xtheme()
    if xtheme:
        xtheme = "%smin.css" % xtheme[:-3]
        xtheme = \
    "<link href='/%s/static/themes/%s' rel='stylesheet' type='text/css' />" % \
        (appname, xtheme)

    if s3.cdn:
        # For Sites Hosted on the Public Internet, using a CDN may provide better performance
        PATH = "//cdn.sencha.com/ext/gpl/3.4.1.1"
    else:
        PATH = "/%s/static/scripts/ext" % appname

    if s3.debug:
        # Provide debug versions of CSS / JS
        adapter = "%s/adapter/jquery/ext-jquery-adapter-debug.js" % PATH
        main_js = "%s/ext-all-debug.js" % PATH
        main_css = \
    "<link href='%s/resources/css/ext-all-notheme.css' rel='stylesheet' type='text/css' />" % PATH
        if not xtheme:
            xtheme = \
    "<link href='%s/resources/css/xtheme-gray.css' rel='stylesheet' type='text/css' />" % PATH
    else:
        adapter = "%s/adapter/jquery/ext-jquery-adapter.js" % PATH
        main_js = "%s/ext-all.js" % PATH
        if xtheme:
            main_css = \
    "<link href='/%s/static/scripts/ext/resources/css/ext-notheme.min.css' rel='stylesheet' type='text/css' />" % appname
        else:
            main_css = \
    "<link href='/%s/static/scripts/ext/resources/css/ext-gray.min.css' rel='stylesheet' type='text/css' />" % appname

    scripts = s3.scripts
    scripts_append = scripts.append
    scripts_append(adapter)
    scripts_append(main_js)

    langfile = "ext-lang-%s.js" % s3.language
    if os.path.exists(os.path.join(request.folder, "static", "scripts", "ext", "src", "locale", langfile)):
        locale = "%s/src/locale/%s" % (PATH, langfile)
        scripts_append(locale)

    if xtheme:
        s3.jquery_ready.append('''$('link:first').after("%s").after("%s")''' % (xtheme, main_css))
    else:
        s3.jquery_ready.append('''$('link:first').after("%s")''' % main_css)
    s3.ext_included = True

# =============================================================================
def s3_include_simile():
    """
        Add Simile CSS & JS into a page for a Timeline
    """

    s3 = current.response.s3
    if s3.simile_included:
        # Simile already included
        return
    appname = current.request.application

    #scripts = s3.scripts

    if s3.debug:
        # Provide debug versions of CSS / JS
        s3.scripts += ["/%s/static/scripts/S3/s3.simile.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/platform.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/debug.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/xmlhttp.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/json.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/dom.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/graphics.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/date-time.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/string.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/html.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/data-structure.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/units.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/ajax.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/history.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/window-manager.js" % appname,
                       "/%s/static/scripts/simile/ajax/scripts/remoteLog.js" % appname,
                       "/%s/static/scripts/S3/s3.timeline.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/timeline.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/band.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/themes.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/ethers.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/ether-painters.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/event-utils.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/labellers.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/sources.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/original-painter.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/detailed-painter.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/overview-painter.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/compact-painter.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/decorators.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/l10n/en/timeline.js" % appname,
                       "/%s/static/scripts/simile/timeline/scripts/l10n/en/labellers.js" % appname,
                       ]
        css = "".join(["<link href='/%s/static/scripts/simile/ajax/styles/graphics.css' rel='stylesheet' type='text/css' />" % appname,
                       "<link href='/%s/static/scripts/simile/timeline/styles/ethers.css' rel='stylesheet' type='text/css' />" % appname,
                       "<link href='/%s/static/scripts/simile/timeline/styles/events.css' rel='stylesheet' type='text/css' />" % appname,
                       "<link href='/%s/static/scripts/simile/timeline/styles/timeline.css' rel='stylesheet' type='text/css' />" % appname,
                       ])
    else:
        s3.scripts.append("/%s/static/scripts/S3/s3.timeline.min.js" % appname)
        css = "".join(["<link href='/%s/static/scripts/simile/ajax/styles/graphics.css' rel='stylesheet' type='text/css' />" % appname,
                       "<link href='/%s/static/scripts/simile/timeline/timeline-bundle.css' rel='stylesheet' type='text/css' />" % appname,
                       ])

    s3.jquery_ready.append('''$('link:first').after("%s")''' % css)

    supported_locales = [
        "cs",       # Czech
        "de",       # German
        "en",       # English
        "es",       # Spanish
        "fr",       # French
        "it",       # Italian
        "nl",       # Dutch (The Netherlands)
        "pl",       # Polish
        "ru",       # Russian
        "se",       # Swedish
        "tr",       # Turkish
        "vi",       # Vietnamese
        "zh"        # Chinese
        ]

    if s3.language in supported_locales:
        locale = s3.language
    else:
        locale = "en"
    s3.scripts += ["/%s/static/scripts/simile/timeline/scripts/l10n/%s/timeline.js" % (appname, locale),
                   "/%s/static/scripts/simile/timeline/scripts/l10n/%s/labellers.js" % (appname, locale),
                   ]

    s3.simile_included = True

# =============================================================================
def s3_include_underscore():
    """
        Add Undercore JS into a page
        - for Map templates
        - for templates in GroupedOptsWidget comment
    """

    s3 = current.response.s3
    debug = s3.debug
    scripts = s3.scripts
    if s3.cdn:
        if debug:
            script = \
"//cdnjs.cloudflare.com/ajax/libs/underscore.js/1.6.0/underscore.js"
        else:
            script = \
"//cdnjs.cloudflare.com/ajax/libs/underscore.js/1.6.0/underscore-min.js"
    else:
        if debug:
            script = URL(c="static", f="scripts/underscore.js")
        else:
            script = URL(c="static", f="scripts/underscore-min.js")
    if script not in scripts:
        scripts.append(script)

# END =========================================================================
