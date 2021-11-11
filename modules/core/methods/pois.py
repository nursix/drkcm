"""
    POI Import/Export

    Copyright: (c) 2010-2021 Sahana Software Foundation

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the
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
import sys

from gluon import current, redirect, HTTP, URL, SQLFORM, IS_IN_SET, IS_EMPTY_OR
from gluon.storage import Storage

from s3dal import Field

from ..gis import GIS
from ..resource import S3ResourceTree
from ..tools import s3_format_datetime, s3_parse_datetime

from .base import CRUDMethod

# =============================================================================
class S3ExportPOI(CRUDMethod):
    """ Export point-of-interest resources for a location """

    # -------------------------------------------------------------------------
    def apply_method(self, r, **attr):
        """
            Apply method.

            Args:
                r: the CRUDRequest
                attr: controller options for this request
        """

        output = {}

        if r.http == "GET":
            output = self.export(r, **attr)
        else:
            r.error(405, current.ERROR.BAD_METHOD)

        return output

    # -------------------------------------------------------------------------
    def export(self, r, **attr):
        """
            Export POI resources.

            Args:
                r: the CRUDRequest
                attr: controller options for this request

            URL options:

                - "resources"   list of tablenames to export records from

                - "msince"      datetime in ISO format, "auto" to use the
                                feed's last update

                - "update_feed" 0 to skip the update of the feed's last
                                update datetime, useful for trial exports

            Supported formats:

                .xml            S3XML
                .osm            OSM XML Format
                .kml            Google KML

            (other formats can be requested, but may give unexpected results)
        """

        # Determine request Lx
        current_lx = r.record
        if not current_lx: # or not current_lx.level:
            # Must have a location
            r.error(400, current.ERROR.BAD_REQUEST)

        tables = []
        # Parse the ?resources= parameter
        if "resources" in r.get_vars:
            resources = r.get_vars["resources"]
        else:
            # Fallback to deployment_setting
            resources = current.deployment_settings.get_gis_poi_export_resources()
        if not isinstance(resources, list):
            resources = [resources]

        for t in resources:
            tables.extend(t.split(","))

        # Parse the ?update_feed= parameter
        update_feed = True
        if "update_feed" in r.get_vars:
            _update_feed = r.get_vars["update_feed"]
            if _update_feed == "0":
                update_feed = False

        # Parse the ?msince= parameter
        msince = None
        if "msince" in r.get_vars:
            msince = r.get_vars["msince"]
            if msince.lower() == "auto":
                msince = "auto"
            else:
                msince = s3_parse_datetime(msince)

        # Export a combined tree
        tree = self.export_combined_tree(tables,
                                         msince = msince,
                                         update_feed = update_feed,
                                         lx = current_lx.id,
                                         )

        xml = current.xml

        # Set response headers
        response = current.response
        s3 = response.s3
        headers = response.headers
        representation = r.representation
        if r.representation in s3.json_formats:
            as_json = True
            default = "application/json"
        else:
            as_json = False
            default = "text/xml"
        headers["Content-Type"] = s3.content_type.get(representation,
                                                      default)

        # Find XSLT stylesheet and transform
        stylesheet = r.stylesheet()
        if tree and stylesheet is not None:
            args = Storage(domain=xml.domain,
                           base_url=s3.base_url,
                           utcnow=s3_format_datetime())
            tree = xml.transform(tree, stylesheet, **args)
        if tree:
            if as_json:
                output = xml.tree2json(tree, pretty_print=True)
            else:
                output = xml.tostring(tree, pretty_print=True)

        return output

    # -------------------------------------------------------------------------
    def export_combined_tree(self, tables, msince=None, update_feed=True, lx=None):
        """
            Export a combined tree of all records in tables, which
            are in Lx, and have been updated since msince.

            Args:
                tables: list of table names
                msince: minimum modified_on datetime, "auto" for
                        automatic from feed data, None to turn it off
                update_feed: update the last_update datetime in the feed
                lx: the id of the current Lx
        """

        db = current.db
        s3db = current.s3db
        ftable = s3db.gis_poi_feed

        elements = []
        for tablename in tables:

            # Define the resource
            try:
                resource = s3db.resource(tablename, components=[])
            except AttributeError:
                # Table not defined (module deactivated?)
                continue

            # Check
            if "location_id" not in resource.fields:
                # Hardly a POI resource without location_id
                continue

            # Add Lx filter
            self._add_lx_filter(resource, lx)

            # Get the feed data
            query = (ftable.tablename == tablename) & \
                    (ftable.location_id == lx)
            feed = db(query).select(limitby = (0, 1)).first()
            if msince == "auto":
                if feed is None:
                    _msince = None
                else:
                    _msince = feed.last_update
            else:
                _msince = msince

            # Export the tree and append its element to the element list
            tree = S3ResourceTree(resource).build(msince = msince,
                                                  references = ["location_id"],
                                                  )

            # Update the feed data
            if update_feed:
                muntil = resource.muntil
                if feed is None:
                    ftable.insert(location_id = lx,
                                  tablename = tablename,
                                  last_update = muntil)
                else:
                    feed.update_record(last_update = muntil)

            elements.extend([c for c in tree.getroot()])

        # Combine all elements in one tree and return it
        tree = current.xml.tree(elements, results=len(elements))
        return tree

    # -------------------------------------------------------------------------
    @staticmethod
    def _add_lx_filter(resource, lx):
        """
            Add a Lx filter for the current location to this resource.

            Args:
                resource: the resource
        """

        from ..resource import FS
        query = (FS("location_id$path").contains("/%s/" % lx)) | \
                (FS("location_id$path").like("%s/%%" % lx))
        resource.add_filter(query)

# =============================================================================
class S3ImportPOI(CRUDMethod):
    """
        Import point-of-interest resources for a location
    """

    # -------------------------------------------------------------------------
    @staticmethod
    def apply_method(r, **attr):
        """
            Apply method.

            Args:
                r: the CRUDRequest
                attr: controller options for this request
        """

        if r.representation == "html":

            T = current.T
            s3db = current.s3db
            request = current.request
            response = current.response
            settings = current.deployment_settings
            s3 = current.response.s3

            title = T("Import from OpenStreetMap")

            resources_list = settings.get_gis_poi_export_resources()
            uploadpath = os.path.join(request.folder,"uploads/")
            from ..tools import s3_yes_no_represent

            fields = [Field("text1", # Dummy Field to add text inside the Form
                            label = "",
                            default = T("Can read PoIs either from an OpenStreetMap file (.osm) or mirror."),
                            writable = False),
                      Field("file", "upload",
                            length = current.MAX_FILENAME_LENGTH,
                            uploadfolder = uploadpath,
                            label = T("File")),
                      Field("text2", # Dummy Field to add text inside the Form
                            label = "",
                            default = "Or",
                            writable = False),
                      Field("host",
                            default = "localhost",
                            label = T("Host")),
                      Field("database",
                            default = "osm",
                            label = T("Database")),
                      Field("user",
                            default = "osm",
                            label = T("User")),
                      Field("password", "string",
                            default = "planet",
                            label = T("Password")),
                      Field("ignore_errors", "boolean",
                            label = T("Ignore Errors?"),
                            represent = s3_yes_no_represent),
                      Field("resources",
                            label = T("Select resources to import"),
                            requires = IS_IN_SET(resources_list, multiple=True),
                            default = resources_list,
                            widget = SQLFORM.widgets.checkboxes.widget)
                      ]

            if not r.id:
                from ..tools import IS_LOCATION
                from ..ui import S3LocationAutocompleteWidget
                # dummy field
                field = s3db.org_office.location_id
                field.requires = IS_EMPTY_OR(IS_LOCATION())
                field.widget = S3LocationAutocompleteWidget()
                fields.insert(3, field)

            from ..tools import s3_mark_required
            labels = s3_mark_required(fields, ["file", "location_id"])[0]
            s3.has_required = True

            form = SQLFORM.factory(*fields,
                                   formstyle = settings.get_ui_formstyle(),
                                   submit_button = T("Import"),
                                   labels = labels,
                                   separator = "",
                                   table_name = "import_poi" # Dummy table name
                                   )

            response.view = "create.html"
            output = {"title": title,
                      "form": form,
                      }

            if form.accepts(request.vars, current.session):
                form_vars = form.vars
                if form_vars.file != "":
                    osm_file = open(uploadpath + form_vars.file, "r")
                else:
                    # Create .poly file
                    if r.record:
                        record = r.record
                    elif not form_vars.location_id:
                        form.errors["location_id"] = T("Location is Required!")
                        return output
                    else:
                        gtable = s3db.gis_location
                        record = current.db(gtable.id == form_vars.location_id).select(gtable.name,
                                                                                       gtable.wkt,
                                                                                       limitby = (0, 1)
                                                                                       ).first()
                        if record.wkt is None:
                            form.errors["location_id"] = T("Location needs to have WKT!")
                            return output
                    error = GIS.create_poly(record)
                    if error:
                        current.session.error = error
                        redirect(URL(args=r.id))
                    # Use Osmosis to extract an .osm file using this .poly
                    name = record.name
                    if os.path.exists(os.path.join(os.getcwd(), "temp")): # use web2py/temp
                        TEMP = os.path.join(os.getcwd(), "temp")
                    else:
                        import tempfile
                        TEMP = tempfile.gettempdir()
                    filename = os.path.join(TEMP, "%s.osm" % name)
                    cmd = ["/home/osm/osmosis/bin/osmosis", # @ToDo: deployment_setting
                           "--read-pgsql",
                           "host=%s" % form_vars.host,
                           "database=%s" % form_vars.database,
                           "user=%s" % form_vars.user,
                           "password=%s" % form_vars.password,
                           "--dataset-dump",
                           "--bounding-polygon",
                           "file=%s" % os.path.join(TEMP, "%s.poly" % name),
                           "--write-xml",
                           "file=%s" % filename,
                           ]
                    import subprocess
                    try:
                        #result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
                        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
                    except subprocess.CalledProcessError as e:
                        current.session.error = T("OSM file generation failed: %s") % e.output
                        redirect(URL(args=r.id))
                    except AttributeError:
                        # Python < 2.7
                        error = subprocess.call(cmd, shell=True)
                        if error:
                            current.log.debug(cmd)
                            current.session.error = T("OSM file generation failed!")
                            redirect(URL(args=r.id))
                    try:
                        osm_file = open(filename, "r")
                    except IOError:
                        current.session.error = T("Cannot open created OSM file!")
                        redirect(URL(args=r.id))

                stylesheet = os.path.join(request.folder, "static", "formats",
                                          "osm", "import.xsl")
                ignore_errors = form_vars.get("ignore_errors", None)
                xml = current.xml
                tree = xml.parse(osm_file)
                define_resource = s3db.resource
                response.error = ""
                import_count = 0

                import_res = list(set(form_vars["resources"]) & \
                                  set(resources_list))

                for tablename in import_res:
                    try:
                        s3db[tablename]
                    except AttributeError:
                        # Module disabled
                        continue
                    resource = define_resource(tablename)
                    s3xml = xml.transform(tree,
                                          stylesheet_path = stylesheet,
                                          name = resource.name,
                                          )
                    try:
                        result = resource.import_xml(s3xml,
                                                     ignore_errors = ignore_errors,
                                                     )
                    except Exception:
                        response.error += str(sys.exc_info()[1])
                    else:
                        import_count += result.count
                if import_count:
                    response.confirmation = "%s %s" % \
                        (import_count, T("PoIs successfully imported."))
                else:
                    response.information = T("No PoIs available.")

            return output

        else:
            raise HTTP(405, current.ERROR.BAD_METHOD)

# END =========================================================================
