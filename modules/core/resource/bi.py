"""
    Bulk Importer Tool

    Copyright: 2011-2021 (c) Sahana Software Foundation

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

__all__ = ("S3BulkImporter",
           )

import datetime
import json
import os

from io import StringIO, BytesIO
from urllib import request as urllib2
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from gluon import current, SQLFORM
from gluon.storage import Storage
from gluon.tools import callback, fetch

from ..tools import IS_JSONS3

# =============================================================================
class S3BulkImporter:
    """
        Import CSV files of data to pre-populate the database.
        Suitable for use in Testing, Demos & Simulations

        http://eden.sahanafoundation.org/wiki/DeveloperGuidelines/PrePopulate
    """

    def __init__(self):

        import csv
        from xml.sax.saxutils import unescape

        self.csv = csv
        self.unescape = unescape
        self.tasks = []
        # Some functions refer to a different resource
        self.alternateTables = {
            "hrm_group_membership": {"tablename": "pr_group_membership",
                                     "prefix": "pr",
                                     "name": "group_membership"},
            "hrm_person": {"tablename": "pr_person",
                           "prefix": "pr",
                           "name": "person"},
            "member_person": {"tablename": "pr_person",
                              "prefix": "pr",
                              "name": "person"},
            }
        # Keep track of which resources have been customised so we don't do this twice
        self.customised = []
        self.errorList = []
        self.resultList = []

    # -------------------------------------------------------------------------
    def perform_tasks(self, path):
        """
            Load and then execute the import jobs that are listed in the
            descriptor file (tasks.cfg)
        """

        self.load_descriptor(path)
        for task in self.tasks:
            if task[0] == 1:
                self.execute_import_task(task)
            elif task[0] == 2:
                self.execute_special_task(task)

    # -------------------------------------------------------------------------
    def load_descriptor(self, path):
        """
            Load the descriptor file and then all the import tasks in that file
            into the task property.
            The descriptor file is the file called tasks.cfg in path.
            The file consists of a comma separated list of:
            module, resource name, csv filename, xsl filename.
        """

        source = open(os.path.join(path, "tasks.cfg"), "r")
        values = self.csv.reader(source)
        for details in values:
            if details == []:
                continue
            prefix = details[0][0].strip('" ')
            if prefix == "#": # comment
                continue
            if prefix == "*": # specialist function
                self.extract_other_import_line(path, details)
            else: # standard CSV importer
                self.extract_csv_import_line(path, details)

    # -------------------------------------------------------------------------
    def extract_csv_import_line(self, path, details):
        """
            Extract the details for a CSV Import Task
        """

        num_args = len(details)
        if num_args == 4 or num_args == 5:
            # Remove any spaces and enclosing double quote
            mod = details[0].strip('" ')
            res = details[1].strip('" ')
            folder = current.request.folder

            csv_filename = details[2].strip('" ')
            if csv_filename[:7] == "http://":
                csv = csv_filename
            else:
                (csv_path, csv_file) = os.path.split(csv_filename)
                if csv_path != "":
                    path = os.path.join(folder,
                                        "modules",
                                        "templates",
                                        csv_path)
                    # @todo: deprecate this block once migration completed
                    if not os.path.exists(path):
                        # Non-standard location (legacy template)?
                        path = os.path.join(folder,
                                            "private",
                                            "templates",
                                            csv_path)
                csv = os.path.join(path, csv_file)

            xslt_filename = details[3].strip('" ')
            xslt_path = os.path.join(folder,
                                     "static",
                                     "formats",
                                     "s3csv")
            # Try the module directory in the templates directory first
            xsl = os.path.join(xslt_path, mod, xslt_filename)
            if os.path.exists(xsl) == False:
                # Now try the templates directory
                xsl = os.path.join(xslt_path, xslt_filename)
                if os.path.exists(xsl) == False:
                    # Use the same directory as the csv file
                    xsl = os.path.join(path, xslt_filename)
                    if os.path.exists(xsl) == False:
                        self.errorList.append(
                        "Failed to find a transform file %s, Giving up." % xslt_filename)
                        return

            if num_args == 5:
                extra_data = details[4]
            else:
                extra_data = None
            self.tasks.append([1, mod, res, csv, xsl, extra_data])
        else:
            self.errorList.append(
            "prepopulate error: job not of length 4, ignored: %s" % str(details))

    # -------------------------------------------------------------------------
    def extract_other_import_line(self, path, details):
        """
            Store a single import job into the tasks property
            *,function,filename,*extra_args
        """

        function = details[1].strip('" ')
        filepath = None
        if len(details) >= 3:
            filename = details[2].strip('" ')
            if filename != "":
                (subfolder, filename) = os.path.split(filename)
                if subfolder != "":
                    path = os.path.join(current.request.folder,
                                        "modules",
                                        "templates",
                                        subfolder)
                    # @todo: deprecate this block once migration completed
                    if not os.path.exists(path):
                        # Non-standard location (legacy template)?
                        path = os.path.join(current.request.folder,
                                            "private",
                                            "templates",
                                            subfolder)
                filepath = os.path.join(path, filename)

        if len(details) >= 4:
            extra_args = details[3:]
        else:
            extra_args = None

        self.tasks.append((2, function, filepath, extra_args))

    # -------------------------------------------------------------------------
    def execute_import_task(self, task):
        """
            Execute each import job, in order
        """

        # Disable min_length for password during prepop
        current.auth.ignore_min_password_length()

        start = datetime.datetime.now()
        if task[0] == 1:
            s3db = current.s3db
            response = current.response
            error_string = "prepopulate error: file %s missing"
            # Store the view
            view = response.view

            #current.log.debug("Running job %s %s (filename=%s transform=%s)" % (task[1],
            #                                                                    task[2],
            #                                                                    task[3],
            #                                                                    task[4],
            #                                                                    ))

            prefix = task[1]
            name = task[2]
            tablename = "%s_%s" % (prefix, name)
            if tablename in self.alternateTables:
                details = self.alternateTables[tablename]
                if "tablename" in details:
                    tablename = details["tablename"]
                s3db.table(tablename)
                if "loader" in details:
                    loader = details["loader"]
                    if loader is not None:
                        loader()
                if "prefix" in details:
                    prefix = details["prefix"]
                if "name" in details:
                    name = details["name"]

            try:
                resource = s3db.resource(tablename)
            except AttributeError:
                # Table cannot be loaded
                self.errorList.append("WARNING: Unable to find table %s import job skipped" % tablename)
                return

            # Check if the source file is accessible
            filename = task[3]
            if filename[:7] == "http://":
                req = urllib2.Request(url=filename)
                try:
                    f = urlopen(req)
                except HTTPError as e:
                    self.errorList.append("Could not access %s: %s" % (filename, e.read()))
                    return
                except:
                    self.errorList.append(error_string % filename)
                    return
                else:
                    csv = f
            else:
                try:
                    csv = open(filename, "rb")
                except IOError:
                    self.errorList.append(error_string % filename)
                    return

            # Check if the stylesheet is accessible
            try:
                stylesheet = open(task[4], "r")
            except IOError:
                self.errorList.append(error_string % task[4])
                return
            else:
                stylesheet.close()

            if tablename not in self.customised:
                # Customise the resource
                customise = current.deployment_settings.customise_resource(tablename)
                if customise:
                    from ..controller import CRUDRequest
                    request = CRUDRequest(prefix, name, current.request)
                    customise(request, tablename)
                    self.customised.append(tablename)

            extra_data = None
            if task[5]:
                try:
                    extradata = self.unescape(task[5], {"'": '"'})
                    extradata = json.loads(extradata)
                    extra_data = extradata
                except:
                    self.errorList.append("WARNING:5th parameter invalid, parameter %s ignored" % task[5])
            auth = current.auth
            auth.rollback = True
            try:
                # @todo: add extra_data and file attachments
                result = resource.import_xml(csv,
                                             source_type = "csv",
                                             stylesheet = task[4],
                                             extra_data = extra_data,
                                             )
            except SyntaxError as e:
                self.errorList.append("WARNING: import error - %s (file: %s, stylesheet: %s)" %
                                     (e, filename, task[4]))
                auth.rollback = False
                return

            error = result.error
            if error:
                # Must roll back if there was an error!
                self.errorList.append("%s - %s: %s" % (
                                      task[3], resource.tablename, error))
                errors = current.xml.collect_errors(result.error_tree)
                if errors:
                    self.errorList.extend(errors)
                current.db.rollback()
            else:
                current.db.commit()

            auth.rollback = False

            # Restore the view
            response.view = view
            end = datetime.datetime.now()
            duration = end - start
            csv_name = task[3][task[3].rfind("/") + 1:]
            duration = '{:.2f}'.format(duration.total_seconds())
            msg = "%s imported (%s sec)" % (csv_name, duration)
            self.resultList.append(msg)
            current.log.debug(msg)

    # -------------------------------------------------------------------------
    def execute_special_task(self, task):
        """
            Execute import tasks which require a custom function,
            such as import_role
        """

        start = datetime.datetime.now()
        s3 = current.response.s3
        if task[0] == 2:
            fun = task[1]
            filepath = task[2]
            extra_args = task[3]
            if filepath is None:
                if extra_args is None:
                    error = s3[fun]()
                else:
                    error = s3[fun](*extra_args)
            elif extra_args is None:
                error = s3[fun](filepath)
            else:
                error = s3[fun](filepath, *extra_args)
            if error:
                self.errorList.append(error)
            end = datetime.datetime.now()
            duration = end - start
            duration = '{:.2f}'.format(duration.total_seconds())
            msg = "%s completed (%s sec)" % (fun, duration)
            self.resultList.append(msg)
            current.log.debug(msg)

    # -------------------------------------------------------------------------
    def import_role(self, filename):
        """
            Import Roles from CSV
        """

        # Check if the source file is accessible
        try:
            open_file = open(filename, "r", encoding="utf-8")
        except IOError:
            raise
            return "Unable to open file %s" % filename

        parse_permissions = self._parse_permissions
        create_role = current.auth.s3_create_role

        reader = self.csv.DictReader(open_file)

        roles, acls, args = {}, {}, {}
        for row in reader:
            if row != None:
                row_get = row.get
                role = row_get("role")
                desc = row_get("description", "")
                rules = {}
                extra_param = {}
                controller = row_get("controller")
                if controller:
                    rules["c"] = controller
                fn = row_get("function")
                if fn:
                    rules["f"] = fn
                table = row_get("table")
                if table:
                    rules["t"] = table
                oacl = row_get("oacl")
                if oacl:
                    rules["oacl"] = parse_permissions(oacl)
                uacl = row_get("uacl")
                if uacl:
                    rules["uacl"] = parse_permissions(uacl)
                #org = row_get("org")
                #if org:
                #    rules["organisation"] = org
                #facility = row_get("facility")
                #if facility:
                #    rules["facility"] = facility
                entity = row_get("entity")
                if entity:
                    if entity == "any":
                        # Pass through as-is
                        pass
                    else:
                        # NB Entity here is *not* hierarchical!
                        try:
                            entity = int(entity)
                        except ValueError:
                            entity = self._lookup_pe(entity)
                    rules["entity"] = entity
                flag = lambda s: bool(s) and s.lower() in ("1", "true", "yes")
                hidden = row_get("hidden")
                if hidden:
                    extra_param["hidden"] = flag(hidden)
                system = row_get("system")
                if system:
                    extra_param["system"] = flag(system)
                protected = row_get("protected")
                if protected:
                    extra_param["protected"] = flag(protected)
                uid = row_get("uid")
                if uid:
                    extra_param["uid"] = uid
            if role in roles:
                acls[role].append(rules)
            else:
                roles[role] = [role, desc]
                acls[role] = [rules]
            if len(extra_param) > 0 and role not in args:
                args[role] = extra_param
        for rulelist in roles.values():
            if rulelist[0] in args:
                create_role(rulelist[0],
                            rulelist[1],
                            *acls[rulelist[0]],
                            **args[rulelist[0]])
            else:
                create_role(rulelist[0],
                            rulelist[1],
                            *acls[rulelist[0]])

        return None

    # -------------------------------------------------------------------------
    def import_user(self, filename):
        """
            Import Users from CSV with an import Prep
        """

        current.response.s3.import_prep = current.auth.s3_import_prep

        current.s3db.add_components("auth_user",
                                    auth_masterkey = "user_id",
                                    )

        user_task = [1,
                     "auth",
                     "user",
                     filename,
                     os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  "s3csv",
                                  "auth",
                                  "user.xsl"
                                  ),
                     None
                     ]
        self.execute_import_task(user_task)

    # -------------------------------------------------------------------------
    def import_feed(self, filename):
        """
            Import RSS Feeds from CSV with an import Prep
        """

        stylesheet = os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  "s3csv",
                                  "msg",
                                  "rss_channel.xsl"
                                  )

        # 1st import any Contacts
        current.response.s3.import_prep = current.s3db.pr_import_prep
        user_task = [1,
                     "pr",
                     "contact",
                     filename,
                     stylesheet,
                     None
                     ]
        self.execute_import_task(user_task)

        # Then import the Channels
        user_task = [1,
                     "msg",
                     "rss_channel",
                     filename,
                     stylesheet,
                     None
                     ]
        self.execute_import_task(user_task)

    # -------------------------------------------------------------------------
    def import_image(self,
                     filename,
                     tablename,
                     idfield,
                     imagefield,
                     ):
        """
            Import images, such as a logo or person image

            Args:
                filename: a CSV list of records and filenames
                tablename: the name of the table
                idfield: the field used to identify the record
                imagefield: the field to where the image will be added

            Example:
                bi.import_image ("org_logos.csv", "org_organisation", "name", "logo")
                ...and the file org_logos.csv may look as follows:
                id                            file
                Sahana Software Foundation    sahanalogo.jpg
                American Red Cross            icrc.gif
        """

        # Check if the source file is accessible
        try:
            open_file = open(filename, "r", encoding="utf-8")
        except IOError:
            return "Unable to open file %s" % filename

        prefix, name = tablename.split("_", 1)

        reader = self.csv.DictReader(open_file)

        db = current.db
        s3db = current.s3db
        audit = current.audit
        table = s3db[tablename]
        idfield = table[idfield]
        base_query = (table.deleted == False)
        fieldnames = [table._id.name,
                      imagefield
                      ]
        # https://github.com/web2py/web2py/blob/master/gluon/sqlhtml.py#L1947
        for field in table:
            if field.name not in fieldnames and field.writable is False \
                and field.update is None and field.compute is None:
                fieldnames.append(field.name)
        fields = [table[f] for f in fieldnames]

        # Get callbacks
        get_config = s3db.get_config
        onvalidation = get_config(tablename, "update_onvalidation") or \
                       get_config(tablename, "onvalidation")
        onaccept = get_config(tablename, "update_onaccept") or \
                   get_config(tablename, "onaccept")
        update_realm = get_config(tablename, "update_realm")
        if update_realm:
            set_realm_entity = current.auth.set_realm_entity
        update_super = s3db.update_super

        for row in reader:
            if row != None:
                # Open the file
                image = row["file"]
                try:
                    # Extract the path to the CSV file, image should be in
                    # this directory, or relative to it
                    path = os.path.split(filename)[0]
                    imagepath = os.path.join(path, image)
                    open_file = open(imagepath, "rb")
                except IOError:
                    current.log.error("Unable to open image file %s" % image)
                    continue
                image_source = BytesIO(open_file.read())
                # Get the id of the resource
                query = base_query & (idfield == row["id"])
                record = db(query).select(limitby = (0, 1),
                                          *fields).first()
                try:
                    record_id = record.id
                except AttributeError:
                    current.log.error("Unable to get record %s of the resource %s to attach the image file to" % (row["id"], tablename))
                    continue
                # Create and accept the form
                form = SQLFORM(table, record, fields=["id", imagefield])
                form_vars = Storage()
                form_vars._formname = "%s/%s" % (tablename, record_id)
                form_vars.id = record_id
                source = Storage()
                source.filename = imagepath
                source.file = image_source
                form_vars[imagefield] = source
                if form.accepts(form_vars, onvalidation=onvalidation):
                    # Audit
                    audit("update", prefix, name, form=form,
                          record=record_id, representation="csv")

                    # Update super entity links
                    update_super(table, form_vars)

                    # Update realm
                    if update_realm:
                        set_realm_entity(table, form_vars, force_update=True)

                    # Execute onaccept
                    callback(onaccept, form, tablename=tablename)
                else:
                    for (key, error) in form.errors.items():
                        current.log.error("error importing logo %s: %s %s" % (image, key, error))

        return None # no error

    # -------------------------------------------------------------------------
    @staticmethod
    def import_font(url):
        """
            Install a Font
        """

        if url == "unifont":
            #url = "http://unifoundry.com/pub/unifont-7.0.06/font-builds/unifont-7.0.06.ttf"
            #url = "http://unifoundry.com/pub/unifont-10.0.07/font-builds/unifont-10.0.07.ttf"
            url = "http://unifoundry.com/pub/unifont/unifont-13.0.01/font-builds/unifont-13.0.01.ttf"
            # Rename to make version upgrades be transparent
            filename = "unifont.ttf"
            extension = "ttf"
        else:
            filename = url.split("/")[-1]
            filename, extension = filename.rsplit(".", 1)

            if extension not in ("ttf", "gz", "zip"):
                current.log.warning("Unsupported font extension: %s" % extension)
                return

            filename = "%s.ttf" % filename

        font_path = os.path.join(current.request.folder, "static", "fonts")
        if os.path.exists(os.path.join(font_path, filename)):
            current.log.warning("Using cached copy of %s" % filename)
            return

        # Download as we have no cached copy

        # Copy the current working directory to revert back to later
        cwd = os.getcwd()

        # Set the current working directory
        os.chdir(font_path)
        try:
            _file = fetch(url)
        except URLError as exception:
            current.log.error(exception)
            # Revert back to the working directory as before.
            os.chdir(cwd)
            return

        if extension == "gz":
            import tarfile
            tf = tarfile.open(fileobj = StringIO(_file))
            tf.extractall()

        elif extension == "zip":
            import zipfile
            zf = zipfile.ZipFile(StringIO(_file))
            zf.extractall()

        else:
            f = open(filename, "wb")
            f.write(_file)
            f.close()

        # Revert back to the working directory as before.
        os.chdir(cwd)

    # -------------------------------------------------------------------------
    def import_remote_csv(self, url, prefix, resource, stylesheet):
        """ Import CSV files from remote servers """

        extension = url.split(".")[-1]
        if extension not in ("csv", "zip"):
            current.log.error("error importing remote file %s: invalid extension" % (url))
            return

        # Copy the current working directory to revert back to later
        cwd = os.getcwd()

        # Shortcut
        os_path = os.path
        os_path_exists = os_path.exists
        os_path_join = os_path.join

        # Create the working directory
        TEMP = os_path_join(cwd, "temp")
        if not os_path_exists(TEMP): # use web2py/temp/remote_csv as a cache
            import tempfile
            TEMP = tempfile.gettempdir()
        temp_path = os_path_join(TEMP, "remote_csv")
        if not os_path_exists(temp_path):
            try:
                os.mkdir(temp_path)
            except OSError:
                current.log.error("Unable to create temp folder %s!" % temp_path)
                return

        filename = url.split("/")[-1]
        if extension == "zip":
            filename = filename.replace(".zip", ".csv")
        if os_path_exists(os_path_join(temp_path, filename)):
            current.log.warning("Using cached copy of %s" % filename)
        else:
            # Download if we have no cached copy
            # Set the current working directory
            os.chdir(temp_path)
            try:
                _file = fetch(url)
            except URLError as exception:
                current.log.error(exception)
                # Revert back to the working directory as before.
                os.chdir(cwd)
                return

            if extension == "zip":
                # Need to unzip
                import zipfile
                try:
                    myfile = zipfile.ZipFile(StringIO(_file))
                except zipfile.BadZipfile as exception:
                    # e.g. trying to download through a captive portal
                    current.log.error(exception)
                    # Revert back to the working directory as before.
                    os.chdir(cwd)
                    return
                files = myfile.infolist()
                for f in files:
                    filename = f.filename
                    extension = filename.split(".")[-1]
                    if extension == "csv":
                        _file = myfile.read(filename)
                        _f = open(filename, "w")
                        _f.write(_file)
                        _f.close()
                        break
                myfile.close()
            else:
                f = open(filename, "w")
                f.write(_file)
                f.close()

            # Revert back to the working directory as before.
            os.chdir(cwd)

        task = [1, prefix, resource,
                os_path_join(temp_path, filename),
                os_path_join(current.request.folder,
                             "static",
                             "formats",
                             "s3csv",
                             prefix,
                             stylesheet
                             ),
                None
                ]
        self.execute_import_task(task)

    # -------------------------------------------------------------------------
    @staticmethod
    def import_script(filename):
        """
            Run a custom Import Script

            @ToDo: Report Errors during Script run to console better
        """

        from gluon.cfs import getcfs
        from gluon.compileapp import build_environment
        from gluon.restricted import restricted

        environment = build_environment(current.request, current.response, current.session)
        environment["current"] = current
        environment["auth"] = current.auth
        environment["db"] = current.db
        environment["gis"] = current.gis
        environment["s3db"] = current.s3db
        environment["settings"] = current.deployment_settings

        code = getcfs(filename, filename, None)
        restricted(code, environment, layer=filename)

    # -------------------------------------------------------------------------
    def import_task(self,
                    task_name,
                    args_json = None,
                    vars_json = None,
                    ):
        """
            Import a Scheduled Task
        """

        # Store current value of Bulk
        bulk = current.response.s3.bulk
        # Set Bulk to true for this parse
        current.response.s3.bulk = True
        validator = IS_JSONS3()
        if args_json:
            task_args, error = validator(args_json)
            if error:
                self.errorList.append(error)
                return
        else:
            task_args = []
        if vars_json:
            all_vars, error = validator(vars_json)
            if error:
                self.errorList.append(error)
                return
        else:
            all_vars = {}
        # Restore bulk setting
        current.response.s3.bulk = bulk

        kwargs = {}
        task_vars = {}
        options = ("function_name",
                   "start_time",
                   "next_run_time",
                   "stop_time",
                   "repeats",
                   "period", # seconds
                   "timeout", # seconds
                   "enabled", # None = Enabled
                   "group_name",
                   "ignore_duplicate",
                   "sync_output",
                   )
        for var in all_vars:
            if var in options:
                kwargs[var] = all_vars[var]
            else:
                task_vars[var] = all_vars[var]

        current.s3task.schedule_task(task_name.split(os.path.sep)[-1], # Strip the path
                                     args = task_args,
                                     vars = task_vars,
                                     **kwargs
                                     )

    # -------------------------------------------------------------------------
    def import_xml(self,
                   filepath,
                   prefix,
                   resourcename,
                   dataformat,
                   source_type = None,
                   ):
        """
            Import XML data using an XSLT: static/formats/<dataformat>/import.xsl
            Setting the source_type is possible
        """

        # Remove any spaces and enclosing double quote
        prefix = prefix.strip('" ')
        resourcename = resourcename.strip('" ')

        try:
            source = open(filepath, "rb")
        except IOError:
            error_string = "prepopulate error: file %s missing"
            self.errorList.append(error_string % filepath)
            return

        stylesheet = os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  dataformat,
                                  "import.xsl")
        try:
            xslt_file = open(stylesheet, "r")
        except IOError:
            error_string = "prepopulate error: file %s missing"
            self.errorList.append(error_string % stylesheet)
            return
        else:
            xslt_file.close()

        tablename = "%s_%s" % (prefix, resourcename)
        resource = current.s3db.resource(tablename)

        if tablename not in self.customised:
            # Customise the resource
            customise = current.deployment_settings.customise_resource(tablename)
            if customise:
                from ..controller import CRUDRequest
                request = CRUDRequest(prefix, resourcename, current.request)
                customise(request, tablename)
                self.customised.append(tablename)

        auth = current.auth
        auth.rollback = True
        try:
            resource.import_xml(source,
                                stylesheet = stylesheet,
                                source_type = source_type,
                                )
        except SyntaxError as e:
            self.errorList.append("WARNING: import error - %s (file: %s, stylesheet: %s/import.xsl)" %
                                 (e, filepath, dataformat))
            auth.rollback = False
            return

        if not resource.error:
            current.db.commit()
        else:
            # Must roll back if there was an error!
            error = resource.error
            self.errorList.append("%s - %s: %s" % (
                                  filepath, tablename, error))
            errors = current.xml.collect_errors(resource)
            if errors:
                self.errorList.extend(errors)
            current.db.rollback()

        auth.rollback = False

    # -------------------------------------------------------------------------
    @staticmethod
    def _lookup_pe(entity):
        """
            Convert an Entity to a pe_id
            - helper for import_role
            - assumes org_organisation.name unless specified
            - entity needs to exist already
        """

        if "=" in entity:
            pe_type, value = entity.split("=")
        else:
            pe_type = "org_organisation.name"
            value = entity
        pe_tablename, pe_field =  pe_type.split(".")

        table = current.s3db.table(pe_tablename)
        record = current.db(table[pe_field] == value).select(table.pe_id,
                                                             limitby = (0, 1)
                                                             ).first()
        try:
            pe_id = record.pe_id
        except AttributeError:
            current.log.warning("import_role cannot find pe_id for %s" % entity)
            pe_id = None

        return pe_id

    # -------------------------------------------------------------------------
    @staticmethod
    def _parse_permissions(rule):
        """
            Convert a permissions rule into its binary representation

            Args:
                rule: |-separated permission names (str)

            Returns:
                int: the binary representation of the rule (bits)
        """

        permissions = current.auth.permission

        bits = 0
        for name in rule.split("|"):
            if name == "READ":
                bits |= permissions.READ
            elif name == "CREATE":
                bits |= permissions.CREATE
            elif name == "UPDATE":
                bits |= permissions.UPDATE
            elif name == "DELETE":
                bits |= permissions.DELETE
            elif name == "REVIEW":
                bits |= permissions.REVIEW
            elif name == "APPROVE":
                bits |= permissions.APPROVE
            elif name == "PUBLISH":
                bits |= permissions.PUBLISH
            elif name == "ALL":
                bits |= permissions.ALL

        return bits

# END =========================================================================
