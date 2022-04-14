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

__all__ = ("BulkImporter",
           )

import csv
import datetime
import json
import os

from io import BytesIO
from urllib.error import URLError
from xml.sax.saxutils import unescape

from gluon import current
from gluon.storage import Storage
from gluon.tools import fetch

from .validators import IS_JSONS3, JSONERRORS

EMPTYLINE = [None, None, None, None, None]

# =============================================================================
class BulkImporter:
    """
        Tool to perform a series of imports from a task file, usually to
        pre-populate the database; import methods can also be run standalone
        (e.g. from CLI or scripts) for system administration/maintenance
        purposes.
    """

    def __init__(self):

        self._handlers = None

    # -------------------------------------------------------------------------
    # Task Runner
    #
    @property
    def handlers(self):
        """
            Returns the import handler registry for this instance

            Returns:
                a dict {name: function}
        """

        handlers = self._handlers
        if not handlers:
            handlers = {"import_feeds": self.import_feeds,
                        "import_font": self.import_font,
                        "import_images": self.import_images,
                        "import_roles": self.import_roles,
                        "schedule_task": self.schedule_task,
                        "import_users": self.import_users,
                        "import_xml": self.import_xml,
                        }

            # Template-defined task handlers
            custom = current.deployment_settings.get_base_import_handlers()
            if custom:
                handlers.update(custom)

            self._handlers = handlers
        return handlers

    # -------------------------------------------------------------------------
    def perform_tasks(self, path):
        """
            Parses a tasks.cfg file, and runs all import tasks specified by it

            Args:
                path: the path to the tasks.cfg file (without filename)

            Returns:
                a list of error messages (empty list if there were no errors)
        """

        errors = []
        db = current.db

        for task in self.parse_task_config(path):
            task_type = task[0]
            if not task_type:
                errors.append(task[1])
                continue

            start = datetime.datetime.now()

            if task_type == 1:
                error = self.import_csv(*(task[1:6]))
                if isinstance(error, list):
                    errors.extend(error)
                elif error:
                    errors.append(error)
                else:
                    db.commit()
                csv_name = os.path.split(task[3])[1]
                msg = "%s imported (%%s sec)" % csv_name

            elif task_type == 2:
                handler = self.handlers.get(task[1])
                if not handler:
                    errors.append("Invalid task type %s" % task[1])
                    continue
                try:
                    error = handler(*task[2:])
                except TypeError as e:
                    errors.append(str(e))
                else:
                    if isinstance(error, list):
                        errors.extend(error)
                    elif error:
                        errors.append(error)
                    else:
                        db.commit()
                msg = "%s completed (%%s sec)" % task[1]

            duration = datetime.datetime.now() - start
            current.log.debug(msg % '{:.2f}'.format(duration.total_seconds()))

        return errors

    # -------------------------------------------------------------------------
    # Task Config Parser
    #
    @classmethod
    def parse_task_config(cls, path):
        """
            Reads a tasks.cfg file, collects import tasks and resolves the
            file paths for standard CSV imports

            Args:
                path: the path to the tasks.cfg file (without filename)

            Returns:
                a list of import tasks (tuples); a tuple with None as
                first element indicates an error (second element is the
                error message then)
        """

        strip_comments = lambda row: row.split("#", 1)[0]
        clean = lambda line: [item.strip('" ') for item in line]

        tasks = []
        with open(os.path.join(path, "tasks.cfg"), "r") as source:
            for line in csv.reader(filter(strip_comments, source)):
                task = cls.parse_task_line(path, clean(line))
                tasks.append(task)
        return tasks

    # -------------------------------------------------------------------------
    @classmethod
    def parse_task_line(cls, path, line):
        """
            Parses a line in the task configuration, and completes file paths

            Args:
                path: the path of the task configuration file
                line: the CSV line to parse (as list of strings)

            Returns:
                - the task as tuple (type, *params)
                - (None, error) if the line is invalid
        """

        folder = current.request.folder

        if line and line[0] == "*":
            # Import using BulkImporter handler (*,handler,filename,args)
            handler, filename = (line + EMPTYLINE)[1:3]
            if not handler or filename is None:
                return (None, "Missing argument(s) in task %s (line ignored)" % str(line))

            # Source file location
            filepath = cls._addpath(path, filename)

            return (2, handler, filepath, *line[3:])
        else:
            # Import using XMLImporter (mod,res,csv_name,xslt_name,extra_data)
            mod, res, csv_name, xslt_name, extra_data = (line + EMPTYLINE)[:5]

            if not all((mod, res, csv_name, xslt_name)):
                return (None, "Missing argument(s) in task %s (line ignored)" % str(line))

            # CSV file location
            csv_path = cls._addpath(path, csv_name)

            # Transformation stylesheet location
            base_path = os.path.join(folder, "static", "formats", "s3csv")
            sub, filename = os.path.split(xslt_name)
            if sub:
                if sub[0] == ".":
                    # Alternative location relative to CSV file
                    location = (path, sub, filename)
                else:
                    # Alternative location relative to base path
                    location = (base_path, sub, filename)
            else:
                # Standard location
                location = (base_path, mod, filename)
            xslt_path = os.path.normpath(os.path.join(*location))
            if not os.path.exists(xslt_path):
                return (None, "Transformation stylesheet not found: %s" % xslt_path)

            return (1, mod, res, csv_path, xslt_path, extra_data)

    # -------------------------------------------------------------------------
    # Import Handlers
    #
    @classmethod
    def import_csv(cls, prefix, name, csv_path, xslt_path, extra_data=None):
        """
            Imports CSV data, using S3CSV transformation stylesheet

            Args:
                prefix: the table name prefix
                name: the table name without prefix
                csv_path: the path to the source file, a local file system path
                          or a http/https URL
                xslt_path: the path to the transformation stylesheet, a local
                           file system path, or a http/https URL
                extra_data: extra data to add to the CSV (as JSON string)

            Returns:
                error message(s) on failure, otherwise None
        """

        current.auth.ignore_min_password_length()

        s3db = current.s3db

        # Customise and instantiate the resource
        tablename = "%s_%s" % (prefix, name)
        if not s3db.customised(tablename):
            from ..controller import CRUDRequest
            r = CRUDRequest(prefix, name, current.request)
            r.customise_resource(tablename)
        try:
            resource = s3db.resource(tablename)
        except AttributeError:
            return "Table %s not found, import skipped" % tablename

        # Decode extra data
        if extra_data:
            try:
                decoded = json.loads(unescape(extra_data, {"'": '"'}))
            except JSONERRORS:
                return "Invalid extra data JSON: %s" % str(extra_data)
            else:
                if not isinstance(decoded, dict):
                    return "Invalid extra data type (dict expected): %s" % str(extra_data)
                else:
                    extra_data = decoded

        # Detect ZIP file extension
        sp = csv_path.rsplit(".", 1)
        zipped = len(sp) > 1 and sp[-1] == "zip"

        # Import from source
        auth = current.auth
        auth.rollback = True
        try:
            with cls._load(csv_path) as source:
                if zipped:
                    data = cls._extract_from_zip(source)
                    if data is None:
                        raise IOError("Could not unpack %s" % csv_path)
                else:
                    data = source
                result = resource.import_xml(data,
                                             source_type = "csv",
                                             stylesheet = xslt_path,
                                             extra_data = extra_data,
                                             )
        except IOError as e:
            return str(e)
        except SyntaxError as e:
            return "Failed to import %s (%s): %s" % (csv_path, xslt_path, e)
        finally:
            auth.rollback = False

        # Collect import errors
        error = result.error
        if error:
            errors = ["%s - %s: %s" % (csv_path, tablename, error)]
            xml_errors = current.xml.collect_errors(result.error_tree)
            if xml_errors:
                errors.extend(xml_errors)
            # Must roll back if there was an error!
            current.db.rollback()
        else:
            errors = None

        return errors

    # -------------------------------------------------------------------------
    @classmethod
    def import_xml(cls,
                   filepath,
                   prefix,
                   name,
                   dataformat = "xml",
                   source_type = None,
                   ):
        """
            Imports XML data, using an static/formats/<dataformat>/import.xsl

            Args:
                filepath: the path to source file, a local file system path
                          or a http/https URL
                prefix: the table name prefix
                name: the table name without prefix
                dataformat: the data format (defaults to S3XML)
                source_type: the source type (xml|json)

            Returns:
                error message(s) on failure, otherwise None
        """

        if not dataformat:
            return "Invalid data format %s" % dataformat

        # XSLT path
        if dataformat not in ("xml", "s3json"):
            xslt_path = os.path.join(current.request.folder,
                                     "static",
                                     "formats",
                                     dataformat,
                                     "import.xsl",
                                     )
        else:
            # Native format, no transformation required
            xslt_path = None

        # Customise and instantiate target resource
        s3db = current.s3db
        tablename = "%s_%s" % (prefix, name)
        if not s3db.customised(tablename):
            from ..controller import CRUDRequest
            r = CRUDRequest(prefix, name, current.request)
            r.customise_resource(tablename)
        try:
            resource = s3db.resource(tablename)
        except AttributeError:
            return "Table %s not found, import skipped" % tablename

        # Detect ZIP file extension
        sp = filepath.rsplit(".", 1)
        zipped = len(sp) > 1 and sp[-1] == "zip"

        # Import from source
        auth = current.auth
        auth.rollback = True
        try:
            with cls._load(filepath) as source:
                if zipped:
                    data = cls._extract_from_zip(source, dataformat=dataformat)
                    if data is None:
                        raise IOError("Could not unpack %s" % filepath)
                else:
                    data = source
                result = resource.import_xml(data,
                                             source_type = source_type,
                                             stylesheet = xslt_path,
                                             )
        except IOError as e:
            return str(e)
        except SyntaxError as e:
            return "Failed to import %s (%s): %s" % (filepath, xslt_path, e)
        finally:
            auth.rollback = False

        if resource.error:
            # Must roll back if there was an error!
            errors = ["%s - %s: %s" % (filepath, tablename, result.error)]
            xml_errors = current.xml.collect_errors(resource)
            if xml_errors:
                errors.extend(xml_errors)
            current.db.rollback()
        else:
            errors = None

        return errors

    # -------------------------------------------------------------------------
    @classmethod
    def import_roles(cls, filepath):
        """
            Imports user roles and permissions from CSV

            Args:
                filepath: the path to source file

            Returns:
                error message(s) on error, otherwise None
        """

        try:
            with open(filepath, "r", encoding="utf-8") as source:
                reader = csv.DictReader(source)

                roles = {}
                for row in reader:
                    cls._add_rule(roles, row)

                create_role = current.auth.s3_create_role
                for name, role in roles.items():
                    create_role(name,
                                role.get("description"),
                                *role["rules"],
                                **role["kwargs"],
                                )
        except IOError:
            return "Unable to open file %s" % filepath

        return None

    # -------------------------------------------------------------------------
    @classmethod
    def import_users(cls, filepath):
        """
            Imports user accounts from CSV

            Args:
                filepath: the path to source file

            Returns:
                error message(s) on error, otherwise None
        """

        xslt_path = os.path.join(current.request.folder,
                                 "static",
                                 "formats",
                                 "s3csv",
                                 "auth",
                                 "user.xsl"
                                 )

        s3db = current.s3db
        auth = current.auth
        s3db.add_components("auth_user", auth_masterkey="user_id")
        s3db.configure("auth_user", onaccept=lambda f: auth.s3_approve_user(f.vars))

        s3 = current.response.s3
        s3.import_prep = current.auth.s3_import_prep
        error = cls.import_csv("auth",
                               "user",
                               filepath,
                               xslt_path,
                               )
        s3.import_prep = None

        return error

    # -------------------------------------------------------------------------
    @classmethod
    def import_feeds(cls, filepath):
        """
            Imports RSS feeds from CSV

            Args:
                filepath: the path to source file

            Returns:
                error message(s) on error, otherwise None
        """

        s3 = current.response.s3

        xslt_path = os.path.join(current.request.folder,
                                  "static",
                                  "formats",
                                  "s3csv",
                                  "msg",
                                  "rss_channel.xsl"
                                  )

        import_csv = cls.import_csv

        # Import contact data
        s3.import_prep = current.s3db.pr_import_prep
        error = import_csv("pr", "contact", filepath, xslt_path)
        s3.import_prep = None
        if error:
            return error

        # Import Messaging Channels
        error = import_csv("msg", "rss_channel", filepath, xslt_path)

        return error

    # -------------------------------------------------------------------------
    @classmethod
    def import_images(cls, filepath, tablename, keyfield, imagefield):
        """
            Imports images, such as organisation logos

            Args:
                filepath: the path to source file
                tablename: the name of the table
                keyfield: the field used to identify the record
                imagefield: the field to store the image

            Returns:
                error message(s) on error, otherwise None

            Example:
                bi.import_images("org_logos.csv", "org_organisation", "name", "logo")

                ...with a file "org_logos.csv" like:

                id, file
                Sahana Software Foundation, sahanalogo.jpg
                American Red Cross, icrc.gif
        """

        table = current.s3db.table(tablename)
        if not table:
            return "Table not found: %s" % tablename
        if keyfield not in table.fields:
            return "Invalid key field: %s" % keyfield

        key = table[keyfield]
        base_query = (table.deleted == False)

        path = os.path.split(filepath)[0]
        errors = []
        try:
            with open(filepath, "r", encoding="utf-8") as source:
                data = csv.DictReader(source)

                for item in data:
                    if not item:
                        continue

                    value = item.get("id")
                    image = item.get("file")
                    if not value or not image:
                        continue

                    image = os.path.join(path, image)
                    error = None
                    try:
                        cls._store_image(table,
                                         base_query & (key==value),
                                         table[imagefield],
                                         image,
                                         )
                    except KeyError as e:
                        # Record not found
                        error = "Record %s=%s not found" % (keyfield, value)
                    except (IOError, AttributeError, ValueError) as e:
                        # Other error
                        error = "Image import failed: %s" % str(e)
                    if error:
                        errors.append(error)

        except IOError as e:
            return "Image list not accessible: %s" % e

        return errors if errors else None

    # -------------------------------------------------------------------------
    @staticmethod
    def import_font(filepath, url):
        """
            Installs a font in static/fonts

            Args:
                filepath: path to the source file (ignored)
                url: the font file url, or keyword "unifont" to fetch from
                     standard location

            Returns:
                error message(s) on error, otherwise None
        """

        if url == "unifont":
            url = "http://unifoundry.com/pub/unifont/unifont-14.0.01/font-builds/unifont-14.0.01.ttf"
            filename, extension = "unifont.ttf", "ttf"
        else:
            filename, extension = url.split("/")[-1].rsplit(".", 1)
            if extension not in ("ttf", "gz", "zip"):
                return "Unsupported font extension: %s" % extension
            filename = "%s.ttf" % filename

        font_path = os.path.join(current.request.folder, "static", "fonts")
        if os.path.exists(os.path.join(font_path, filename)):
            # Already installed
            current.log.warning("Using cached copy of %s" % filename)
            return None

        # Change to the font directory
        cwd = os.getcwd()
        os.chdir(font_path)

        # Fetch the font file
        try:
            stream = fetch(url)
        except URLError as e:
            os.chdir(cwd)
            return str(e)

        # Unpack and store the font
        try:
            if extension == "gz":
                import tarfile
                tf = tarfile.open(fileobj=stream)
                tf.extractall()
            elif extension == "zip":
                import zipfile
                zf = zipfile.ZipFile(stream)
                zf.extractall()
            else:
                f = open(filename, "wb")
                f.write(stream)
                f.close()
        finally:
            # Revert back to the working directory as before.
            os.chdir(cwd)

        return None

    # -------------------------------------------------------------------------
    @staticmethod
    def schedule_task(filepath, task_name, args_json=None, vars_json=None, params=None):
        """
            Schedules a background task

            Args:
                filepath: path to the source file (ignored)
                task_name: the task name
                args_json: the task arguments (args), as JSON string
                vars_json: the task keyword arguments (vars), as JSON string
                params: scheduler parameters, as JSON string

            Returns:
                error message(s) on error, otherwise None
        """

        # Decode args/vars
        validator = IS_JSONS3(fix_quotes=True)

        # Arguments to pass to the task function
        if args_json:
            task_args, error = validator(args_json)
            if error:
                return error
        else:
            task_args = []

        # Keyword arguments to pass to the task function
        if vars_json:
            task_vars, error = validator(vars_json)
            if error:
                return error
        else:
            task_vars = {}

        # Parameters for s3task.schedule_task()
        if params:
            kwargs, error = validator(params)
            if error:
                return error
            # Supported options
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
            kwargs = {k: v for k, v in kwargs.items() if k in options}
        else:
            kwargs = {}

        current.s3task.schedule_task(task_name,
                                     args = task_args,
                                     vars = task_vars,
                                     **kwargs)
        return None

    # -------------------------------------------------------------------------
    # Utility Methods
    #
    @staticmethod
    def _addpath(path, filename):
        """
            Adds the path to a source file

            Args:
                path: the base path (i.e. where the tasks.cfg is)
                filename: the file name as specified in tasks.cfg

            Returns:
                the updated file name
        """

        if filename:
            sp = filename.split("://", 1)
            if sp[0] in ("http", "https") and len(sp) > 1:
                filepath = filename
            else:
                template, filename = os.path.split(filename)
                if template:
                    # File in other template
                    path = os.path.join(current.request.folder,
                                        "modules",
                                        "templates",
                                        template,
                                        )
                filepath = os.path.join(path, filename)
        else:
            filepath = None

        return filepath

    # -------------------------------------------------------------------------
    @staticmethod
    def _load(path):
        """
            Opens an import source from path

            Args:
                path: local file path, or a http/https URL

            Returns:
                binary, file-like object with the source data

            Raises:
                IOError: if the source is not accessible
        """

        sp = path.split("://", 1)
        if sp[0] in ("http", "https") and len(sp) > 1:
            import requests, tempfile
            try:
                r = requests.get(path, stream=True)
            except requests.RequestException as e:
                raise IOError("Failed to load source %s: %s" % (path, type(e).__name__))
            else:
                source = tempfile.TemporaryFile()
                for chunk in r.iter_content(chunk_size=65536):
                    source.write(chunk)
                source.seek(0)
        else:
            source = open(path, "rb")

        return source

    # -------------------------------------------------------------------------
    @staticmethod
    def _extract_from_zip(source, dataformat="csv"):
        """
            Extracts a source file from a ZIP archive

            Args:
                source: the ZIP archive (file-like object, or file name)
                dataformat: the format extension of the source file

            Returns:
                BytesIO, the data from the first file with a matching
                format extension found in the archive, or None if the
                archive is not readable or does not contain any file
                with a matching extension
        """

        import zipfile

        data = None

        try:
            with zipfile.ZipFile(source) as zipped:
                for f in zipped.infolist():
                    filename = f.filename
                    extension = filename.split(".")[-1]
                    if extension == dataformat:
                        data = BytesIO(zipped.read(filename))
                        break
        except zipfile.BadZipfile:
            pass

        return data

    # -------------------------------------------------------------------------
    @classmethod
    def _add_rule(cls, roles, row):
        """
            Parses a single CSV row for import_roles, and updates the
            roles-dict with the data

            Args:
                roles: the roles-dict to update
                row: the CSV row
        """

        if not row:
            return
        row_get = row.get

        name = row_get("role")
        if name not in roles:
            role = roles[name] = {"kwargs": {}, "rules": []}
        else:
            role = roles[name]

        # Update description
        description = row_get("description")
        if description:
            role["description"] = description

        # Update role keyword args (uid and flags)
        kwargs = role["kwargs"]

        uid = row_get("uid")
        if uid:
            kwargs["uid"] = uid

        for flag in ("hidden", "system", "protected"):
            value = row_get(flag)
            if value:
                if value.lower() in ("true", "yes", "1"):
                    kwargs[flag] = True
                elif value.lower() in ("false", "no", "0"):
                    kwargs[flag] = False

        # Parse the rule
        rule = {param: row_get(keyword) or None
                for keyword, param in (("controller", "c"),
                                       ("function", "f"),
                                       ("table", "t"),
                                       )}
        if any(rule.values()):
            parse_permissions = cls._parse_permissions
            for keyword in ("oacl", "uacl"):
                value = row_get(keyword)
                if value:
                    rule[keyword] = parse_permissions(value)

            entity = row_get("entity")
            if entity:
                if entity != "any":
                    try:
                        entity = int(entity)
                    except ValueError:
                        entity = cls._lookup_pe(entity)
                rule["entity"] = entity

            role["rules"].append(rule)

    # -------------------------------------------------------------------------
    @staticmethod
    def _parse_permissions(rule):
        """
            Converts a permissions rule into its binary representation

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

    # -------------------------------------------------------------------------
    @staticmethod
    def _lookup_pe(entity):
        """
            Converts an Entity to a pe_id
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
    def _store_image(table, query, field, filepath):
        """
            Store an image in a record

            Args:
                table: the Table
                query: the Query to retrieve the record
                field: the Field to store the image
                filepath: the path to the image file

            Raises:
                KeyError: if the record was not found
                ValueError: if the image is invalid
        """

        db = current.db
        s3db = current.s3db
        audit = current.audit

        table_id = table._id

        # Get the record
        record = db(query).select(table_id, limitby=(0, 1)).first()
        if not record:
            raise KeyError("Record not found")
        record_id = record[table_id]

        filename = os.path.split(filepath)[1]
        with open(filepath, "rb") as image:
            # Validate the image
            error = field.validate(Storage(filename=filename, file=image))[1]
            if error:
                raise ValueError("Invalid image %s: %s" % (filename, error))

            # Store it in the record
            data = {field.name: field.store(image, filename)}
            record.update_record(**data)

            # Postprocess the record update
            prefix, name = str(table).split("_", 1)
            audit("update", prefix, name,
                  form = Storage(vars=Storage(record)),
                  record = record_id,
                  representation = "csv",
                  )
            s3db.update_super(table, record)
            s3db.onaccept(table, record, method="update")

# END =========================================================================
