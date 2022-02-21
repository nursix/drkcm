Bulk Importer
=============

The **BulkImporter** is a tool to run a series of data import tasks
from a configuration file. It is most commonly used during the first
run of the application, to pre-populate the database with essential
data (a process called *prepop*).

The individual import task handlers of the BulkImporter can also
be used standalone, e.g. in upgrade/maintenance scripts, or for
database administration from the CLI.

Configuration File
------------------

Configuration files for the BulkImporter are CSV-like files that
must be named ``task.cfg``, and are typically placed in the template
directory to be picked up by the first-run script.

.. code-block::
   :caption: Example of tasks.cfg

   # Roles
   *,import_roles,auth_roles.csv
   # GIS
   gis,marker,gis_marker.csv,marker.xsl
   gis,config,gis_config.csv,config.xsl
   gis,hierarchy,gis_hierarchy.csv,hierarchy.xsl
   gis,layer_feature,gis_layer_feature.csv,layer_feature.xsl

.. tip::

   This file format differs from normal CSV in that it allows for
   comments, i.e. everything from ``#`` to the end of the line is
   ignored by the parser.

Each line in the file specifies a *task* for the BulkImporter. The
general format of a task is:

.. code-block::

   <prefix>,<name>,<filename>,<xslt_path>

By default, tasks is the S3CSV import handler (*import_csv*). In this
case, the task parameters are:

.. _taskscfg:

+-----------+---------------------------------------------------------------------------------------------------+
|Parameter  |Meaning                                                                                            |
+===========+===================================================================================================+
|prefix     |The module prefix of the table name (e.g. *org*)                                                   |
+-----------+---------------------------------------------------------------------------------------------------+
|name       |The table name without module prefix (e.g. *organisation*)                                         |
+-----------+---------------------------------------------------------------------------------------------------+
|filename   | | - the source file name (if located in the same directory as tasks.cfg), or                      |
|           | | - a file system path relative to *modules/templates*, or                                        |
|           | | - an absolute file system path, or                                                              |
|           | | - a HTTP/HTTPs URL to fetch the file from                                                       |
+-----------+---------------------------------------------------------------------------------------------------+
|stylesheet | | - the name of the transformation stylesheet (if located in *static/formats/s3csv/<prefix>*), or |
|           | | - a file system path relative to *static/formats/s3csv*, or                                     |
|           | | - a file system path starting with ``./`` relative to the location of the CSV file              |
+-----------+---------------------------------------------------------------------------------------------------+

Import Handlers
---------------

It is possible to override the default handler for a task with
a *prefix* ``*``, and then specifying the import handler with
the *name* parameter, i.e.:

.. code-block::

   *,<handler>,<filename>,<arg>,<arg>,...

In this case, the number and meaning of the further parameters depends
on the respective handler:

+--------------+--------------------------------------------------------------------------------+
|Handler       |Task Format, Action                                                             |
+==============+================================================================================+
|import_xml    | | ``*,import_xml,<filename>,<prefix>,<name>,<dataformat>,<source_type>``       |
|              | | - import XML/JSON data using *static/formats/<dataformat>/import.xsl*        |
|              | | - *source_type* can be ``xml`` or ``json``                                   |
+--------------+--------------------------------------------------------------------------------+
|import_roles  | | ``*,import_roles,<filename>``                                                |
|              | | - import user roles and permissions from CSV with a special format           |
+--------------+--------------------------------------------------------------------------------+
|import_users  | | ``*,import_roles,<filename>``                                                |
|              | | - import user accounts with special pre-processing of the data               |
+--------------+--------------------------------------------------------------------------------+
|import_images | | ``*,import_images,<filename>,tablename,keyfield,imagefield``                 |
|              | | - import image files and store them in record of the specified table         |
|              | | - source file is a CSV file with columns *id* and *file*                     |
|              | | - records are identified by *keyfield* matching the *id* in the source file  |
+--------------+--------------------------------------------------------------------------------+
|schedule_task | | ``*,schedule_task,,taskname,args,vars,params``                               |
|              | | - schedule a task with the scheduler                                         |
|              | | - *args*, *vars* and *params* are JSON strings, but can use single quotes    |
|              | | - *args* (list) and *vars* (dict) are passed to the task function            |
|              | | - *params* (dict) specifies the task parameters, e.g. frequency of execution |
|              | | - second task parameter (filename) is empty here (not a typo)!               |
+--------------+--------------------------------------------------------------------------------+

It is possible to run the import task handlers standalone, e.g.:

.. code-block:: python
   :caption: Running a task handler function standalone
   :emphasize-lines: 3

   from core import BulkImporter
   path = os.path.join(current.request.folder, "modules", "templates", "MYTEMPLATE", "auth_roles.csv")
   error = BulkImporter.import_roles(path)

The arguments for the handler function are the same as for the task line in the tasks.cfg
(except ``*`` and handler name of course). All handler functions return an error message
upon failure (or a list of error messages, if there were multiple errors) - or None on success.

.. note::

   When running task handlers standalone (e.g. in a script, or from the CLI), the
   import result will **not** automatically be committed - an explicit ``db.commit()``
   is required.

Task Runner
-----------

The task runner is a BulkImporter **instance**. To run tasks, the ``perform_tasks`` method
is called with the path where the *tasks.cfg* file is located:

.. code-block:: python

   from core import BulkImporter
   bi = BulkImporter()

   path = os.path.join(current.request.folder, "modules", "templates", "MYTEMPLATE")
   bi.perform_tasks(path)

.. important::

   The task runner automatically commits all imports - i.e. *perform_tasks* cannot be rolled back!

Template-specific Task Handlers
-------------------------------

It is possible for templates to add further task handlers to the BulkImporter,
e.g. to perform special (import or other) tasks during prepop.

.. code-block:: python
   :caption: Template-specific task handler for the BulkImporter, in config.py

   # Define the task handler:
   # - must take filename as first argument
   # - further arguments are freely definable, but tasks must match
   #   this signature
   def special_import_handler(filename, arg1, arg2):
       ...do something with filename and args

   # Configure a dict {name: function} for template-specific task handlers:
   settings.base.import_handlers = {"import_special": special_import_handler}

This also allows to override existing task handlers with template-specific
variants.

With this, tasks for the new handler can be added to tasks.cfg like:

.. code-block::

   *,import_special,<filename>,<arg1>,<arg2>

.. note::

   When received by the handler, the *filename* will be completed with a path,
   (see interpretation of *filename* in :ref:`tasks.cfg <taskscfg>`). All other
   parameters are passed-in unaltered.

   However, the *filename* parameter can be left empty, and/or get ignored by
   the task handler, if a file name is not required for the task.
