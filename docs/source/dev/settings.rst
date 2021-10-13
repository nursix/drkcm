Application Settings
====================

current
-------

The *current* object holds thread-local global variables. It can be imported into any context:

.. code-block:: python

   from gluon import current

.. table:: Objects accessible through current
   :widths: auto

   ===========================  =================  ============================================
   Attribute                    Type               Explanation
   ===========================  =================  ============================================
   current.db                   DAL                the database (DAL)
   current.s3db                 S3Model            the model loader (S3Model)
   current.config               S3Config           deployment settings
   current.deployment_settings  S3Config           alias for current.config
   current.auth                 AuthS3             global authentication/authorisation service
   current.gis                  GIS                global GIS service
   current.msg                  S3Msg              global messaging service
   current.xml                  S3XML              global XML decoder/encoder service
   current.request              Request            web2py's global request object
   current.response             Response           web2py's global response object
   current.T                    TranslatorFactory  String Translator (for i18n)
   current.messages             Messages           Common labels (internationalised)
   current.ERROR                Messages           Common error messages (internationalised)
   ===========================  =================  ============================================

Global Config
-------------

Many elements of Eden ASP can be controlled by configuration settings.

These configuration settings are stored in a global *S3Config* instance - which
is accessible through *current.config* (alias *current.deployment_settings*).

Templates
---------

*current.config* comes with meaningful defaults, but some of them may need
to be adjusted to enable/disable, configure, customize or extend features in
the context of the specific application.

These application settings are implemented as configuration **templates**,
which are Python packages located in the *modules/templates* directory:

.. image:: template_location.png
   :align: center

A template package must contain a module *config.py* which defines a *config*-function :

.. code-block:: python
   :caption: modules/templates/MYAPP/config.py

   def config(settings):

       T = current.T

       settings.base.system_name = T("My Application")
       settings.base.system_name_short = T("MyApp")

       ...

The *config* function is called with the *current.config* instance as parameter,
so it can modify the global settings as needed by the application.

.. note::
   The template directory must also contain an *__init__.py* file (which can
   be empty) in order to become a Python package!

Deployment Settings
-------------------

Some settings, e.g. database credentials, must be configured for the
individual installation. These *deployment settings* are configured
in a machine-specific configuration file (*models/000_config.py*).

.. note::
   If *models/000_config.py* does not exist, an annotated skeleton is
   automatically generated when Eden ASP is first started. This
   skeleton file can also be found in the *modules/templates* directory.

The *000_config.py* is a Python script consisting of three sections:

  - machine-specific settings
  - template import
  - settings after template import (can override template settings)

.. code-block:: python
   :caption: models/000_config.py (partial example)

   # -*- coding: utf-8 -*-

   """
       Machine-specific settings
   """

   # Remove this line when this file is ready for 1st run
   FINISHED_EDITING_CONFIG_FILE = True

   # Select the Template
   settings.base.template = "MYAPP"

   # Database settings
   settings.database.db_type = "postgres"
   #settings.database.host = "localhost"
   #settings.database.port = 3306
   settings.database.database = "myapp"
   #settings.database.username = "eden"
   #settings.database.password = "password"

   # Do we have a spatial DB available?
   settings.gis.spatialdb = True

   settings.base.migrate = True
   #settings.base.fake_migrate = True

   settings.base.debug = True
   #settings.log.level = "WARNING"
   #settings.log.console = False
   #settings.log.logfile = None
   #settings.log.caller_info = True

   # =============================================================================
   # Import the settings from the Template
   #
   settings.import_template()

   # =============================================================================
   # Over-rides to the Template may be done here
   #
   # After 1st_run, set this for Production
   #settings.base.prepopulate = 0

   # =============================================================================
   VERSION = 1

   # END =========================================================================
