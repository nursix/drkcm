# =============================================================================
#    Import Modules
#    Configure the Database
#    Instantiate Classes
# =============================================================================

if settings.get_L10n_languages_readonly():
    # Make the Language files read-only for improved performance (default)
    T.is_writable = False

get_vars = request.get_vars

# Are we running in debug mode?
settings.check_debug()

import datetime
import json

# -----------------------------------------------------------------------------
# Database Configuration
#
migrate = settings.get_base_migrate()
fake_migrate = settings.get_base_fake_migrate()

(db_type, db_string, pool_size) = settings.get_database_string()
if migrate:
    if db_type == "mysql":
        check_reserved = ["postgres"]
    elif db_type == "postgres":
        check_reserved = ["mysql"]
    else:
        check_reserved = ["mysql", "postgres"]
else:
    check_reserved = []

try:
    db = DAL(db_string,
             check_reserved = check_reserved,
             pool_size = pool_size,
             migrate_enabled = migrate,
             fake_migrate_all = fake_migrate,
             lazy_tables = not migrate,
             ignore_field_case = db_type != "postgres",
             )
except:
    db_location = db_string.split("@", 1)[1]
    raise HTTP(503, "Cannot connect to %s Database: %s" % (db_type, db_location))

current.db = db
db.set_folder("upload")

# -----------------------------------------------------------------------------
# Sessions Storage
#
if settings.get_base_session_db():
    # Store sessions in the database to avoid a locked session
    session.connect(request, response, db)
elif settings.get_base_session_memcache():
    # Store sessions in Memcache
    from gluon.contrib.memcache import MemcacheClient
    cache.memcache = MemcacheClient(request,
                                    [settings.get_base_session_memcache()])
    from gluon.contrib.memdb import MEMDB
    session.connect(request, response, db=MEMDB(cache.memcache))
#else:
    ## Default to filesystem
    # pass

# -----------------------------------------------------------------------------
# Instantiate Classes from Modules
# - store instances in current to be accessible from other modules
#
from gluon.tools import Mail
mail = current.mail = Mail()

from gluon.storage import Messages
messages = current.messages = Messages(T)
ERROR = current.ERROR = Messages(T)

# Import the Core Framework
import core as s3base

# Set up logger (before any module attempts to use it!)
import s3log
s3log.S3Log.setup()

# AAA
current.auth = auth = s3base.AuthS3()

# Use session for persistent per-user variables
# - beware of a user having multiple tabs open!
# - don't save callables or class instances as these can't be pickled
if not session.s3:
    session.s3 = Storage()

# Use username instead of email address for logins
# - would probably require further customisation
#   to get this fully-working within Eden as it's not a Tested configuration
#auth.settings.login_userfield = "username"

auth.settings.hmac_key = settings.get_auth_hmac_key()
auth.define_tables(migrate=migrate, fake_migrate=fake_migrate)

current.audit = audit = s3base.S3Audit(migrate=migrate, fake_migrate=fake_migrate)

# Calendar
current.calendar = s3base.S3Calendar()

# CRUD
s3.crud = Storage()

# Frequently used S3 utilities, validators and widgets, imported here
# into the global namespace in order to access them without the s3base
# namespace prefix
s3_str = s3base.s3_str
s3_action_buttons = s3base.S3CRUD.action_buttons
s3_fullname = s3base.s3_fullname
s3_redirect_default = s3base.s3_redirect_default
S3ResourceHeader = s3base.S3ResourceHeader
from core.tools.validators import *
from core.ui.navigation import s3_rheader_tabs
from core.ui.widgets import *

# GIS Module
gis = current.gis = s3base.GIS()

# crud_request
crud_request = s3base.crud_request

# Field Selectors
FS = s3base.FS

# S3XML
s3xml = current.xml = s3base.S3XML()

# Messaging
msg = current.msg = s3base.S3Msg()

# Sync
sync = current.sync = s3base.S3Sync()

# -----------------------------------------------------------------------------
def s3_clear_session():

    # CRUD last opened records
    s3base.remove_last_record_id()

    # Session-owned records
    if "owned_records" in session:
        del session["owned_records"]

    if "s3" in session:
        s3 = session.s3
        opts = ["hrm", "report_options", "deduplicate"]
        for o in opts:
            if o in s3:
                del s3[o]

# -----------------------------------------------------------------------------
def s3_auth_on_login(form):
    """
        Actions to be performed upon successful login
            Do not redirect from here!
    """
    s3_clear_session()

# -----------------------------------------------------------------------------
def s3_auth_on_logout(user):
    """
        Actions to be performed after logout
            Do not redirect from here!
    """
    s3_clear_session()

# END =========================================================================
