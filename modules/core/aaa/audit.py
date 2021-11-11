"""
    Authentication, Authorization, Accounting

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

__all__ = ("S3Audit",
           )

import datetime

from gluon import current

from s3dal import Row, Field

from ..tools import S3DateTime

# =============================================================================
class S3Audit:
    """ S3 Audit Trail Writer Class """

    def __init__(self,
                 tablename = "s3_audit",
                 migrate = True,
                 fake_migrate = False
                 ):
        """
            Constructor

            Args:
                tablename: the name of the audit table
                migrate: migration setting

            Note:
                This defines the audit table.
        """

        settings = current.deployment_settings
        audit_read = settings.get_security_audit_read()
        audit_write = settings.get_security_audit_write()
        if not audit_read and not audit_write:
            # Auditing is Disabled
            self.table = None
            return

        db = current.db
        if tablename not in db:
            db.define_table(tablename,
                            Field("timestmp", "datetime",
                                  represent = S3DateTime.datetime_represent,
                                  ),
                            Field("user_id", db.auth_user),
                            Field("method"),
                            Field("tablename"),
                            Field("record_id", "integer"),
                            Field("representation"),
                            # List of Key:Values
                            Field("old_value", "text"),
                            # List of Key:Values
                            Field("new_value", "text"),
                            Field("repository_id", "integer"),
                            migrate = migrate,
                            fake_migrate = fake_migrate,
                            )
        self.table = db[tablename]

        user = current.auth.user
        if user:
            self.user_id = user.id
        else:
            self.user_id = None

    # -------------------------------------------------------------------------
    def __call__(self, method, prefix, name,
                 form = None,
                 record = None,
                 representation = "unknown"
                 ):
        """
            Audit

            Args:
                method: Method to log, one of
                        "create", "update", "read", "list" or "delete"
                prefix: the module prefix of the resource
                name: the name of the resource (without prefix)
                form: the form
                record: the record ID
                representation: the representation format
        """

        table = self.table
        if not table:
            # Don't Audit
            return True

        #if DEBUG:
        #    _debug("Audit %s: %s_%s record=%s representation=%s",
        #           method,
        #           prefix,
        #           name,
        #           record,
        #           representation,
        #           )

        if method in ("list", "read"):
            audit = current.deployment_settings.get_security_audit_read()
        elif method in ("create", "update", "delete"):
            audit = current.deployment_settings.get_security_audit_write()
        else:
            # Don't Audit
            return True

        if not audit:
            # Don't Audit
            return True

        tablename = "%s_%s" % (prefix, name)

        if record:
            if isinstance(record, Row):
                record = record.get("id", None)
                if not record:
                    return True
            try:
                record = int(record)
            except ValueError:
                record = None
        elif form:
            try:
                record = form.vars["id"]
            except:
                try:
                    record = form["id"]
                except:
                    record = None
            if record:
                try:
                    record = int(record)
                except ValueError:
                    record = None
        else:
            record = None

        if callable(audit):
            audit = audit(method, tablename, form, record, representation)
            if not audit:
                # Don't Audit
                return True

        if method in ("list", "read"):
            table.insert(timestmp = datetime.datetime.utcnow(),
                         user_id = self.user_id,
                         method = method,
                         tablename = tablename,
                         record_id = record,
                         representation = representation,
                         repository_id = current.response.s3.repository_id,
                         )

        elif method == "create":
            if form:
                form_vars = form.vars
                if not record:
                    record = form_vars["id"]
                new_value = ["%s:%s" % (var, str(form_vars[var]))
                             for var in form_vars if form_vars[var]]
            else:
                new_value = []
            table.insert(timestmp = datetime.datetime.utcnow(),
                         user_id = self.user_id,
                         method = method,
                         tablename = tablename,
                         record_id = record,
                         representation = representation,
                         new_value = new_value,
                         repository_id = current.response.s3.repository_id,
                         )

        elif method == "update":
            if form:
                rvars = form.record
                if rvars:
                    old_value = ["%s:%s" % (var, str(rvars[var]))
                                 for var in rvars]
                else:
                    old_value = []
                fvars = form.vars
                if not record:
                    record = fvars["id"]
                new_value = ["%s:%s" % (var, str(fvars[var]))
                             for var in fvars]
            else:
                new_value = []
                old_value = []
            table.insert(timestmp = datetime.datetime.utcnow(),
                         user_id = self.user_id,
                         method = method,
                         tablename = tablename,
                         record_id = record,
                         representation = representation,
                         old_value = old_value,
                         new_value = new_value,
                         repository_id = current.response.s3.repository_id,
                         )

        elif method == "delete":
            db = current.db
            query = (db[tablename].id == record)
            row = db(query).select(limitby = (0, 1)
                                   ).first()
            old_value = []
            if row:
                old_value = ["%s:%s" % (field, row[field])
                             for field in row]
            table.insert(timestmp = datetime.datetime.utcnow(),
                         user_id = self.user_id,
                         method = method,
                         tablename = tablename,
                         record_id = record,
                         representation = representation,
                         old_value = old_value,
                         repository_id = current.response.s3.repository_id,
                         )

        return True

    # -------------------------------------------------------------------------
    def represent(self, records):
        """
            Provide a Human-readable representation of Audit records
            - currently unused

            Args:
                record: the record IDs
        """

        table = self.table
        # Retrieve the records
        if isinstance(records, int):
            limit = 1
            query = (table.id == records)
        else:
            limit = len(records)
            query = (table.id.belongs(records))
        records = current.db(query).select(table.tablename,
                                           table.method,
                                           table.user_id,
                                           table.old_value,
                                           table.new_value,
                                           limitby = (0, limit)
                                           )

        # Convert to Human-readable form
        s3db = current.s3db
        output = []
        oappend = output.append
        for record in records:
            table = s3db[record.tablename]
            method = record.method
            if method == "create":
                new_value = record.new_value
                if not new_value:
                    continue
                diff = []
                dappend = diff.append
                for v in new_value:
                    fieldname, value = v.split(":", 1)
                    represent = table[fieldname].represent
                    if represent:
                        value = represent(value)
                    label = table[fieldname].label or fieldname
                    dappend("%s is %s" % (label, value))

            elif method == "update":
                old_values = record.old_value
                new_values = record.new_value
                if not new_value:
                    continue
                changed = {}
                for v in new_values:
                    fieldname, new_value = v.split(":", 1)
                    old_value = old_values.get(fieldname, None)
                    if new_value != old_value:
                        ftype = table[fieldname].type
                        if ftype == "integer" or \
                           ftype.startswith("reference"):
                            if new_value:
                                new_value = int(new_value)
                            if new_value == old_value:
                                continue
                        represent = table[fieldname].represent
                        if represent:
                            new_value = represent(new_value)
                        label = table[fieldname].label or fieldname
                        if old_value:
                            if represent:
                                old_value = represent(old_value)
                            changed[fieldname] = "%s changed from %s to %s" % \
                                (label, old_value, new_value)
                        else:
                            changed[fieldname] = "%s changed to %s" % \
                                (label, new_value)
                diff = []
                dappend = diff.append
                for fieldname in changed:
                    dappend(changed[fieldname])

            elif method == "delete":
                old_value = record.old_value
                if not old_value:
                    continue
                diff = []
                dappend = diff.append
                for v in old_value:
                    fieldname, value = v.split(":", 1)
                    represent = table[fieldname].represent
                    if represent:
                        value = represent(value)
                    label = table[fieldname].label or fieldname
                    dappend("%s was %s" % (label, value))

            oappend("\n".join(diff))

        return output

# END =========================================================================
