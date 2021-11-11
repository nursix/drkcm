"""
    Simple Generic Location Tracking System

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

__all__ = ("S3CheckInMethod",
           "S3CheckOutMethod",
           )

from gluon import current, HTTP, FORM, INPUT, LABEL, TABLE
from gluon.storage import Storage

from ..tools import S3Trackable

from .base import CRUDMethod

# =============================================================================
class S3CheckInMethod(CRUDMethod):
    """
        Custom Method to allow a trackable resource to check-in
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
            response = current.response
            table = r.table
            tracker = S3Trackable(table, record_id=r.id)

            title = T("Check-In")

            get_vars = r.get_vars

            # Are we being passed a location_id?
            location_id = get_vars.get("location_id", None)
            if not location_id:
                # Are we being passed a lat and lon?
                lat = get_vars.get("lat", None)
                if lat is not None:
                    lon = get_vars.get("lon", None)
                    if lon is not None:
                        form_vars = Storage(lat = float(lat),
                                            lon = float(lon),
                                            )
                        form = Storage(vars=form_vars)
                        s3db.gis_location_onvalidation(form)
                        location_id = s3db.gis_location.insert(**form_vars)


            form = None
            if not location_id:
                # Give the user a form to check-in

                # Test the formstyle
                formstyle = current.deployment_settings.get_ui_formstyle()
                row = formstyle("test", "test", "test", "test")
                if isinstance(row, tuple):
                    # Formstyle with separate row for label (e.g. default Eden formstyle)
                    tuple_rows = True
                else:
                    # Formstyle with just a single row (e.g. Bootstrap, Foundation or DRRPP)
                    tuple_rows = False

                form_rows = []
                comment = ""

                _id = "location_id"
                label = LABEL("%s:" % T("Location"))

                from ..ui import S3LocationSelector
                field = table.location_id
                #value = tracker.get_location(_fields=["id"],
                #                             as_rows=True).first().id
                value = None # We always want to create a new Location, not update the existing one
                widget = S3LocationSelector(show_latlon = True)(field, value)

                row = formstyle("%s__row" % _id, label, widget, comment)
                if tuple_rows:
                    form_rows.append(row[0])
                    form_rows.append(row[1])
                else:
                    form_rows.append(row)

                _id = "submit"
                label = ""
                widget = INPUT(_type="submit", _value=T("Check-In"))
                row = formstyle("%s__row" % _id, label, widget, comment)
                if tuple_rows:
                    form_rows.append(row[0])
                    form_rows.append(row[1])
                else:
                    form_rows.append(row)

                if tuple_rows:
                    # Assume TRs
                    form = FORM(TABLE(*form_rows))
                else:
                    form = FORM(*form_rows)

                if form.accepts(current.request.vars, current.session):
                    location_id = form.vars.get("location_id", None)

            if location_id:
                # We're not Checking-in in S3Track terms (that's about interlocking with another object)
                #tracker.check_in()
                #timestmp = form.vars.get("timestmp", None)
                #if timestmp:
                #    # @ToDo: Convert from string
                #    pass
                #tracker.set_location(location_id, timestmp=timestmp)
                tracker.set_location(location_id)
                response.confirmation = T("Checked-In successfully!")

            response.view = "check-in.html"
            output = dict(form = form,
                          title = title,
                          )
            return output

        # @ToDo: JSON representation for check-in from mobile devices
        else:
            raise HTTP(415, current.ERROR.BAD_FORMAT)

# =============================================================================
class S3CheckOutMethod(CRUDMethod):
    """
        Custom Method to allow a trackable resource to check-out
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

            response = current.response
            tracker = S3Trackable(r.table, record_id=r.id)

            title = T("Check-Out")

            # Give the user a form to check-out

            # Test the formstyle
            formstyle = current.deployment_settings.get_ui_formstyle()
            row = formstyle("test", "test", "test", "test")
            if isinstance(row, tuple):
                # Formstyle with separate row for label (e.g. default Eden formstyle)
                tuple_rows = True
            else:
                # Formstyle with just a single row (e.g. Bootstrap, Foundation or DRRPP)
                tuple_rows = False

            form_rows = []
            comment = ""

            _id = "submit"
            label = ""
            widget = INPUT(_type="submit", _value=T("Check-Out"))
            row = formstyle("%s__row" % _id, label, widget, comment)
            if tuple_rows:
                form_rows.append(row[0])
                form_rows.append(row[1])
            else:
                form_rows.append(row)

            if tuple_rows:
                # Assume TRs
                form = FORM(TABLE(*form_rows))
            else:
                form = FORM(*form_rows)

            if form.accepts(current.request.vars, current.session):
                # Check-Out
                # We're not Checking-out in S3Track terms (that's about removing an interlock with another object)
                # What we're doing is saying that we're now back at our base location
                #tracker.check_out()
                #timestmp = form_vars.get("timestmp", None)
                #if timestmp:
                #    # @ToDo: Convert from string
                #    pass
                #tracker.set_location(r.record.location_id, timestmp=timestmp)
                tracker.set_location(r.record.location_id)
                response.confirmation = T("Checked-Out successfully!")

            response.view = "check-in.html"
            output = dict(form = form,
                          title = title,
                          )
            return output

        # @ToDo: JSON representation for check-out from mobile devices
        else:
            raise HTTP(415, current.ERROR.BAD_FORMAT)

# END =========================================================================
