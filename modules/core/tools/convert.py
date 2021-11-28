"""
    Type Conversion Utilities

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

__all__ = ("S3TypeConverter",
           "s3_str",
           )

import datetime
import time

from gluon import IS_TIME
from gluon.languages import lazyT

from .calendar import ISOFORMAT, s3_decode_iso_datetime, s3_relative_datetime

# =============================================================================
class S3TypeConverter:
    """ Universal data type converter """

    @classmethod
    def convert(cls, a, b):
        """
            Convert b into the data type of a

            Raises:
                TypeError: if any of the data types are not supported
                           or the types are incompatible
                ValueError: if the value conversion fails
        """

        if isinstance(a, lazyT):
            a = str(a)
        if b is None:
            return None
        if type(a) is type:
            if a is str:
                return cls._str(b)
            if a is int:
                return cls._int(b)
            if a is bool:
                return cls._bool(b)
            if a is float:
                return cls._float(b)
            if a is datetime.datetime:
                return cls._datetime(b)
            if a is datetime.date:
                return cls._date(b)
            if a is datetime.time:
                return cls._time(b)
            raise TypeError
        if isinstance(b, type(a)):
            return b
        if isinstance(a, (list, tuple, set)):
            if isinstance(b, (list, tuple, set)):
                return b
            elif isinstance(b, str):
                if "," in b:
                    b = b.split(",")
                else:
                    b = [b]
            else:
                b = [b]
            if len(a):
                cnv = cls.convert
                return [cnv(a[0], item) for item in b]
            else:
                return b
        if isinstance(b, (list, tuple, set)):
            cnv = cls.convert
            return [cnv(a, item) for item in b]
        if isinstance(a, str):
            return cls._str(b)
        if isinstance(a, bool):
            return cls._bool(b)
        if isinstance(a, int):
            return cls._int(b)
        if isinstance(a, float):
            return cls._float(b)
        if isinstance(a, datetime.datetime):
            return cls._datetime(b)
        if isinstance(a, datetime.date):
            return cls._date(b)
        if isinstance(a, datetime.time):
            return cls._time(b)
        raise TypeError

    # -------------------------------------------------------------------------
    @staticmethod
    def _bool(b):
        """ Convert into bool """

        if isinstance(b, bool):
            return b
        if isinstance(b, str):
            if b.lower() in ("true", "1"):
                return True
            elif b.lower() in ("false", "0"):
                return False
        if isinstance(b, int):
            if b == 0:
                return False
            else:
                return True
        raise TypeError

    # -------------------------------------------------------------------------
    @staticmethod
    def _str(b):
        """ Convert into string """

        if isinstance(b, str):
            return b
        return str(b)

    # -------------------------------------------------------------------------
    @staticmethod
    def _int(b):
        """ Convert into int """

        if isinstance(b, int):
            return b
        return int(b)

    # -------------------------------------------------------------------------
    @staticmethod
    def _float(b):
        """ Convert into float """

        if isinstance(b, float):
            return b
        return float(b)

    # -------------------------------------------------------------------------
    @staticmethod
    def _datetime(b):
        """ Convert into datetime.datetime """

        if isinstance(b, datetime.datetime):
            return b
        elif isinstance(b, str):
            # NB: converting from string (e.g. URL query) assumes the string
            #     is specified for the local time zone, unless a timezone is
            #     explicitly specified in the string (e.g. trailing Z in ISO)
            dt = None
            if b and b.lstrip()[0] in "+-nN":
                # Relative datetime expression?
                dt = s3_relative_datetime(b)
            if dt is None:
                try:
                    # Try ISO Format (e.g. filter widgets)
                    (y, m, d, hh, mm, ss) = time.strptime(b, ISOFORMAT)[:6]
                except ValueError:
                    # Fall back to default format (deployment setting)
                    dt = b
                else:
                    dt = datetime.datetime(y, m, d, hh, mm, ss)
                # Validate and convert to UTC (assuming local timezone)
                from .validators import IS_UTC_DATETIME
                validator = IS_UTC_DATETIME()
                dt, error = validator(dt)
                if error:
                    # dateutil as last resort
                    # NB: this can process ISOFORMAT with time zone specifier,
                    #     returning a timezone-aware datetime, which is then
                    #     properly converted by IS_UTC_DATETIME
                    dt, error = validator(s3_decode_iso_datetime(b))
            return dt
        else:
            raise TypeError

    # -------------------------------------------------------------------------
    @classmethod
    def _date(cls, b):
        """ Convert into datetime.date """

        if isinstance(b, datetime.date):
            return b
        elif isinstance(b, str):
            value = None
            if b and b.lstrip()[0] in "+-nN":
                # Relative datime expression?
                dt = s3_relative_datetime(b)
                if dt:
                    value = dt.date()
            if value is None:
                from .validators import IS_UTC_DATE
                # Try ISO format first (e.g. S3DateFilter)
                value, error = IS_UTC_DATE(format="%Y-%m-%d")(b)
                if error:
                    # Try L10n format
                    value, error = IS_UTC_DATE()(b)
                if error:
                    # Maybe specified as datetime-string?
                    # NB: converting from string (e.g. URL query) assumes
                    #     the string is specified for the local time zone,
                    #     specify an ISOFORMAT date/time with explicit time zone
                    #     (e.g. trailing Z) to override this assumption
                    value = cls._datetime(b).date()
            return value
        else:
            raise TypeError

    # -------------------------------------------------------------------------
    @staticmethod
    def _time(b):
        """ Convert into datetime.time """

        if isinstance(b, datetime.time):
            return b
        elif isinstance(b, str):
            value, error = IS_TIME()(b)
            if error:
                raise ValueError
            return value
        else:
            raise TypeError

# =============================================================================
def s3_str(s, encoding="utf-8"):
    """
        Convert an object into a str

        Args:
            s: the object
            encoding: the character encoding
    """

    if type(s) is str:
        return s
    elif type(s) is bytes:
        return s.decode(encoding, "strict")
    else:
        return str(s)

# END =========================================================================
