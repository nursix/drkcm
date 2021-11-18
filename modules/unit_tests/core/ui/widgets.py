# Eden Unit Tests
#
# To run this script use:
# python web2py.py -S eden -M -R applications/eden/modules/unit_tests/core/ui/widgets.py

import unittest

from collections import OrderedDict

from gluon import *
from gluon.storage import Storage

from core import S3HoursWidget

from unit_tests import run_suite

# =============================================================================
class HoursWidgetTests(unittest.TestCase):
    """ Tests for S3HoursWidget """

    # -------------------------------------------------------------------------
    def test_parse(self):
        """ Test parsing of regular hours-values """

        assertEqual = self.assertEqual

        w = S3HoursWidget(interval=None,
                          precision=None,
                          explicit_above=None,
                          )
        parse = w.parse_input

        samples = {"0": 0.0,              # decimal
                   "0.28754": 0.28754,    # decimal
                   "6:30": 6.5,           # colon-notation HH:MM
                   "3:45:36": 3.76,       # colon-notation, with seconds
                   "12:36.75": 12.6125,   # colon-notation, with fraction
                   "1h15min": 1.25,       # unit-notation
                   "9h18s": 9.005,        # unit-notation without minutes segment
                   }

        for s in samples.items():
            hours = parse(s[0])
            assertEqual(round(hours, 8), s[1], "'%s' recognized as %s, expected %s" % (s[0], hours, s[1]))

    # -------------------------------------------------------------------------
    def test_parse_rounded(self):
        """ Test parsing of regular hours-values, with rounding """

        assertEqual = self.assertEqual

        w = S3HoursWidget(interval=None,
                          precision=2,
                          explicit_above=None,
                          )
        parse = w.parse_input

        samples = {"18": 18.0,          # decimal, hours assumed
                   "18m": 0.3,          # decimal, explicit unit
                   "0.28754": 0.29,     # decimal, hours assumed
                   "3,4": 3.4,          # decimal, hours assumed, comma tolerated
                   "6:30": 6.5,         # colon-notation, hours assumed
                   "3:45:36": 3.76,     # colon-notation, with seconds
                   "12:36.75m": 0.21,   # colon-notation, explicit unit
                   "1h15min": 1.25,     # unit-notation
                   "9h18s": 9.01,       # unit-notation without minutes
                   }

        for s in samples.items():
            hours = parse(s[0])
            assertEqual(round(hours, 8), s[1], "'%s' recognized as %s, expected %s" % (s[0], hours, s[1]))

    # -------------------------------------------------------------------------
    def test_parse_interval(self):
        """ Test parsing of regular hours-values, rounded up to minutes interval """

        assertEqual = self.assertEqual

        w = S3HoursWidget(interval=15, # Round to 1/4 hours
                          precision=None,
                          explicit_above=None,
                          )
        parse = w.parse_input

        samples = {"0": 0.0,
                   "0.28754": 0.5,
                   "6:30": 6.5,
                   "3:45:36": 4.0,
                   "1h15min": 1.25,
                   "9h18s": 9.25,
                   }

        for s in samples.items():
            hours = parse(s[0])
            assertEqual(round(hours, 8), s[1], "'%s' recognized as %s, expected %s" % (s[0], hours, s[1]))

    # -------------------------------------------------------------------------
    def test_validate_implied_unit_limit(self):
        """ Test in-widget validation of explicit notation above limit """

        assertEqual = self.assertEqual
        assertNotEqual = self.assertNotEqual

        w = S3HoursWidget(interval=None,
                          precision=2,
                          explicit_above=4,
                          )
        validate = w.validate

        samples = {"3": 3.0,        # lacks unit but below limit, hours assumed
                   "0.28754": 0.29, # lacks unit but below limit, hours assumed
                   "6:30": 6.5,     # lacks unit and above limit, but tolerated for colon-notation
                   "3:45:36": 3.76, # lacks unit and above limit, but tolerated for colon-notation
                   "6:18m": 0.11,   # Colon-notation with explicit unit, overriding assumption
                   "1h15min": 1.25, # Explicit unit
                   "9h18s": 9.01,   # Explicit unit
                   "4.95": "error", # lacks unit and above limit => ambiguous
                   "3h15": "error", # 15 segment lacks unit and above limit => edge-case, treated as ambiguous
                   }

        for s in samples.items():
            hours, error = validate(s[0])

            if s[1] == "error":
                assertNotEqual(error, None)
            else:
                assertEqual(error, None)
                assertEqual(round(hours, 8), s[1], "'%s' recognized as %s, expected %s" % (s[0], hours, s[1]))

# =============================================================================
if __name__ == "__main__":

    run_suite(
        HoursWidgetTests,
    )

# END ========================================================================
