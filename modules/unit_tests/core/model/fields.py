# Eden unit tests
#
# To run this script use:
# python web2py.py -S eden -M -R applications/eden/modules/unit_tests/core/model/fields.py
#
import unittest
from gluon.languages import lazyT

from core import *

from unit_tests import run_suite

# =============================================================================
class ReusableFieldTests(unittest.TestCase):
    """ Test multiple named widgets in reusable fields """

    # -------------------------------------------------------------------------
    def widget1(self):
        """ Dummy widget """
        pass

    def widget2(self):
        """ Dummy widget """
        pass

    def widget3(self):
        """ Dummy widget """
        pass

    # -------------------------------------------------------------------------
    def testWidgetOverrideWithoutDefault(self):
        """ Test setting the widget in the instance (no default) """

        rf = S3ReusableField("test", "integer")

        # Default None
        field = rf()
        self.assertEqual(field.widget, None)

        # Widget-parameter overrides default
        field = rf(widget=self.widget1)
        self.assertEqual(field.widget, self.widget1)

    # -------------------------------------------------------------------------
    def testWidgetOverrideWithDefault(self):
        """ Test overriding the default widget in the instance """

        rf = S3ReusableField("test", "integer",
                             widget=self.widget1)

        # Default widget
        field = rf()
        self.assertEqual(field.widget, self.widget1)

        # Widget-parameter overrides default
        field = rf(widget=self.widget2)
        self.assertEqual(field.widget, self.widget2)

    # -------------------------------------------------------------------------
    def testSingleWidget(self):
        """ Test using widget set with single widget """

        rf = S3ReusableField("test", "integer",
                             widgets=self.widget1)

        # Default
        field = rf()
        self.assertEqual(field.widget, self.widget1)

        # Deliberate default
        field = rf(widget="default")
        self.assertEqual(field.widget, self.widget1)

        # Override
        field = rf(widget=self.widget2)
        self.assertEqual(field.widget, self.widget2)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="alternative")

    # -------------------------------------------------------------------------
    def testMultipleWidgets(self):
        """ Test using widget set with multiple widgets """

        rf = S3ReusableField("test", "integer",
                             widgets={"default": self.widget1,
                                      "alternative": self.widget2,
                                      },
                             )

        # Using default from set
        field = rf()
        self.assertEqual(field.widget, self.widget1)

        # Deliberate default
        field = rf(widget="default")
        self.assertEqual(field.widget, self.widget1)

        # Other choice
        field = rf(widget="alternative")
        self.assertEqual(field.widget, self.widget2)

        # Override
        field = rf(widget=self.widget3)
        self.assertEqual(field.widget, self.widget3)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="other")

    # -------------------------------------------------------------------------
    def testMultipleWidgetsWithDefault(self):
        """ Test using widget set with multiple widgets and override default """

        rf = S3ReusableField("test", "integer",
                             widgets={"default": self.widget1,
                                      "alternative": self.widget2,
                                      },
                             widget=self.widget3,
                             )

        # "widget"-setting overrides "default"
        field = rf()
        self.assertEqual(field.widget, self.widget3)

        # "widget"-setting overrides "default"
        field = rf(widget="default")
        self.assertEqual(field.widget, self.widget3)

        # Other alternatives still available
        field = rf(widget="alternative")
        self.assertEqual(field.widget, self.widget2)

        # And can still override
        field = rf(widget=self.widget1)
        self.assertEqual(field.widget, self.widget1)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="other")

    # -------------------------------------------------------------------------
    def testFallbackWithDefault(self):
        """ Test fallback to default widget """

        rf = S3ReusableField("test", "integer",
                             widget=self.widget1,
                             widgets={"alternative": self.widget2},
                             )

        # Standard fallback
        field = rf()
        self.assertEqual(field.widget, self.widget1)

        # Deliberate default
        field = rf(widget="default")
        self.assertEqual(field.widget, self.widget1)

        # Alternative
        field = rf(widget="alternative")
        self.assertEqual(field.widget, self.widget2)

        # Override
        field = rf(widget=self.widget1)
        self.assertEqual(field.widget, self.widget1)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="other")

    # -------------------------------------------------------------------------
    def testExplicitNone(self):
        """ Test explicit None-widget in instance """

        rf = S3ReusableField("test", "integer",
                             widgets={"default": self.widget1,
                                      "alternative": self.widget2,
                                      },
                             widget=self.widget3,
                             )

        # Standard fallback
        field = rf(widget=None)
        self.assertEqual(field.widget, None)

    # -------------------------------------------------------------------------
    def testFallbackWithoutDefault(self):
        """ Test fallback to None """

        rf = S3ReusableField("test", "integer",
                             widgets={"alternative": self.widget2},
                             )

        # Standard fallback
        field = rf()
        self.assertEqual(field.widget, None)

        # Deliberate default
        field = rf(widget="default")
        self.assertEqual(field.widget, None)

        # Alternative
        field = rf(widget="alternative")
        self.assertEqual(field.widget, self.widget2)

        # Override
        field = rf(widget=self.widget1)
        self.assertEqual(field.widget, self.widget1)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="other")

    # -------------------------------------------------------------------------
    def testFallbackWithoutWidgets(self):
        """ Test fallback to None """

        rf = S3ReusableField("test", "integer")

        # Standard fallback
        field = rf()
        self.assertEqual(field.widget, None)

        # Deliberate default
        field = rf(widget="default")
        self.assertEqual(field.widget, None)

        # Alternative
        self.assertRaises(NameError, rf, widget="alternative")

        # Override
        field = rf(widget=self.widget1)
        self.assertEqual(field.widget, self.widget1)

        # Undefined widget
        self.assertRaises(NameError, rf, widget="other")

# =============================================================================
if __name__ == "__main__":

    run_suite(
        ReusableFieldTests,
    )

# END ========================================================================
