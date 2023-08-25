# Eden unit tests
#
# To run this script use:
# python web2py.py -S eden -M -R applications/eden/modules/unit_tests/core/model/fields.py
#
import unittest
from gluon import Field, IS_INT_IN_RANGE, IS_EMPTY_OR, IS_NOT_EMPTY
from gluon.languages import lazyT

from core import *

from unit_tests import run_suite

# =============================================================================
class FieldTemplateTests(unittest.TestCase):

    @staticmethod
    def widget_1():
        """ Dummy """
        pass

    @staticmethod
    def widget_2():
        """ Dummy """
        pass

    def testTemplating(self):
        """ Verify FieldTemplate definition """

        field_a = FieldTemplate("field_a", "integer")
        field_b = FieldTemplate("field_b")

        self.assertIs(type(field_a.template), type)
        self.assertTrue(issubclass(field_a.template, Field))

        # Check that Field classes are separate
        self.assertIsNot(field_a.template, field_b.template)

    def testInstantiation(self):
        """ Verify FieldTemplate instantiation """

        w = self.widget_1
        r = IS_INT_IN_RANGE(0, 10)

        field = FieldTemplate("field", "integer",
                              widget = w,
                              requires = r,
                              )
        instance = field()

        self.assertEqual(instance.name, "field")
        self.assertEqual(instance.type, "integer")
        self.assertIs(instance.widget, w)
        self.assertIs(instance.requires, r)

    def testDefaultType(self):
        """ FieldTemplate default type is string """

        field = FieldTemplate("field")
        instance = field()

        self.assertEqual(instance.type, "string")

    def testOverrideName(self):
        """ FieldTemplate instantiation can override field name """

        field = FieldTemplate("field_a", "integer")

        instance_a = field()
        instance_b = field("field_b")
        instance_c = field()

        self.assertEqual(instance_a.name, "field_a")
        self.assertEqual(instance_b.name, "field_b")
        self.assertEqual(instance_c.name, "field_a")

    def testOverrideType(self):
        """ FieldTemplate instantiation cannot override type """

        field = FieldTemplate("field", "integer")

        instance_a = field(type="double")
        instance_b = field()

        self.assertEqual(instance_a.type, "integer")
        self.assertEqual(instance_b.type, "integer")

    def testOverrideRequires(self):
        """ FieldTemplate instantiation can override default validator """

        r_a = IS_INT_IN_RANGE(0, 10)
        r_b = IS_INT_IN_RANGE(1, 99)

        field = FieldTemplate("field", "integer", requires=r_a)

        instance_a = field()
        instance_b = field(requires=r_b)
        instance_c = field()
        instance_d = field(requires=None)

        self.assertIs(instance_a.requires, r_a)
        self.assertIs(instance_b.requires, r_b)
        self.assertIs(instance_c.requires, r_a)

        # None is converted to [] in Field constructor
        self.assertEqual(instance_d.requires, [])

    def testOverrideWidget(self):
        """ FieldTemplate instantiation can override default widget """

        field = FieldTemplate("field", widget=self.widget_1)

        instance_a = field()
        instance_b = field(widget=self.widget_2)
        instance_c = field()
        instance_d = field(widget=None)

        self.assertIs(instance_a.widget, self.widget_1)
        self.assertIs(instance_b.widget, self.widget_2)
        self.assertIs(instance_c.widget, self.widget_1)
        self.assertIs(instance_d.widget, None)

    def testEmptyNone(self):
        """ FieldTemplate instantiation with empty=None retains original validator """

        r = IS_INT_IN_RANGE(0, 15)

        field = FieldTemplate("field", "integer", requires = r)

        instance_a = field(empty=None)
        instance_b = field()

        self.assertIs(instance_a.requires, r)
        self.assertIs(instance_b.requires, r)

    def testEmptyTrue(self):
        """ FieldTemplate instantiation with empty=True allows empty field values """

        # Adds IS_EMPTY_OR if required
        r = IS_INT_IN_RANGE(0, 15)

        field = FieldTemplate("field", "integer", requires = r)

        instance_a = field(empty=True)
        instance_b = field()

        self.assertIsInstance(instance_a.requires, IS_EMPTY_OR)
        self.assertIs(instance_a.requires.other, r)
        self.assertIs(instance_b.requires, r)

        # Does not add IS_EMPTY_OR if already present
        field = FieldTemplate("field", "integer", requires = IS_EMPTY_OR(r))

        instance_a = field(empty=True)
        instance_b = field()

        self.assertIsInstance(instance_a.requires, IS_EMPTY_OR)
        self.assertIs(instance_a.requires.other, r)
        self.assertIsInstance(instance_b.requires, IS_EMPTY_OR)
        self.assertIs(instance_b.requires.other, r)

        # Does not add IS_EMPTY_OR if there is no validation at all
        field = FieldTemplate("field", "integer", requires = None)

        instance_a = field(empty=True)
        instance_b = field()

        self.assertEqual(instance_a.requires, [])
        self.assertEqual(instance_b.requires, [])

    def testEmptyFalse(self):
        """ FieldTemplate instantiation with empty=False enforces non-empty field values """

        # Keeps validator that is not IS_EMPTY_OR
        r = IS_INT_IN_RANGE(0, 15)

        field = FieldTemplate("field", "integer", requires = r)

        instance_a = field(empty=False)
        instance_b = field()

        self.assertIs(instance_a.requires, r)
        self.assertIs(instance_b.requires, r)

        # Removes IS_EMPTY_OR if present
        field = FieldTemplate("field", "integer", requires = IS_EMPTY_OR(r))

        instance_a = field(empty=False)
        instance_b = field()

        self.assertIs(instance_a.requires, r)
        self.assertIsInstance(instance_b.requires, IS_EMPTY_OR)
        self.assertIs(instance_b.requires.other, r)

        # Adds IS_NOT_EMPTY if there is no validation at all
        field = FieldTemplate("field", "integer", requires = None)

        instance_a = field(empty=False)
        instance_b = field()

        self.assertIsInstance(instance_a.requires, IS_NOT_EMPTY)
        self.assertEqual(instance_b.requires, [])

# =============================================================================
if __name__ == "__main__":

    run_suite(
        FieldTemplateTests,
    )

# END ========================================================================
