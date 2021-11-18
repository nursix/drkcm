# Eden Unit Tests
#
# To run this script use:
# python web2py.py -S eden -M -R applications/eden/modules/unit_tests/core/tools/utils.py
#
import unittest

from core import *

from unit_tests import run_suite

# =============================================================================
class FKWrappersTests(unittest.TestCase):
    """ Test has_foreign_key and get_foreign_key """

    # -------------------------------------------------------------------------
    def testHasForeignKey(self):
        """ Test has_foreign_key """

        ptable = current.s3db.pr_person
        self.assertFalse(s3_has_foreign_key(ptable.first_name))
        self.assertTrue(s3_has_foreign_key(ptable.pe_id))

        htable = current.s3db.hrm_human_resource
        self.assertFalse(s3_has_foreign_key(htable.start_date))
        self.assertTrue(s3_has_foreign_key(htable.person_id))

        # @todo: restore with a different list:reference
        #otable = s3db.org_organisation
        #self.assertTrue(s3_has_foreign_key(otable.multi_sector_id))
        #self.assertFalse(s3_has_foreign_key(otable.multi_sector_id, m2m=False))

    # -------------------------------------------------------------------------
    def testGetForeignKey(self):

        ptable = current.s3db.pr_person
        ktablename, key, multiple = s3_get_foreign_key(ptable.pe_id)
        self.assertEqual(ktablename, "pr_pentity")
        self.assertEqual(key, "pe_id")
        self.assertFalse(multiple)

        # @todo: restore with a different list:reference
        #otable = s3db.org_organisation
        #ktablename, key, multiple = s3_get_foreign_key(otable.multi_sector_id)
        #self.assertEqual(ktablename, "org_sector")
        #self.assertEqual(key, "id")
        #self.assertTrue(multiple)

        # @todo: restore with a different list:reference
        #ktablename, key, multiple = s3_get_foreign_key(otable.multi_sector_id, m2m=False)
        #self.assertEqual(ktablename, None)
        #self.assertEqual(key, None)
        #self.assertEqual(multiple, None)

# =============================================================================
if __name__ == "__main__":

    run_suite(
        FKWrappersTests,
        )

# END ========================================================================
