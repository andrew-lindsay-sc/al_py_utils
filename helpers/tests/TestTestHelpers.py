# This block allows importing as if we were running at the root level
import os, sys
from pathlib import Path
p = f"{str(Path.home())}/Projects/mono/infrastructure/gcloud/client/bq/tools/src/dev"
p = f"{str(Path.home())}/util/al_py_utils"
sys.path.insert(1, p)

from helpers.StaticMethods import get_bq_path
import unittest
from helpers.TestHelpers import TempFile

class TestTestHelpers(unittest.TestCase):
    def test_tempfile_existing_directory(self):
        test_path = get_bq_path()+'/core/view/vw_unit_testing.sql'
        test_file_content = 'select 1 as num, \'a\' as ch'

        test_dir = '/'.join(test_path.split('/')[:-1])

        # Assert that the directory exists but the file does not
        self.assertTrue(os.path.exists(test_dir), "Dir should exist")
        self.assertFalse(os.path.exists(test_path), "File should not exist yet")

        with TempFile(test_path, test_file_content) as temp_file:
            # Assert that both now exist
            self.assertTrue(os.path.exists(test_dir), "Dir should exist")
            self.assertTrue(os.path.exists(test_path), "File should exist now")

        # Assert initial condition to verify cleanup
        self.assertTrue(os.path.exists(test_dir), "Dir should exist")
        self.assertFalse(os.path.exists(test_path), "File should not exist again")        

    def test_tempfile_non_existing_directory(self):
        test_path = get_bq_path()+'/core/view/new_dir/vw_unit_testing.sql'
        test_file_content = 'select 1 as num, \'a\' as ch'

        test_dir = '/'.join(test_path.split('/')[:-1])
        dir_above_test_dir = '/'.join(test_path.split('/')[:-2])

        # Assert that the directory exists but the file does not
        self.assertTrue(os.path.exists(dir_above_test_dir), "Dir above test dir should exist")
        self.assertFalse(os.path.exists(test_dir), "Dir should not exist")
        self.assertFalse(os.path.exists(test_path), "File should not exist")

        with TempFile(test_path, test_file_content) as temp_file:
            # Assert that both now exist
            self.assertTrue(os.path.exists(dir_above_test_dir), "Dir above test dir should exist")
            self.assertTrue(os.path.exists(test_dir), "Dir should exist")
            self.assertTrue(os.path.exists(test_path), "File should exist")

        # Assert initial condition to verify cleanup
        self.assertTrue(os.path.exists(dir_above_test_dir), "Dir above test dir should exist")
        self.assertFalse(os.path.exists(test_dir), "Dir should not exist")
        self.assertFalse(os.path.exists(test_path), "File should not exist again")    

if __name__ == '__main__':
    unittest.main()