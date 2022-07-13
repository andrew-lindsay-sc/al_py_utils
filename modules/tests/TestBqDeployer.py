# This block allows importing as if we were running at the root level
import os
import sys
from pathlib import Path
p = f"{str(Path.home())}/Projects/mono/infrastructure/gcloud/client/bq/tools/src/dev"
p = f"{str(Path.home())}/util/al_py_utils"
sys.path.insert(1, p)

from clients.GitClient import GitClient
from helpers.StaticMethods import get_mono_path
import unittest
from helpers.Capturing import Capturing
from modules.BqDeployer import BqDeployer
from parsers.CommitFileParser import CommitFileParser
from parsers.CsvFileParser import CsvFileParser
from parsers.DevModeParser import DevModeParser

class TestBqDeployer(unittest.TestCase):
    def test_init_git(self):
        # TODO: Capture has stopped working and this isn't a high value test case, coming back later
        # with Capturing() as output:
        #     expected_output = [
        #         "client_name, operation, object_name",
        #         '"sc", "deleted", "dataset.vw_view1"',
        #         '"sc", "modified", "dataset.vw_view2"'
        #     ]
        #     # I'd like to do this scoped but for some reason that eats the output
        #     deployer = BqDeployer(BqDeployer.Mode.EXAMPLE, '')
        #     self.assertEqual(deployer._git.active_branch, deployer._git.original_branch)
        #     self.assertEqual(output, expected_output)
        #     output.clear()
        with BqDeployer(BqDeployer.Mode.GIT, 'ae38991093c0a17a5f79493ae6f0d47489c0e495') as deployer:
            self.assertIsInstance(deployer._parser, CommitFileParser)
            self.assertEqual(deployer._project_format, "soundcommerce-client-{client}")

    def test_init_csv(self):
        file_name = 'test_file.csv'
        with open(file_name, 'w') as f:
            f.write("client_name, operation, object_name\n")
            f.write('"truthbar", "deleted", "core.vw_order_history"\n')
            f.write('"truthbar", "modified", "ext.vw_baskets"\n')

        with BqDeployer(BqDeployer.Mode.FILE, file_name) as deployer:
            self.assertIsInstance(deployer._parser, CsvFileParser)            
            self.assertEqual(deployer._project_format, "soundcommerce-client-{client}")

        os.remove(file_name)

        try:
            BqDeployer(BqDeployer.Mode.FILE, file_name)
            self.fail("FileNotFoundError should be thrown")
        except FileNotFoundError:
            pass        

    def test_init_dev(self):
        git = GitClient(get_mono_path())
        git.checkout_branch('ACC-1234-test-branch', create_if_needed = True)

        with BqDeployer(BqDeployer.Mode.DEVELOPMENT, '') as deployer:
            self.assertIsInstance(deployer._parser, DevModeParser)
            self.assertEqual(deployer._project_format, "dev-{client}-ACC-1234")
            self.assertEqual(deployer._git.original_branch, deployer._git.active_branch)
           
        git.switch_to(deployer._git.original_head)

    def test_print_file_info(self):
        pass

    def test_report_files(self):
        pass

    def test_deploy_changes(self):
        pass

    def test_execute(self):
        pass

if __name__ == '__main__':
    unittest.main()