# This block allows importing from directories above tests
import os, sys
p = os.path.abspath('.')
sys.path.insert(1, p)

from helpers.StaticMethods import get_mono_path
from clients.BqClient import BqClient
import unittest

class TestBqClient(unittest.TestCase):
    sandbox_project_id = 'soundcommerce-data-sandbox'

    def test_ctor_client(self):
        client_name = 'xyz'
        bq_client = BqClient(client_name)
        self.assertEqual(client_name, bq_client.client_name)
        self.assertEqual(f"soundcommerce-client-{client_name}", bq_client.project_id)
        self.assertIsNotNone(bq_client.instance)

    def test_ctor_sandbox(self):
        bq_client = BqClient(self.sandbox_project_id)
        self.assertEqual(self.sandbox_project_id, bq_client.client_name)
        self.assertEqual(self.sandbox_project_id, bq_client.project_id)
        self.assertIsNotNone(bq_client.instance)

    def test_ctor_skip_instance(self):
        client_name = 'xyz'
        bq_client = BqClient(client_name, skip_instance=True)
        self.assertEqual(client_name, bq_client.client_name)
        self.assertEqual(f"soundcommerce-client-{client_name}", bq_client.project_id)
        self.assertIsNone(bq_client.instance)
        
    def test_get_object_meta(self):
        bq_client = BqClient(self.sandbox_project_id)

        test_file_path = get_mono_path() + '/infrastructure/gcloud/client/bq/core/view/vw_client_settings.sql'
        object_type, dataset, object_name = bq_client._get_object_meta(test_file_path)
        self.assertEqual(object_type, 'view')
        self.assertEqual(dataset, 'core')
        self.assertEqual(object_name, 'vw_client_settings')

    def test_manage_view(self):
        bq_client = BqClient(self.sandbox_project_id)

if __name__ == '__main__':
    unittest.main()