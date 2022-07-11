# This block allows importing from directories above tests
import json
import os, sys
from typing import Sequence
p = os.path.abspath('.')
sys.path.insert(1, p)

from domain.SqlObject import SqlObject
from helpers.StaticMethods import get_bq_path
from helpers.TestHelpers import TempFile
from clients.BqClient import BqClient
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
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

    def test_create_modify_delete_view(self):
        # Process test file
        test_definition = """select 1 as num, \'a\' as letter"""
        test_file_path = get_bq_path() + '/sandbox/ext/view/vw_unittest_view.sql'
        bq_client = BqClient(self.sandbox_project_id)
        bq_table = bigquery.Table('soundcommerce-data-sandbox.ext.vw_unittest_view')

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")
        # This scope block tests create functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the fail, the expected path is a NotFound exception
                self.fail("View should not exist at this point")
            except Exception as e:
                self.assertTrue(type(e) == NotFound, "View should not exist at this point")

            sql_object = SqlObject('soundcommerce-data-sandbox.ext.vw_unittest_view')
            self.assertIsNotNone(sql_object.definition, "Definition should be populated")
            
            bq_client.manage_object(BqClient.Operation.MODIFIED, sql_object)

            live_definition = bq_client.instance.get_table(bq_table).view_query
            self.assertEqual(live_definition, test_definition, "Live definition should match file definition")

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")
        test_definition += " from unnest([1,2,3])"

        # This scope block is intentionally almost identical to test the update functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the assertion, the expected path is a NotFound exception
                self.assertIsNotNone(bq_table_details, "View should exist at this point")
            except Exception as e:
                self.fail("View should exist at this point")

            sql_object = SqlObject('soundcommerce-data-sandbox.ext.vw_unittest_view')
            self.assertIsNotNone(sql_object.definition, "Definition should be populated")
            
            bq_client.manage_object(BqClient.Operation.MODIFIED, sql_object)

            live_definition = bq_client.instance.get_table(bq_table).view_query
            self.assertEqual(live_definition, test_definition, "Live definition should match file definition")            

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")

        # This scope block tests delete functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the assertion, the expected path is a NotFound exception
                self.assertIsNotNone(bq_table_details, "View should exist at this point")
            except Exception as e:
                self.fail("View should exist at this point")

            sql_object = SqlObject('soundcommerce-data-sandbox.ext.vw_unittest_view')
            bq_client.manage_object(BqClient.Operation.DELETED, sql_object)

            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the fail, the expected path is a NotFound exception
                self.fail("View should not exist at this point")
            except Exception as e:
                self.assertTrue(type(e) == NotFound, "View should not exist at this point")

    def _isSchemaSame(self, schema1: Sequence[bigquery.SchemaField], schema2: Sequence[bigquery.SchemaField]):
        schema1 = list(schema1)
        schema2 = list(schema2)

        if len(schema1) != len(schema2):
            return False, f"Field count mismatch: {len(schema1)} vs. {len(schema2)}"

        i = 0
        while i < len(schema1):
            match = schema1[i].name == schema2[i].name and \
                schema1[i].field_type == schema2[i].field_type and \
                    schema1[i].mode == schema2[i].mode

            if not match:
                return False, f"Schema detail mismatch on field {schema1[i]}"
            i += 1

        return True, "All schema fields should match"

    def test_create_modify_delete_table(self):
        test_definition = """{
    "mode": "NULLABLE",
    "name": "col1",
    "type": "STRING"
},
{
    "mode": "NULLABLE",
    "name": "col2",
    "type": "INTEGER"
}"""

        test_file_path = get_bq_path() + '/sandbox/ext/schema/unittest_table.json'
        bq_client = BqClient(self.sandbox_project_id)
        object_ref = 'soundcommerce-data-sandbox.ext.unittest_table'
        bq_table = bigquery.Table(object_ref)

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")
        # This scope block tests create functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the fail, the expected path is a NotFound exception
                self.fail("Table should not exist at this point")
            except Exception as e:
                self.assertTrue(type(e) == NotFound, "Table should not exist at this point")

            sql_object = SqlObject(object_ref)
            self.assertIsNotNone(sql_object.definition, "Definition should be populated")
            
            bq_client.manage_object(BqClient.Operation.MODIFIED, sql_object)

            status, msg = self._isSchemaSame(bq_client.instance.get_table(bq_table).schema, sql_object.get_schema_fields())
            self.assertTrue(status, msg)

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")
        test_definition += """,
{
    "mode": "NULLABLE",
    "name": "col3",
    "type": "STRING"
}"""

        # This scope block is intentionally almost identical to test the update functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the assertion, the expected path is a NotFound exception
                self.assertIsNotNone(bq_table_details, "Table should exist at this point")
            except Exception as e:
                self.fail("Table should exist at this point")

            sql_object = SqlObject(object_ref)
            self.assertIsNotNone(sql_object.definition, "Definition should be populated")
            
            bq_client.manage_object(BqClient.Operation.MODIFIED, sql_object)

            status, msg = self._isSchemaSame(bq_client.instance.get_table(bq_table).schema, sql_object.get_schema_fields())
            self.assertTrue(status, msg)    

        self.assertFalse(os.path.exists(test_file_path), "Test file should not exist.")

        # This scope block tests delete functionality
        with TempFile(test_file_path, test_definition):
            try:
                bq_table_details = bq_client.instance.get_table(bq_table)
                # We shouldn't actually hit the assertion, the expected path is a NotFound exception
                self.assertIsNotNone(bq_table_details, "Table should exist at this point")
            except Exception as e:
                self.fail("Table should exist at this point")

            sql_object = SqlObject(object_ref)
            result =  bq_client.manage_object(BqClient.Operation.DELETED, sql_object)
            expected_message_prefix = 'This utility will not drop tables'
            self.assertEqual(expected_message_prefix, result[:len(expected_message_prefix)], 
                "Dropping tables should not be supported")

        # Clean up for next test run
        to_delete = bigquery.Table(sql_object.fully_qualified_name)
        bq_client.instance.delete_table(to_delete)
        try:
            bq_table_details = bq_client.instance.get_table(bq_table)
            # We shouldn't actually hit the fail, the expected path is a NotFound exception
            self.fail("Table should not exist at this point")
        except Exception as e:
            self.assertTrue(type(e) == NotFound, "Table should not exist at this point")

if __name__ == '__main__':
    unittest.main()