from helpers.BqClient import *
from helpers.StaticMethods import *

class BqDeploymentClient(BqClient):
    """Helper class (child of BqClient) designed to help with deployment to BQ."""
    def __init__(self, client_name):
        BqClient.__init__(self, client_name)
        self.before_state = self.get_views_and_tables()

    def get_views_and_tables(self):
        """
            (None) -> list<str>
            Returns all views and tables currently present in BQ for the associated client
        """
        results = self.instance.query(BqClient.all_tables_and_views_query).result()
        views_and_tables = list()
        for result in results:
            views_and_tables.append(result.full_name)

        return views_and_tables

    def deploy_files(self, files, operation):
        """
            (list<str>, str) -> None
            Orchestrator for deployment of provided list of objects
        """
        for file in files: 
            result = self.manage_object(operation, file)
            print(f"\t{result}")

    def verify_drops(self, deletions):
        """
            (list<str>) -> None
            Validates that all expected drops happened correctly.
        """
        failed_deletions = self.check_objects_exist(deletions)
        if len(failed_deletions) == 0:
            print_success("All deletions dropped.")
        else:
            for fail in failed_deletions:
                print_fail(f"{fail} still exists")

    def verify_no_collateral(self, deletions):
        """
            (list<str>) -> None
            Validates that all expected drops happened correctly.
        """
        after_state = self.get_views_and_tables()
        delta = (set(self.before_state) - set(after_state))
        deleted_set = set(deletions)

        if len(delta - deleted_set) == 0:
            print_success("No collateral drops detected.")
        else:
            for fail in delta - deleted_set:
                print_fail(f"{fail} was not in this commit and is now missing.")

    def validate_deletions(self, deletions):
        """
            (list<str>) -> None
            Orchestrator for validation of deleted files.
        """
        if len(deletions) > 0:
            self.verify_drops(deletions)
            self.verify_no_collateral(deletions)        
        else:
            print_success("No deletions to validate.")

    def validate_modifications(self, modifications):
        """
            (list<str>) -> None
            Orchestrator for validation of deleted files.
        """
        if len(modifications) > 0:
            self.fetch_definitions(modifications)
        else:        
            print_success("No modifications to validate.")
        return

    def validate_deployment(self, deleted, modified):
        """
            (list<str>, list<str>) -> None
            Orchestrator for validation of commit deployment.
        """
        print(f"Validating deployment for {self.client_name}...")
        self.validate_deletions(list(paths_to_sql_names(deleted)))
        self.validate_modifications(list(paths_to_sql_names(modified)))