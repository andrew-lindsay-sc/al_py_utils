from domain.SqlObjectReferences import SqlObjectReferences
from clients.BqClient import *
from helpers.StaticMethods import *
from anytree import Node, RenderTree

class BqDeploymentClient(BqClient):
    """Helper class (child of BqClient) designed to help with deployment to BQ."""
    def __init__(self, client_name, project_name = None):
        BqClient.__init__(self, client_name = client_name, project_name = project_name)
        self.before_state = self.get_views_and_tables()

    def _get_dependencies(self, sql_object: SqlObject) -> set[str]:
        return SqlObjectReferences(sql_object).get_children

    def deploy_files(self, files: list[str], operation: BqClient.Operation, handle_dependencies = False):
        """
            (list[str], str) -> None
            Orchestrator for deployment of provided list of objects
        """
        if handle_dependencies:
            self._processed_dependencies = list()
        for file in files:
            if handle_dependencies:                
                sql_object = SqlObject(self.path_to_fully_qualified(file))

                dependencies = self._get_dependencies(sql_object)
                for dependency in dependencies:
                    # Quick measure to avoid updating the same file multiple times if it appears as a dependency
                    #   to multiple items
                    if dependency in self._processed_dependencies:
                        continue
                    print_info(f"Dependency check: {self.manage_object(BqClient.Operation.MODIFIED, dependency)}")
                    self._processed_dependencies.append(dependency)

            if file not in self._processed_dependencies:
                print_info(self.manage_object(operation, file))
            else:
                print_info(f"Skipping {file}, already deployed as a dependency")

    def verify_drops(self, deletions):
        """
            (list[str]) -> None
            Validates that all expected drops happened correctly.
        """
        failed_deletions = self.check_objects_exist(deletions)
        if len(failed_deletions) == 0:
            print_success("All deletions dropped.")
        else:
            for fail in failed_deletions:
                print_fail(f"{fail} still exists")

    def _verify_no_collateral(self, deletions):
        """
            (list[str]) -> None
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

    def _validate_deletions(self, deletions):
        """
            (list[str]) -> None
            Orchestrator for validation of deleted files.
        """
        if len(deletions) > 0:
            self.verify_drops(deletions)
            self.verify_no_collateral(deletions)        
        else:
            print_success("No deletions to validate.")

    def _validate_modifications(self, modifications):
        """
            (list[str]) -> None
            Orchestrator for validation of deleted files.
        """
        if len(modifications) > 0:
            self.fetch_definitions(modifications)
        else:        
            print_success("No modifications to validate.")
        return

    def validate_deployment(self, deleted, modified):
        """
            (list[str], list[str]) -> None
            Orchestrator for validation of commit deployment.
        """
        print(f"Validating deployment for {self.client_name}...")
        self._validate_deletions(list(paths_to_sql_names(deleted)))
        self._validate_modifications(list(paths_to_sql_names(modified)))
        