# pip install GitPython

import argparse
from helpers.StaticMethods import print_info
from modules.BqDeployer import BqDeployer

def prepare_args(parser):
    parser.add_argument(
        '-sha', help='(Optional) Specify the SHA of a PR merge commit to deploy')
    parser.add_argument(
        '-file', help='(Optional) Specify a file of modifications to deploy, use -exampleFile for usage')
    parser.add_argument(
        '-exampleFile', action='store_true', help='(Optional) Displays expected format for a modification file, then exits')
    parser.add_argument(
        '-go', action='store_true', help='(Optional) Deploy the specified commit, otherwise changes will be reported but not deployed')
    parser.add_argument(
        '-c', '--clients', help='(Optional) Restrict clients to be modified to the specified list (e.g. "truthbar, rainbow2"')
    parser.add_argument(
        '-d', '--dev', action='store_true', help='(Optional) If specified, changes will be deployed to dev projects only. If changes are only to non-client objects, -c/--clients is required.')
    parser.add_argument(
        '-p', '--project_id', help='(Optional) Specify an override project_id to deploy to. Used by Dev Mode when the project cannot be determined based on branch name.')
    # TODO: Development Mode Considerations
    #   - Do not globally apply core/ext updates DONE
    #   - Allow manual specification of project DONE
    #   - Logic to switch to dev projects DONE
    #   - Resolution of relevant clients, error if impossible DONE

def validate_args(args):
    if (not (args.sha or args.file)):
        print_info(f"Neither -sha or -file were specified, generating example file.")
    if (args.sha and args.file):
        raise Exception("Only one of -sha or -file is allowed at a time.")

def handle_args():
    """
        Parses and validates arguments passed in by the command line invocation.
    """
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)  

    return args

def main():
    args = handle_args()
    fetch_files_from = ''

    if args.file:
        mode = BqDeployer.Mode.FILE
        fetch_files_from = args.file
    elif args.sha:
        mode = BqDeployer.Mode.GIT
        fetch_files_from = args.sha
    elif args.d or args.dev:
        mode = BqDeployer.Mode.DEVELOPMENT
    else:
        mode = BqDeployer.Mode.EXAMPLE

    deployer = BqDeployer(mode, fetch_files_from, args.c, args.p)
    deployer.execute(is_dry_run = not args.go)

if __name__ == "__main__":
    main()