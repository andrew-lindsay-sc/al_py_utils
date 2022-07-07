import pandas as pd

def read_orphaned_file(orphaned_file):
    return pd.read_csv(orphaned_file)

def read_dependency_file(dependency_file, orphaned_data):
    dependency_data = pd.read_csv(dependency_file)
    for index, dep in dependency_data.iterrows():
        for orphan in orphaned_data[orphaned_data['client_name'] == dep['client_name']]:
            print(f"{orphan['client_name']}, {orphan['vvw_name']}")
    return

def resolve_file_paths():
    return

def update_file_contents():
    return

def main(orphaned_file, dependency_file, mono_bq_root):
    # read orphaned views file
    orphaned_data = read_orphaned_file(orphaned_file)
    
    # read dependencies file
        # ignore any which are orphaned
    read_dependency_file(dependency_file, orphaned_data)

    # resolve file names for deps
        # call out any which don't exist locally
    resolve_file_paths()

    # update files locally    
        # stage them in git
    update_file_contents()

if __name__ == "__main__":
    main(
        "orphaned_vvw_report.csv", 
        "dep_report.csv",
        "/users/andrew.lindsay/Projects/mono/infrastructure/gcloud/client/bq"
    )