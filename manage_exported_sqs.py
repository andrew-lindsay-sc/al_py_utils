from helpers.GitClient import *
from helpers.StaticMethods import *

def match_on_display_name(not_matched, diffs: DiffIndex, untracked_files: list[str]):
    mono_path = get_mono_path()
    diffs_to_keep = DiffIndex()
    untracked_files_to_keep = set()

    # Try tracked files first
    for change in diffs:
        if change.b_path[-5:] != '.json':
            continue
        client = change.b_path.split('/')[4]
        with open(mono_path+'/'+change.b_path, 'r', newline = '\n') as f:
            sq_json = json.load(f)
            display_name = sq_json['displayName']
            
            if display_name in not_matched[client]:
                # not_matched[client].remove(display_name)
                diffs_to_keep.append(change)

    for change in untracked_files:
        if change[-5:] != '.json':
            continue
        client = change.split('/')[4]
        with open(mono_path+'/'+change, 'r', newline = '\n') as f:
            sq_json = json.load(f)
            display_name = sq_json['displayName']
            if display_name in not_matched[client]:
                # not_matched[client].remove(display_name)
                untracked_files_to_keep.add(change)

    return diffs_to_keep, untracked_files_to_keep

manifest_path = '/Users/andrew.lindsay/Documents/sq_changes.txt'
files = dict()
file_count = 0
with open(manifest_path, 'r') as manifest:
    # relative_base_path = 'infrastructure/gcloud/client/bq'
    for line in manifest.read().split('\n'):
        if line[-3:] == '...':
            client = line[:-3]
            files[client] = list()
        elif client != 'jco':
            files[client].append(
                # f"{relative_base_path}/{client}/scheduled/{line.strip().replace(' ','-')}.json".lower()
                line.strip()
            )
            file_count += 1

# print(files)
git_client = GitClient(get_mono_path())#+'/infrastructure/gcloud/client/bq')
diffs = git_client.get_git_status()
untracked_files = list() if len(git_client.repo.untracked_files) == 0 else git_client.repo.untracked_files
diffs_to_keep, untracked_files_to_keep = match_on_display_name(files, diffs, untracked_files)
diffs_to_revert = DiffIndex(filter(lambda d: d.b_path not in [k.b_path for k in diffs_to_keep], diffs))
untracked_to_delete = set(untracked_files) - untracked_files_to_keep

git_client.revert_tracked_changes(diffs_to_revert)
git_client.revert_untracked_changes(untracked_to_delete)

print('wow')