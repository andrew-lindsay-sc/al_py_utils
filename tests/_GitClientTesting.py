from helpers.StaticMethods import get_mono_path
from clients.GitClient import GitClient
from clients.BqClient import *

gc = GitClient(get_mono_path())
status = gc.get_active_branch_changes()
# parents_list = list([tuple(x.hexsha, x.parents) for x in gc.original_head.commit.iter_parents()])

changes = { BqClient.Operation.MODIFIED: set(), BqClient.Operation.DELETED: set() }
for diff in status:
    if diff.change_type == 'D' and diff.a_path[-4:] == '.sql':
        changes[BqClient.Operation.DELETED].add(diff.a_path)
    # Renames should clean up after themselves
    elif diff.change_type == 'R' and diff.b_path[-4:] == '.sql':
        changes[BqClient.Operation.DELETED].add(diff.a_path)
        changes[BqClient.Operation.MODIFIED].add(diff.b_path)
    elif diff.b_path[-4:] == '.sql':
        changes[BqClient.Operation.MODIFIED].add(diff.b_path)

path = Path(get_mono_path())
print(status)