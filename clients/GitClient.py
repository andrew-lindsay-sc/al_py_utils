import os
from git import DiffIndex, Repo, Commit
from helpers.StaticMethods import get_mono_path

class GitClient:
    """Helper class made to simplify git interaction"""
    def __init__(self, base_path):
        """
            (str) -> GitClient
            Initialize the GitClient using the specified base path.
        """
        self.base_path = base_path
        self.repo = self._get_repo()
        self.original_head = self.repo.active_branch
        self.original_branch = self.original_head.path.replace('refs/heads/','')
        self.master = self.repo.heads.master
        self.origin = self._get_origin()

    def _get_repo(self):
        """
            (None) -> Repo
            Initialize the repo for the instance's base path.
        """
        repo = Repo(self.base_path)
        assert not repo.bare
        return repo

    def get_commit_by_sha(self, sha):
        """
            (str) -> Commit
            Fetch the commit with the provided sha.
            ** Note ** This currently only supports full length sha
        """
        for c in self.repo.iter_commits():
            if c.hexsha == sha:
                return c

        raise Exception("Did not find the specified commit")    
    
    def _get_origin(self):
        """
            (None) -> Remote
            Fetch the configured origin remote for this repo.
        """
        return list(filter(lambda r: r.name == 'origin', self.repo.remotes))[0]

    def switch_to(self, head):
        """
            (Head) -> None
            Switch to the provided head after fetching from origin.
        """
        self.origin.fetch()
        head.checkout()  # checkout local "master" to working tree
        self.origin.pull()

    # I'm sure i'm doing this the hard way but I can't find an easier one
    def _get_branch_commits(self, ref_name: str) -> list[Commit]:
        ref_name = ref_name.replace('refs/heads/','')

        commits = list()
        commit = self.original_head.commit

        while commit:
            commits.append(commit)
            candidates = [x for x in commit.parents if self.original_branch in x.name_rev]
            if len(candidates) > 0:
                commit = candidates[0]
            else:
                commit = None

        return commits

    def get_active_branch_changes(self) -> DiffIndex:
        return self.original_head.commit.diff(
            f"HEAD~{len(self._get_branch_commits(self.repo.active_branch.path))}"
        )

    def revert_tracked_changes(self, to_revert: DiffIndex):
        self.repo.index.checkout([x.b_path for x in to_revert], force=True)

    def revert_untracked_changes(self, to_revert: list[str]):
        mono_path = get_mono_path()        
        for file in to_revert:
            os.remove(f"{mono_path}/{file}")