import os
from git import DiffIndex, Head, Repo, Commit
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
        self.master = self.repo.heads.master
        self.origin = self._get_origin()

    @property
    def original_branch(self):
        return self.original_head.path.replace('refs/heads/','')

    @property
    def active_branch(self) -> str:
        return self.repo.active_branch.path.replace('refs/heads/','')

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

    def switch_to(self, head: Head):
        """
            (Head) -> None
            Switch to the provided head after fetching from origin.
        """
        self.origin.fetch()
        head.checkout()

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

    # def create_branch(self, branch_name: str, source_head: Head = None) -> Head:
    #     new_head = Head(self.repo, f'refs/heads/{branch_name}')
    #     self.repo.heads.append(new_head)
    #     return new_head

    def checkout_branch(self, branch_shorthand: str, create_if_needed: bool = False):
        matches = [x for x in self.repo.heads if branch_shorthand in x.path]
        if len(matches) > 1:
            raise Exception(f"Multiple candidates for \"{branch_shorthand}\" found, be more specific.")
        elif len(matches) == 0:
            if not create_if_needed:
                raise Exception(f"Branch {branch_shorthand} not found.")
            else:
                new_head = self.repo.create_head(branch_shorthand)
                self.switch_to(new_head)
        else:
            self.switch_to(matches[0])