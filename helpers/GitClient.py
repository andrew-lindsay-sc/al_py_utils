from typing import get_origin
from git import Repo

class GitClient:
    """Helper class made to simplify git interaction"""
    def __init__(self, base_path):
        """
            (str) -> GitClient
            Initialize the GitClient using the specified base path.
        """
        self.base_path = base_path
        self.repo = self.get_repo()
        self.original_head = self.repo.heads[0]
        self.master = self.repo.heads.master
        self.origin = self.get_origin()

    def get_repo(self):
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
    
    def get_origin(self):
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