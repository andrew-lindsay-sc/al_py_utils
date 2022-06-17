from typing import get_origin
from git import Repo

class GitClient:
    """Helper class made to simplify git interaction"""
    def __init__(self, base_path):
        self.base_path = base_path
        self.repo = self.get_repo()
        self.original_head = self.repo.heads[0]
        self.master = self.repo.heads.master
        self.origin = self.get_origin()

    def get_repo(self):
        repo = Repo(self.base_path)
        assert not repo.bare
        return repo

    def get_commit_by_sha(self, sha):
        for c in self.repo.iter_commits():
            if c.hexsha == sha:
                return c

        raise Exception("Did not find the specified commit")    
    
    def get_origin(self):
        return list(filter(lambda r: r.name == 'origin', self.repo.remotes))[0]

    def switch_to(self, head):
        self.origin.fetch()
        head.checkout()  # checkout local "master" to working tree
        self.origin.pull()