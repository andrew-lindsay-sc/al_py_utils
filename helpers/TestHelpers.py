import os
import shutil
from helpers.StaticMethods import get_bq_path

class TempFile:
    def __init__(self, file_path: str, content: str):
        self._file_path = file_path 
        self._deepest_existing = None

        if os.path.exists(self._file_path):
            raise Exception("Path specified for TempFile already exists, use a non-existent path.")

        self._path_parts = file_path.split('/')
        self._directory = '/'.join(self._path_parts[:-1])        

        if not os.path.exists(self._directory):
            # Resolve current deepest folder that exists so that we can restore that state later
            parts_stripped = -2
            test_path = '/'.join(self._path_parts[:parts_stripped])
            # The len comparison is a safeguard against wiping out too much
            while test_path and (len(self._path_parts) - abs(parts_stripped)) > len(get_bq_path().split('/')):
                if os.path.exists(test_path):
                    self._deepest_existing = test_path
                    break
                parts_stripped -= 1
                test_path = '/'.join(self._path_parts[:parts_stripped])

            os.makedirs(self._directory)

        with open(self._file_path, 'w') as f:
            f.write(content)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._deepest_existing and self._directory != self._deepest_existing:
            to_delete = '/'.join(self._path_parts[:len(self._deepest_existing.split('/'))+1])
            shutil.rmtree(to_delete)
        else:
            os.remove(self._file_path)