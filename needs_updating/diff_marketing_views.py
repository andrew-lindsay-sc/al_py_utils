import os
from pathlib import Path
import difflib

def main():
    client_bq_dir = '/Users/andrew.lindsay/Projects/mono/infrastructure/gcloud/client/bq'
    ext_view_dir= client_bq_dir+'/ext/view'

    os.chdir(client_bq_dir)

    ext_marketing_views = Path(ext_view_dir).glob('*vw_marketing*.sql')
    for ext_view in ext_marketing_views:
        client_versions = Path('.').rglob(ext_view.name)
        with open(ext_view.name+'_diffs.txt', 'w') as f:
            for client_view in client_versions:
                if client_view._str.split("/")[0] in \
                    ['rainbow2', 'truthbar', 'looker']:
                    continue
                with open(ext_view, 'r') as hosts0:
                    with open(client_view, 'r') as hosts1:
                        host0_lines = hosts0.readlines()
                        for line in host0_lines: line = line.strip()

                        host1_lines = hosts1.readlines()
                        for line in host1_lines: line = line.strip()
                        
                        diff = difflib.unified_diff(
                            host0_lines,
                            host1_lines,
                            fromfile=ext_view._str,
                            tofile=client_view._str,
                            n=0
                        )
                        for line in diff:
                            f.write(line)
main()