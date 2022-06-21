import json
import subprocess
import sys
import argparse

def main(argv):
    parser=argparse.ArgumentParser()

    parser.add_argument('-c', help='(Required) Specify a Client, with no sq value this script shows the names of all SQs')
    parser.add_argument('-sq', help='(Optional) Specify a Scheduled Query Name to get the Query')

    args=parser.parse_args()
    
    if (args.c):
        client_name = args.c
        bashCommand = f'bq ls --transfer_config --transfer_location=US --format=prettyjson --project_id=soundcommerce-client-{client_name}'
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        data = json.loads(output)
        for i in data:
            if(i['dataSourceId'] == 'scheduled_query'):
                if(not args.sq or args.sq == i['displayName']):
                    print(f'{i["displayName"]}, {i["name"]}')
                if(args.sq == i['displayName']):
                    print(f'\t{i["params"]["query"]}')

if __name__ == "__main__":
   main(sys.argv[1:])