import json
import subprocess
import sys
import argparse
from helpers.BqTransferClient import *

def main(argv):
    parser=argparse.ArgumentParser()

    parser.add_argument('-c', help='(Required) Specify a Client, with no sq value this script shows the names of all SQs')
    parser.add_argument('-sq', help='(Optional) Specify a Scheduled Query Name to get the Query')

    args=parser.parse_args()
    
    if (args.c):
        output = BqTransferClient(args.c).get_transfer_configs()
        for i in output:
            if i.display_name == "etl_sku_mappings":
                print(i)
            # if(i['dataSourceId'] == 'scheduled_query'):
            #     if(not args.sq or args.sq == i['displayName']):
            #         print(f'{i["displayName"]}, {i["name"]}')
            #     if(args.sq == i['displayName']):
            #         print(f'\t{i["params"]["query"]}')

if __name__ == "__main__":
   main(sys.argv[1:])