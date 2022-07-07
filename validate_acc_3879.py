# pip install pandas

import argparse
from distutils.command.clean import clean
import os
from google.cloud import bigquery
import subprocess
from helpers.StaticMethods import *

from clients.BqTransferClient import BqTransferClient
from clients.BqClient import BqClient
from clients.BqDeploymentClient import BqDeploymentClient
# from google.cloud.bigquery_datatransfer_v1.types.TransferState
from google.cloud.bigquery_datatransfer_v1.types import TransferState

# This list is all objects created by provision-funnel before changes for ACC-3879
all_funnel_objects = [
    'client_directaccess.sc_marketing_campaign_performance',
    'client_directaccess.sc_adspend_order_attribution',
    'core.mv_marketing_source_medium_override',
    'core.vvw_marketing_funnel_data_1',
    'core.vw_marketing_campaign_mapping',
    'core.vw_marketing_source_medium_override',
    'ext.gs_marketing_source_medium_override',
    'ext.mv_marketing_source_medium_override',
    'ext.vvw_adspend_order_attribution_0',
    'ext.vvw_marketing_campaign_all_0',
    'ext.vvw_marketing_campaign_mapping_0',
    'ext.vvw_marketing_campaign_override_0',
    'ext.vvw_marketing_campaign_performance_0',
    'ext.vvw_marketing_funnel_data_0',
    'ext.vvw_marketing_funnel_data_1',
    'ext.vvw_marketing_order_attribution_0',
    'ext.vvw_marketing_source_medium_all_0',
    'ext.vvw_marketing_source_medium_mapping_0',
    'ext.vw_adspend_order_attribution',
    'ext.vw_marketing_adspend',
    'ext.vw_marketing_campaign_all',
    'ext.vw_marketing_campaign_mapping',
    'ext.vw_marketing_campaign_override',
    'ext.vw_marketing_campaign_performance',
    'ext.vw_marketing_funnel_data',
    'ext.vw_marketing_order_attribution',
    'ext.vw_marketing_orders',
    'ext.vw_marketing_source_medium_all',
    'ext.vw_marketing_source_medium_mapping',
    'ext.vw_marketing_source_medium_mapping_with_stats',
    'ext.vw_marketing_source_medium_override',
    'ext.vw_order_attribution',
    'funnel.all_normalized_export_view',
    'looker.sc_adspend_order_attribution',
    'looker.sc_marketing_campaign_performance',
    'looker.vw_base_adspend_order_attribution'
]

provision_funnel_objects = [        
    "ext.gs_marketing_source_medium_override",
    "core.mv_marketing_source_medium_override",
    "core.vw_marketing_source_medium_override",
]

provision_client_objects = set(all_funnel_objects) - set(provision_funnel_objects)

def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Required) Specify a Client, with no sq value this script shows the names of all SQs')

def validate_args(args):
    if (not args.c):
        raise Exception("Client is required, use -c to set.")

def prepare_bq_client(client_name):
    project_id = "soundcommerce-client-"+client_name
    client = bigquery.Client(project=project_id)
    return client

def validate_provision_client(bq_client: BqDeploymentClient) -> bool:
    objects_to_validate = provision_client_objects
    existing_objects = bq_client.check_objects_exist(objects_to_validate)

    missing_views = set(objects_to_validate) - set(existing_objects)
    if len(missing_views) > 0:
        missing_views_str = ',\n\t'.join(missing_views)
        print_fail(f"The following objects are missing:\n\t{missing_views_str}")
        return False
    
    print_success("All expected provision client objects are present.")
    return True

def validate_ext_cleanup(bq_client: BqDeploymentClient) -> bool:
    objects_to_validate = [
        "ext.vw_marketing_adspend",
        "ext.vw_marketing_campaign_all",
        "ext.vw_marketing_campaign_mapping",
        "ext.vw_marketing_order_attribution",
        "ext.vw_marketing_source_medium_mapping",
        "ext.vw_marketing_source_medium_mapping_with_stats",
        "ext.vw_marketing_source_medium_override",
        "ext.mv_marketing_campaign_mapping",
        "ext.mv_marketing_source_medium_override",
        "ext.vvw_marketing_adspend_0",
        "ext.vw_marketing_campaign_all_0",
        "ext.vw_marketing_campaign_mapping_0",
        "ext.vw_marketing_order_attribution_0",
        "ext.vw_marketing_campaign_override_0",
        "ext.vw_marketing_source_medium_mapping_0",
        "ext.vw_marketing_source_medium_override_0"
    ]

    existing_objects = bq_client.check_objects_exist(objects_to_validate)
    
    is_success = len(existing_objects) == 0
    if not is_success:
        remaining_views = ',\n\t'.join(existing_objects)
        print(f"Failure - These objects still exist:\n\t{remaining_views}")

    print_success("All expected migrated ext views were removed.")
    return is_success

def validate_provision_funnel(bq_client: BqDeploymentClient) -> bool:
    objects_to_validate = provision_funnel_objects
    existing_objects = bq_client.check_objects_exist(objects_to_validate)

    missing_views = set(objects_to_validate) - set(existing_objects)
    if len(missing_views) > 0:
        missing_views_str = ',\n\t'.join(missing_views)
        print(f"The following funnel objects are missing:\n\t{missing_views_str}")
        return False
    
    transfer_client = BqTransferClient(bq_client.client_name)
    funnel_transfer_names = [
        'Materialize Core mv_marketing_source_medium_override',
    ]
    funnel_transfers = \
        list(
            filter(
                lambda tc: tc.display_name in funnel_transfer_names, 
                transfer_client.get_transfer_configs()
            )
        )

    if len(funnel_transfers) == 0:
        print_fail(f'Scheduled Query `{funnel_transfer_names[0]}` was not deployed')
    elif len(funnel_transfers) == 1:
        if not funnel_transfers[0].state == TransferState.RUNNING:
            print_warn(f"Scheduled query `{funnel_transfers[0].display_name}` was deployed but has not completed. See BQ console.")
            return False
        if not funnel_transfers[0].state == TransferState.FAILED:
            print_warn(f"Scheduled query `{funnel_transfers[0].display_name}` was deployed but failed to run. See BQ console.")
            return False
        else:
            print_success(f"Scheduled query `{funnel_transfers[0].display_name}` was deployed successfully.")
    else:
        print_fail("Multiple SQ matches were found, this should not happen.")
        return False

    print_success("All expected provision funnel objects are present.")
    return True

def objects_to_paths(objects, client_name):
    file_paths = []
    for obj in objects:
        if 'mv_' in obj or 'gs_' in obj:
            obj_type = 'schema'
        else:
            obj_type = 'view'
        file_paths.append(f"{get_bq_path()}/{client_name}/{obj.replace('.',f'/{obj_type}/')}.sql")

    return file_paths

def uninstall_funnel(bq_client: BqDeploymentClient) -> bool:
    funnel_changes = {BqClient.Operation.DELETED: objects_to_paths(all_funnel_objects, bq_client.client_name)}
    bq_client.deploy_files(funnel_changes[BqClient.Operation.DELETED], BqClient.Operation.DELETED)
    bq_client.validate_deployment(list(), funnel_changes[BqClient.Operation.DELETED])

    transfer_client = BqTransferClient(bq_client.client_name)
    funnel_transfer_names = [
        'Materialize Looker sc_adspend_order_attribution',
        'Materialize Looker sc_marketing_campaign_performance',
        'Materialize ext mv_marketing_source_medium_override',
        'Materialize Core mv_marketing_source_medium_override'
    ]
    funnel_transfers = \
        list(
            filter(
                lambda tc: tc.display_name in funnel_transfer_names, 
                transfer_client.get_transfer_configs()
            )
        )
    transfer_client.delete_transfers(funnel_transfers)

# def provision_client(client):
#     os.chdir(get_bq_path())
#     os.system(f"./provision-client.sh --all {client} > provision_client_log_{client}.txt")

def provision_client(bq_client: BqDeploymentClient):
    to_deploy = {BqClient.Operation.MODIFIED, objects_to_paths(provision_client_objects, bq_client.client_name)}
    bq_client.deploy_files(to_deploy[BqClient.Operation.MODIFIED], BqClient.Operation.MODIFIED, True)
    bq_client.validate_deployment(list(), to_deploy[BqClient.Operation.MODIFIED])

def provision_funnel(client):
    os.chdir(get_bq_path())
    os.system(f"amm provision-funnel.sc {client} true true true > provision_funnel_log_{client}.txt")

def main(clean_install = False, provision_client_only = False):
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)     

    if clean_install and not provision_client_only:
        bq_client = BqDeploymentClient(args.c)
        uninstall_funnel(bq_client)

    bq_client = BqClient(args.c)

    provision_client(bq_client)
    validate_provision_client(bq_client)

    if not provision_client_only:
        provision_funnel(args.c)
        validate_provision_funnel(bq_client)
        validate_ext_cleanup(bq_client)
    
if __name__ == "__main__":
    main(clean_install = True, provision_client_only = True)