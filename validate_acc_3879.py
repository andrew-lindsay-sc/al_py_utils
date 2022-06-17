# pip install pandas

import argparse
from google.cloud import bigquery

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

def validate_core_migration(bq_client, client_name):
    views_to_validate = [
        "vw_marketing_adspend",
        "vw_marketing_campaign_all",
        "vw_marketing_campaign_mapping",
        "vw_marketing_order_attribution",
        "vw_marketing_campaign_override",
        "vw_marketing_source_medium_mapping",
        "vw_marketing_source_medium_mapping_with_stats",
        "vw_marketing_source_medium_override"
    ]

    mvs_to_validate = [
        "mv_marketing_campaign_mapping",
        "mv_marketing_source_medium_override",
    ]
    validation_query = f'''
        SELECT concat(table_schema,'.',table_name) as full_name
        FROM region-us.INFORMATION_SCHEMA.VIEWS
        WHERE table_schema = 'core' and table_name in (
            "{'", "'.join(views_to_validate)}"
        )
        UNION ALL
        SELECT concat(table_schema,'.',table_name) as full_name
        FROM region-us.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'core' and table_name in (
            "{'", "'.join(mvs_to_validate)}"
        )
    '''
    try:
        result = list((bq_client.query(validation_query).to_dataframe())['full_name'])

        all_objects_to_validate = []
        for item in views_to_validate:
            all_objects_to_validate.append('core.'+item)

        for item in mvs_to_validate:
            all_objects_to_validate.append('core.'+item)

        missing_views = set(all_objects_to_validate) - set(result)
        if len(missing_views) > 0:
            missing_views_str = ', '.join(missing_views)
            print(f"The following core views are missing: {missing_views_str}")
            return False
            
    except Exception as e:
        print(f"Failure validating created core views, exception:\n{e}")
        return False
    
    return True

def validate_ext_cleanup(bq_client):
    validation_query = '''
        SELECT concat(table_schema,'.',table_name) as full_name
        FROM region-us.INFORMATION_SCHEMA.VIEWS
        WHERE table_schema = 'ext' and table_name in (
            "vvw_baskets_0",
            "vw_marketing_adspend",
            "vw_marketing_campaign_all",
            "vw_marketing_campaign_mapping",
            "vw_marketing_order_attribution",
            "vw_marketing_source_medium_mapping",
            "vw_marketing_source_medium_mapping_with_stats",
            "vw_marketing_source_medium_override",
            "mv_marketing_campaign_mapping",
            "mv_marketing_source_medium_override",
            "vvw_marketing_adspend_0",
            "vw_marketing_campaign_all_0",
            "vw_marketing_campaign_mapping_0",
            "vw_marketing_order_attribution_0",
            "vw_marketing_campaign_override_0",
            "vw_marketing_source_medium_mapping_0",
            "vw_marketing_source_medium_override_0"
        )
    '''
    try:
        result = bq_client.query(validation_query).to_dataframe()
        unnested_results = list(result['full_name'])

        status = len(unnested_results) == 0
        if not status:
            remaining_views = ', '.join(unnested_results)
            print(f"Failure - These views still exist: {remaining_views}")
        return status
    except Exception as e:
        print(f"Failure validating cleanup, exception: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)     

    client = prepare_bq_client(args.c)

    validate_core_migration(client, args.c)
    validate_ext_cleanup(client)
    
if __name__ == "__main__":
    main()