import base64
from google.cloud import bigquery_datatransfer
import json
from types import SimpleNamespace
import datetime
import os

# Entrypoint for the cloud function
def sq_invoker_handle_pub(event, context):
    
    event_object = parse_event_data(event)
    print(f'Transfer Config Name: {event_object.name}')

    config_table = get_env_var('config_table')

    project_name = event_object.name.split("/")[1]
    client, parent = prepare_client(get_env_var('project_id'))

    sq_config = fetch_sq_config(client, config_table)

    # If the triggering event was not an SQ (kubernetes job) set stage to 1
    if(event_object.name.find('transferConfigs') == -1): 
        stage = 1
    # Otherwise identify stage of triggering SQ
    else:
        # There should never be more than one match since SQ names are unique
        stage = [x for x in sq_config if x.name == event_object.name][0].stage

        # Update the status of the SQ which triggered this event
        update_finished_query(client, event_object, config_table)

    # Identify any SQs in this stage which are still pending
    remaining_same_stage = [x for x in sq_config if x.stage == stage and x.status in ('pending', 'processing')]

    # If any exist, do nothing
    if (len(remaining_same_stage) > 0):
        return 0
    else:
        # If none exist and there is a next stage, kick it off
        next_stage = [x for x in sq_config if x.stage == stage + 1]
        if(len(next_stage) > 0):
            responses = invoke_next_stage(client, parent, next_stage)
            update_sq_config(client, responses, config_table)

    # If none exist and there is no next stage, there is nothing to do
    return 0

def get_env_var(request):
    return os.environ.get(request, 'Specified environment variable is not set.')

# Takes in the event object and returns an object from the deserialized json
def parse_event_data(event):
    if 'data' in event:
        event_payload = base64.b64decode(event['data']).decode('utf-8')
    else:
        return -1

    # Parse JSON into an object with attributes corresponding to dict keys.
    return json.loads(event_payload, object_hook=lambda d: SimpleNamespace(**d))
    
# Retrieves SQ config data stored in BQ
def fetch_sq_config(client, config_table):
    query_job = client.query(
        """
        select * 
        from {config_table}
        where enabled'
        """.format(config_table=config_table)
    )

    return query_job.result()  # Waits for job to complete.

# Set tracking information on SQ config table
def update_finished_query(client, event_object, config_table):
    # TODO: handle error status
    query_job = client.query(
        """
        update {config_table}
        set 
            status = 'success',
            end_time = timestamp({updateTime})
        where name = {name}
        --where status='pending'
        """.format(
            config_table=config_table, 
            startTime = event_object.startTime, 
            updateTime = event_object.updateTime,
            name = event_object.name
        )
    )

    return query_job.result()  # Waits for job to complete. 
    # Todo: validate results / check for errors

# invoke all SQs in the next stage
def invoke_next_stage(client, parent, next_stage):
    names_to_start = map(lambda x: x.name, next_stage)

    # Fetch SQ config from BQ client for later use
    transfer_configs = client.list_transfer_configs(parent=parent)

    transfer_configs_to_start = [x for x in transfer_configs if x.name in names_to_start]

    # This part is very experimental and was taken from the following google article:
    #   https://cloud.google.com/bigquery-transfer/docs/working-with-transfers#updating_a_transfer

    now = datetime.datetime.now(datetime.timezone.utc)
    start_time = now - datetime.timedelta(days=5)
    end_time = now - datetime.timedelta(days=2)

    # Some data sources, such as scheduled_query only support daily run.
    # Truncate start_time and end_time to midnight time (00:00AM UTC).
    start_time = datetime.datetime(
        start_time.year, start_time.month, start_time.day, tzinfo=datetime.timezone.utc
    )
    end_time = datetime.datetime(
        end_time.year, end_time.month, end_time.day, tzinfo=datetime.timezone.utc
    )   

    responses = list()
    for to_start in transfer_configs_to_start:
        responses.append( client.schedule_transfer_runs(
            parent=to_start.name,
            start_time=start_time,
            end_time=end_time,
        ))

        print("Started transfer runs:")
        for run in responses.runs:
            print(f"backfill: {run.run_time} run: {run.name}")

    # TODO: handle errors in transfer run attempt
    return responses

def update_sq_config(client, responses, config_table):
    # Build a list of sql statements to use as source data in the update
    response_sql_statements = list()
    for response in responses:
        for run in responses.runs:
            response_sql.append(f'select "{run.name}" as name, timestamp({run.run_time}) as start_time')

    response_sql = "\nunion all\n".join(response_sql_statements)

    # TODO: handle error status
    query_job = client.query(
        """
        update {config_table}
        set 
            status = 'processing',
            start_time = sq_data.start_time
        from (
            {response_sql}
        ) sq_data
        where name = sq_data.name
        """.format(
            config_table=config_table,
            response_sql=response_sql
        )
    )

    return query_job.result()  # Waits for job to complete. 
    # Todo: validate results / check for errors

# Sets up the bigquery client for later use
def prepare_client(project_id):
    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = client.common_project_path(project_id)
    return client, parent


# print("""This Function was triggered by messageId {} published at {} to {}
# """.format(context.event_id, context.timestamp, context.resource["name"]))