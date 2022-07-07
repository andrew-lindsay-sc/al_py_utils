# Must run this before using this script:
#     pip install --upgrade google-cloud-bigquery

from google.cloud import bigquery
import argparse

def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Optional) Specify one or more Client(s), if not provided all clients will be updated (e.g. "bbb, pacsun"')
    parser.add_argument(
        '-t', help='(Optional) Specify one or more tables"')

def validate_args(args):
    if (not args.c):
        raise Exception("Client is required, use -c to set.")
    elif(not args.t):
        raise Exception("Table/view is required, use -t to set.")

def prepare_bq_client(client_name):
    project_id = "soundcommerce-client-"+client_name
    client = bigquery.Client(project=project_id)
    return client
    
def get_tables(view, client, bq_client):    
    project_name = 'soundcommerce-client-'+client
    return bq_client.list_tables(f"{project_name}.ext")

def sanitize_type(type):
    if type == 'FLOAT':
        return 'FLOAT64'
    else:
        return type

def build_definition(schema, depth = 0):
    if depth == 0:
        definition = 'select\n'
    else:
        definition = ''

    for field in schema:
        spacer = '\t'*(depth+1)
        is_repeated = field.mode == "REPEATED"
        if len(field.fields) > 0:
            if(is_repeated):
                definition += spacer+'[\n'

            definition += spacer+'struct (\n'
            definition += build_definition(field.fields, depth+1)
            definition += spacer+')\n'

            if(is_repeated):
                definition += '\n'+spacer+']\n'
        else:
            if(is_repeated):
                definition += spacer+f"cast ( null as ARRAY<{sanitize_type(field.field_type)}> ) "
            else:
                definition += spacer+f"cast ( null as {sanitize_type(field.field_type)} ) "
        
        
        if definition[-1] == '\n':
            definition += spacer
        definition += (f"as {field.name}")

        if(field != schema[-1]):
            definition += ','
        definition += '\n'

    return definition;

def main():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)     

    bq_client = prepare_bq_client(args.c)

    # tables = get_tables(args.t, args.c, bq_client)
    # for table in tables:
    #     table_detail = bq_client.get_table(f"{table.dataset_id}.{table.table_id}")
    table_detail = bq_client.get_table(args.t)
    definition = build_definition(table_detail.schema)
    print(definition)

if __name__ == "__main__":
    main()