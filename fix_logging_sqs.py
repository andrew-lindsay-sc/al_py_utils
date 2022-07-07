from clients.BqTransferClient import *

clients = ['eddieb', 'brcc', 'ghcc', 'tempo', 'ftd', 'cbi', 'bala', 'pacsun', 'ovme']
for client in clients:
    bq_client = BqTransferClient(client)
    updates = {
        "display_name": "Execute logging.proc_validate_test_schemas",
        "disabled": True
    }

    bq_client.update_transfers(updates, False)      

    