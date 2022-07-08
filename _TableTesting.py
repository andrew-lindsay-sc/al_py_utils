from clients.BqClient import BqClient

bq_client = BqClient('pacsun')
order_history = bq_client.instance.get_routine('ext.fn_zip_first_five')
print(1)