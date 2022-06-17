from helpers.bq_client import *

client = bq_client("bbb")
client.check_objects_exist(list())