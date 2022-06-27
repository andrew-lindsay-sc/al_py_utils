from helpers.SqlObjectReferences import SqlObjectReferences
from helpers.SqlObject import SqlObject

my_obj = SqlObject('soundcommerce-client-lantern.core.vw_marketing_campaign_mapping')
refs = SqlObjectReferences(my_obj)
refs.print_children()
for child in refs.get_child_set():
    print(child)