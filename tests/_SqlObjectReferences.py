from domain.SqlObjectReferences import SqlObjectReferences
from domain.SqlObject import SqlObject

my_obj = SqlObject('soundcommerce-client-lantern.core.vw_marketing_campaign_mapping')
refs = SqlObjectReferences(my_obj)
refs.print_children()
for child in refs.get_children():
    print(child)