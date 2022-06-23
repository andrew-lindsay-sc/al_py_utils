create or replace table `soundcommerce-data-sandbox.Andrew_Testing.sq_invoker_config` (
    stage int64,
    display_name string,
    name string,
    status string,
    start_time timestamp,
    end_time timestamp,
    enabled bool,
    error string
);
insert `soundcommerce-data-sandbox.Andrew_Testing.sq_invoker_config` (
    stage, 
    display_name,
    name,
    status,
    enabled
)
select
    data.stage,
    data.display_name,
    data.name,
    'pending',
    true
from (
    SELECT 1 as stage, "Execute ext proc_process_cuids" as display_name, "projects/731356616585/locations/us/transferConfigs/62410085-0000-296a-8fcb-14223bb22d4a" as name
    union all SELECT 1, "Materialize Staging mv_line_items", "projects/731356616585/locations/us/transferConfigs/62396b52-0000-2788-91e2-582429aec7d0"
    union all SELECT 1, "Materialize Staging vw_order_customer_loyalty", "projects/731356616585/locations/us/transferConfigs/62996a13-0000-26ac-b601-089e082cd594"
    union all SELECT 1, "Materialize vw_ecom_order_ship_bill_address", "projects/731356616585/locations/us/transferConfigs/62956786-0000-2077-9c09-24058873873c"
    union all SELECT 1, "Materialize vw_ecom_sales_transactions", "projects/731356616585/locations/us/transferConfigs/628d4a8b-0000-2dba-80da-94eb2c0b0f52"
    union all SELECT 1, "Materialize vw_fulfillments", "projects/731356616585/locations/us/transferConfigs/624d96d9-0000-2e2e-bded-001a1142d130"
    union all SELECT 2, "Materialize core orders_current (proc)", "projects/731356616585/locations/us/transferConfigs/62db77c0-0000-261b-8b9c-089e08253c30"
    union all SELECT 2, "Materialize looker sc_inventory_by_location", "projects/731356616585/locations/us/transferConfigs/626500fa-0000-2387-8056-14c14ef26860"
    union all SELECT 2, "Materialize looker sc_products", "projects/731356616585/locations/us/transferConfigs/6265010f-0000-2387-8056-14c14ef26860"
    union all SELECT 2, "Materialize looker sc_sku_categories", "projects/731356616585/locations/us/transferConfigs/624f106c-0000-27d7-9b2a-d4f547f139c8"
    union all SELECT 2, "Materialize looker sc_sku_images", "projects/731356616585/locations/us/transferConfigs/625525ca-0000-2cf7-a98b-2405887217bc"
    union all SELECT 3, "Execute ext proc_orders_base", "projects/731356616585/locations/us/transferConfigs/62ca65ec-0000-2e54-98be-94eb2c09dc06"
    union all SELECT 3, "Materialize core mv_customer_order_metrics", "projects/731356616585/locations/us/transferConfigs/6241bd9f-0000-2d5d-9d22-94eb2c19a502"
    union all SELECT 4, "Materialize looker sc_adspend_order_attribution", "projects/731356616585/locations/us/transferConfigs/6252c7c8-0000-28f1-a924-883d24fa6dd4"
    union all SELECT 4, "Materialize looker sc_customer_metrics_raw", "projects/731356616585/locations/us/transferConfigs/62578ab3-0000-2414-b26a-089e082bf248"
    union all SELECT 4, "Materialize looker sc_order_fulfillments", "projects/731356616585/locations/us/transferConfigs/626de4e6-0000-2921-92fa-94eb2c1cb230"
    union all SELECT 4, "Materialize looker sc_orders", "projects/731356616585/locations/us/transferConfigs/625774c2-0000-27de-af62-f40304381e94"
    union all SELECT 4, "Materialize looker sc_sku_mapping_gaps", "projects/731356616585/locations/us/transferConfigs/6256db85-0000-25c4-8076-089e082cf058"
    union all SELECT 5, "Materialize looker sc_adspend_order_attribution_agg", "projects/731356616585/locations/us/transferConfigs/62654527-0000-28ac-8d62-94eb2c1bf65a"
    union all SELECT 5, "Materialize looker sc_customers", "projects/731356616585/locations/us/transferConfigs/62725ded-0000-211e-91cb-f403043aa5ac"
    union all SELECT 5, "Materialize looker sc_order_metrics", "projects/731356616585/locations/us/transferConfigs/62654589-0000-28ac-8d62-94eb2c1bf65a"
    union all SELECT 6, "Materialize looker sc_order_items", "projects/731356616585/locations/us/transferConfigs/621c280c-0000-246a-819d-94eb2c1b8890"
) data