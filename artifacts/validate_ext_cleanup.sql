-- NOTE: This must be run in the client's project
--  If there are no results, that is success

SELECT table_schema, table_name, view_definition
FROM region-us.INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'ext' and table_name in (
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
