DECLARE client STRING;
DECLARE query STRING;

SET client = 'truthbar';
SET query = '''
  select 1 as result from `soundcommerce-client-{client}.core.mv_marketing_campaign_mapping` union all
  select 1 as result from `soundcommerce-client-{client}.core.mv_marketing_source_medium_override` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_adspend` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_campaign_all` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_campaign_mapping` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_order_attribution` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_campaign_override` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_source_medium_mapping` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_source_medium_mapping_with_stats` union all
  select 1 as result from `soundcommerce-client-{client}.core.vw_marketing_source_medium_override` limit 0''';

SET query = replace(query, '{client}', client);
select query;
EXECUTE IMMEDIATE(query);