insert overwrite table dwd.dwd_fd_snowplow_click_impr partition (pt = '${pt}')
SELECT
/*+ REPARTITION(50) */
project AS project_name,
upper(country) as country,
platform_type,
page_code,
goods_event_struct.list_type AS list_type,
domain_userid,
event_name,
session_id,
derived_ts as derived_tstamp
FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
WHERE event_name in ('goods_click', 'goods_impression')
AND pt = '${pt}'
AND project is not null
AND project != ''
AND length(country) = 2
AND platform_type is not null
AND platform_type != ''
AND page_code != '404'
AND page_code != ''
AND goods_event_struct.list_type is not null
AND goods_event_struct.list_type != 'null'
AND goods_event_struct.list_type != 'NULL';

