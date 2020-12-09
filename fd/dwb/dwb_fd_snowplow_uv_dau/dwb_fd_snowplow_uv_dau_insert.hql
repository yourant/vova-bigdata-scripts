INSERT OVERWRITE TABLE dwb.dwb_fd_snowplow_uv_dau PARTITION (pt = '${pt}')
select
/*+ REPARTITION(1) */
project,
platform_type,
count(distinct session_id) as dau,
count(distinct domain_userid) as uv
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}'
and event_name in ('page_view', 'screen_view', 'add', 'checkout', 'checkout_option', 'purchase')
group by platform_type,project