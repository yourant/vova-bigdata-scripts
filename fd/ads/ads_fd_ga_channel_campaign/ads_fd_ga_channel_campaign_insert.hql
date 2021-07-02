insert overwrite table ads.ads_fd_ga_channel_campaign partition (pt='${pt}')
select
/*+ REPARTITION(1) */
order_id,
nvl(domain_userid,'') domain_userid,
nvl(pre_event_time,'') pre_event_time,
nvl(pre_ga_channel,'') pre_ga_channel,
nvl(pre_mkt_source,'') pre_mkt_source,
nvl(pre_campaign_name,'') pre_campaign_name,
nvl(pre_campaign_id,'') pre_campaign_id,
nvl(pre_adgroup_id,'') pre_adgroup_id,
nvl(pre_mkt_medium,'') pre_mkt_medium,
nvl(pre_mkt_term,'') pre_mkt_term
from
(
select
COALESCE(ga_channel, last_value(ga_channel, true)
                               OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_ga_channel,
COALESCE(mkt_source, last_value(mkt_source, true)
                               OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_mkt_source,
COALESCE(campaign_name, last_value(campaign_name, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_campaign_name,
COALESCE(campaign_id, last_value(campaign_id, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_campaign_id,
COALESCE(adgroup_id, last_value(adgroup_id, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_adgroup_id,
COALESCE(mkt_medium, last_value(mkt_medium, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_mkt_medium,
COALESCE(mkt_term, last_value(mkt_term, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_mkt_term,
COALESCE(derived_ts, last_value(derived_ts, true)
                              OVER (PARTITION BY domain_userid ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_event_time,
domain_userid,
ts,
event_name,
order_id
from
(
select distinct
if(ga_channel='',null,ga_channel) ga_channel,
if(mkt_source='',null,mkt_source) mkt_source,
if(campaign_name='',null,campaign_name) campaign_name,
if(campaign_id='',null,campaign_id) campaign_id,
if(adgroup_id='',null,adgroup_id) adgroup_id,
if(mkt_medium='',null,mkt_medium) mkt_medium,
if(mkt_term='',null,mkt_medium) mkt_term,
domain_userid,
derived_ts,
derived_ts ts,
'click' event_name,
null order_id
from dwd.dwd_fd_session_channel where pt>='${pre_month}'
union all
select distinct
if(ga_channel='',null,ga_channel) ga_channel,
if(mkt_source='',null,mkt_source) mkt_source,
if(campaign_name='',null,campaign_name) campaign_name,
if(campaign_id='',null,campaign_id) campaign_id,
if(adgroup_id='',null,adgroup_id) adgroup_id,
if(mkt_medium='',null,mkt_medium) mkt_medium,
if(mkt_term='',null,mkt_medium) mkt_term,
domain_userid,
derived_ts,
derived_ts ts,
'click' event_name,
null order_id
from dwd.dwd_fd_session_channel_arc where pt>='${pt}'
union all
select
null ga_channel,
null mkt_source,
null campaign_name,
null campaign_id,
null adgroup_id,
null mkt_medium,
null mkt_term,
ud.sp_duid domain_userid,
null derived_ts,
unix_timestamp(to_utc_timestamp(order_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss') ts,
'order' event_name,
order_id
from ods_fd_vb.ods_fd_order_info_h oi
left join (
select du.user_id, du.sp_duid
from (
select user_id, sp_duid, row_number() OVER (PARTITION BY user_id ORDER BY last_update_time DESC) AS rank
from ods_fd_vb.ods_fd_user_duid_h
where sp_duid IS NOT NULL
) du
 where du.rank = 1
) ud ON oi.user_id = ud.user_id
 where to_date(from_unixtime(unix_timestamp(to_utc_timestamp(order_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss'))>='${pre_two_pt}'
) t
) t where event_name='order';
