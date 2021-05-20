sql="

drop table tmp.tmp_fd_ga_channel_campaign;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_fd_ga_channel_campaign
(
    order_id     bigint COMMENT 'd_date',
    domain_userid string COMMENT 'd_date',
    pre_event_time string COMMENT 'd_date',
    pre_ga_channel string COMMENT 'd_date',
    pre_mkt_source string COMMENT 'd_date',
    pre_campaign_name string COMMENT 'd_date',
    pre_campaign_id string COMMENT 'd_date',
    pre_adgroup_id string COMMENT 'd_date',
    pre_mkt_medium string COMMENT 'd_date',
    pre_mkt_term string COMMENT 'd_date'
) COMMENT 'tmp_fd_ga_channel_campaign'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS tmp_fd_ga_channel_campaign (
  order_id int(11) NOT NULL,
  domain_userid varchar(64) NOT NULL DEFAULT '0' comment '商品id',
  pre_event_time varchar(128) NOT NULL DEFAULT '',
  pre_ga_channel varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_source varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_name varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_adgroup_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_medium varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_term varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://bd-warehouse-maxscale.gitvv.com:3311/artemis \
--username market --password MyF4k2y9jJSv \
--m 1 \
--table tmp_fd_ga_channel_campaign \
--hcatalog-database tmp \
--hcatalog-table tmp_fd_ga_channel_campaign \
--fields-terminated-by '\001'






insert overwrite table tmp.tmp_fd_ga_channel_campaign
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
from dwd.dwd_fd_session_channel where pt>='2021-04-12'
union all
select
null ga_channel,
null mkt_source,
null campaign_name,
null campaign_id,
null adgroup_id,
null mkt_medium,
null mkt_term,
sp_duid domain_userid,
null derived_ts,
from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss') ts,
'order' event_name,
order_id
from dwd.dwd_fd_order_info where from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')>='2021-05-12 00:00:00'
) t
) t where event_name='order';
"