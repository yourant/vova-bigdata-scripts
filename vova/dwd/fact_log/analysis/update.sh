#!/bin/bash
#指定日期和引擎
event_name="goods_click"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_log_analysis_d PARTITION (pt='${pt}',dp,event_name)
SELECT
aa.event_fingerprint,
aa.datasource,
aa.event_type,
aa.device_id,
aa.session_id,
aa.buyer_id,
aa.domain_userid,
aa.collector_tstamp,
aa.dvce_created_tstamp,
aa.derived_tstamp,
aa.collector_ts,
aa.dvce_created_ts,
aa.page_code,
aa.platform,
aa.app_version,
aa.language,
aa.currency,
aa.country,
aa.sys_lang,
aa.sys_country,
aa.os_type,
aa.os_version,
aa.media_source,
aa.uri,
aa.referer,
aa.test_info,
aa.geo_city,
aa.geo_country,
aa.device_model,
aa.gender,
aa.account_class,
aa.device_manufacturer,
aa.ip,
aa.list_type,
aa.element_id,
aa.element_name,
aa.element_type,
aa.element_position,
aa.view_type,
aa.enter_ts,
aa.leave_ts,
g.first_cat_id,
g.first_cat_name,
g.second_cat_id,
g.second_cat_name,
g.shop_price,
b.user_age_group age_range,
d.main_channel,
datediff('$pt',d.activate_time) act_days,
case when aa.datasource in ('airyclub','vova') then aa.datasource
         else 'others'
         end dp,
event_name
from
(
select
event_fingerprint,
datasource,
event_name,
event_type,
device_id,
session_id,
buyer_id,
domain_userid,
collector_tstamp,
dvce_created_tstamp,
derived_tstamp,
collector_ts,
dvce_created_ts,
page_code,
platform,
app_version,
language,
currency,
country,
sys_lang,
sys_country,
os_type,
os_version,
media_source,
uri,
referer,
test_info,
geo_city,
geo_country,
device_model,
gender,
account_class,
device_manufacturer,
ip,
list_type,
element_id,
element_name,
element_type,
element_position,
view_type,
enter_ts,
leave_ts,
case when element_id rlike '^\\d+$' then element_id else rand(1000)*10000 end ele_id
from dwd.dwd_vova_log_analysis_arc where pt='$pt' and hour='20'
) aa
left join dim.dim_vova_buyers b on aa.buyer_id = b.buyer_id and aa.datasource = b.datasource
left join dim.dim_vova_devices d on aa.device_id = d.device_id and aa.datasource = d.datasource
left join dim.dim_vova_goods g on cast (aa.ele_id as bigint) = cast (g.virtual_goods_id as bigint)
"
#spark-sql \
#--executor-memory 8G \
#--conf "spark.dynamicAllocation.maxExecutors=150" \
#--conf "spark.app.name=dwd_vova_log_analysis_d" \
hive -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi