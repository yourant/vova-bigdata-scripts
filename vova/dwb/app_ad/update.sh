#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
#ods_yx_cy.ods_yx_ads_ga_channel_daily_gmv_flat_report
#ods_yx_yxlc.ods_yx_temp_device_order_date_cohort
#ods_yx_yxl.ods_yx_temp_device_order_date_cohort
#utc time -> cn

sql="

drop table if exists tmp.tmp_dwb_vova_ad_cost;
create table tmp.tmp_dwb_vova_ad_cost as
select
/*+ REPARTITION(1) */
nvl(date, 'all') AS event_date,
nvl(if(ads_site_code = 'AC', 'airyclub', 'vova'), 'all') AS datasource,
nvl(ga_channel, 'all') AS ga_channel,
nvl(r.region_code, 'all') AS region_code,
nvl(platform, 'all') AS platform,
sum(tot_cost) AS tot_cost,
sum(tot_gmv) AS tot_gmv
from
(
select
date,
ads_site_code,
ga_channel,
country,
platform,
sum(cost) AS tot_cost,
0 AS tot_gmv
from
(
select
date,
ads_site_code,
ga_channel,
country,
case
when ga_channel like '%api_ios%' then 'ios'
when ga_channel like '%api_android%' then 'android'
else 'others' end as platform,
cost
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
where ads_site_code IN ('VO', 'AC')
and date >= '2020-01-01'
) cost_data
group by date, ads_site_code, ga_channel, country, platform

UNION ALL

select
date,
ads_site_code,
ga_channel,
country,
platform,
0 AS tot_cost,
sum(gmv) AS tot_gmv
from
(
select
date,
ads_site_code,
ga_channel,
country,
case
when ga_channel like '%api_ios%' then 'ios'
when ga_channel like '%api_android%' then 'android'
else 'others' end as platform,
gmv
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_gmv_flat_report
where ads_site_code IN ('VO', 'AC')
and date >= '2020-01-01'
) gmv_data
group by date, ads_site_code, ga_channel, country, platform
) fin
inner join (
select region_name, region_code
from ods_vova_vts.ods_vova_region
WHERE parent_id = 0 and region_display = 1
) r on r.region_name = fin.country
group by cube(date, if(ads_site_code = 'AC', 'airyclub', 'vova'), ga_channel, r.region_code, platform)
;

drop table if exists tmp.tmp_dwb_vova_ad_est;
create table tmp.tmp_dwb_vova_ad_est as
select
/*+ REPARTITION(1) */
nvl(install_date, 'all') AS event_date,
nvl(project_name, 'all') AS datasource,
nvl(ga_channel, 'all') AS ga_channel,
nvl(country, 'all') AS region_code,
nvl(platform, 'all') AS platform,
sum(est_gmv_7d) AS est_gmv_7d
from
(
select
install_date,
project_name,
ga_channel,
country,
case
when ga_channel like '%api_ios%' then 'ios'
when ga_channel like '%api_android%' then 'android'
else 'others' end as platform,
est_gmv_7d
from
ods_yx_yxl.ods_yx_temp_device_order_date_cohort
where install_date >= '2020-01-01'
and project_name = 'vova'

UNION ALL

select
install_date,
project_name,
ga_channel,
country,
case
when ga_channel like '%api_ios%' then 'ios'
when ga_channel like '%api_android%' then 'android'
else 'others' end as platform,
est_gmv_7d
from
ods_yx_yxlc.ods_yx_temp_device_order_date_cohort
where install_date >= '2020-01-01'
and project_name = 'airyclub'
) est
group by cube(install_date, project_name, ga_channel, country, platform)
;



drop table if exists tmp.tmp_dwb_vova_ad_install;
create table tmp.tmp_dwb_vova_ad_install as
select
/*+ REPARTITION(1) */
nvl(dd.datasource, 'all') AS datasource,
nvl(dd.ga_channel, 'all') AS ga_channel,
nvl(dd.platform, 'all') AS platform,
nvl(nvl(dd.region_code, 'NA') , 'all') AS region_code,
nvl(date_format(from_utc_timestamp(dd.activate_time,'GMT+8'),'yyyy-MM-dd'), 'all') AS activate_date,
count(DISTINCT device_id) AS activate_dau
from
(
select
dd.datasource,
CASE
     WHEN campaign REGEXP ' app_dr| app dr' THEN 'app_dr_api'
     WHEN lower(campaign) REGEXP ' #app' THEN 'mweb'
     WHEN lower(child_channel) REGEXP '2019_mweb' THEN 'mweb'
     WHEN lower(child_channel) = 'aura_int' THEN 'aura_api_android'
     WHEN lower(child_channel) = 'bytedanceglobal_int' AND platform = 'android' THEN 'bytedance_api_android'
     WHEN lower(child_channel) = 'bytedanceglobal_int' AND platform = 'ios' THEN 'bytedance_api_ios'
     WHEN lower(child_channel) = 'outbrain_int' AND platform = 'android' THEN 'outbrain_api_android'
     WHEN lower(child_channel) = 'outbrain_int' AND platform = 'ios' THEN 'outbrain_api_ios'
     WHEN lower(child_channel) = 'twitter' AND platform = 'android' THEN 'twitter_api_android'
     WHEN lower(child_channel) = 'twitter' AND platform = 'ios' THEN 'twitter_api_ios'
     WHEN lower(child_channel) = 'liftoff_int' AND platform = 'android' THEN 'liftoff_api_android'
     WHEN lower(child_channel) = 'liftoff_int' AND platform = 'ios' THEN 'liftoff_api_ios'
     WHEN lower(child_channel) = 'tapjoy_int' AND platform = 'android' THEN 'tapjoy_api_android'
     WHEN lower(child_channel) = 'tapjoy_int' AND platform = 'ios' THEN 'tapjoy_api_ios'
     WHEN lower(child_channel) = 'admitad1_int' AND platform = 'android' THEN 'admitad_api_android'
     WHEN lower(child_channel) = 'admitad1_int' AND platform = 'ios' THEN 'admitad_api_ios'
     WHEN lower(child_channel) = 'revcontent_int' AND platform = 'android' THEN 'revcontent_api_android'
     WHEN lower(child_channel) = 'revcontent_int' AND platform = 'ios' THEN 'revcontent_api_ios'
     WHEN lower(child_channel) = 'applovin_int' AND platform = 'android' THEN 'applovin_api_android'
     WHEN lower(child_channel) = 'applovin_int' AND platform = 'ios' THEN 'applovin_api_ios'
     WHEN lower(child_channel) = 'taboola_int' AND platform = 'android' THEN 'taboola_api_android'
     WHEN lower(child_channel) = 'taboola_int' AND platform = 'ios' THEN 'taboola_api_ios'
     WHEN lower(child_channel) = 'snapchat_int' AND platform = 'android' THEN 'snapchat_api_android'
     WHEN lower(child_channel) = 'snapchat_int' AND platform = 'ios' THEN 'snapchat_api_ios'
     WHEN lower(child_channel) = 'pinterest_int' AND platform = 'android' THEN 'pinterest_api_android'
     WHEN lower(child_channel) = 'pinterest_int' AND platform = 'ios' THEN 'pinterest_api_ios'
     WHEN lower(child_channel) = 'apple search ads' THEN 'apple_search_api'
     WHEN lower(child_channel) REGEXP 'facebook ads' AND platform = 'android' THEN 'facebook_api_android'
     WHEN lower(child_channel) REGEXP 'facebook ads' AND platform = 'ios' THEN 'facebook_api_ios'
     WHEN lower(child_channel) REGEXP 'googleadwords_int' AND platform = 'android' THEN 'google_api_android'
     WHEN lower(child_channel) REGEXP 'googleadwords_int' AND platform = 'ios' THEN 'google_api_ios'
     ELSE 'api_other'
END AS ga_channel,
dd.platform,
dd.region_code,
dd.activate_time,
dd.device_id
from
dim.dim_vova_devices dd
WHERE dd.datasource in ('vova', 'airyclub')
and date(dd.activate_time) >= '2020-01-01'
) dd
group by cube (date_format(from_utc_timestamp(dd.activate_time,'GMT+8'),'yyyy-MM-dd'), dd.datasource, dd.ga_channel, dd.platform, nvl(dd.region_code, 'NA'))
HAVING activate_date != 'all'
;


drop table if exists tmp.tmp_dwb_vova_ad_gmv_base;
create table tmp.tmp_dwb_vova_ad_gmv_base as
select
/*+ REPARTITION(1) */
nvl(dd.datasource, 'all') AS datasource,
nvl(dd.ga_channel, 'all') AS ga_channel,
nvl(dd.platform, 'all') AS platform,
nvl(nvl(dd.region_code, 'NA') , 'all') AS region_code,
nvl(date_format(from_utc_timestamp(dd.activate_time,'GMT+8'),'yyyy-MM-dd'), 'all') AS activate_date,
nvl(date_format(from_utc_timestamp(dd.pay_time,'GMT+8'),'yyyy-MM-dd'), 'all') AS pay_date,
sum(gmv) AS gmv
from
(
select
fp.datasource,
CASE
     WHEN campaign REGEXP ' app_dr| app dr' THEN 'app_dr_api'
     WHEN lower(campaign) REGEXP ' #app' THEN 'mweb'
     WHEN lower(child_channel) REGEXP '2019_mweb' THEN 'mweb'
     WHEN lower(child_channel) = 'aura_int' THEN 'aura_api_android'
     WHEN lower(child_channel) = 'bytedanceglobal_int' AND dd.platform = 'android' THEN 'bytedance_api_android'
     WHEN lower(child_channel) = 'bytedanceglobal_int' AND dd.platform = 'ios' THEN 'bytedance_api_ios'
     WHEN lower(child_channel) = 'outbrain_int' AND dd.platform = 'android' THEN 'outbrain_api_android'
     WHEN lower(child_channel) = 'outbrain_int' AND dd.platform = 'ios' THEN 'outbrain_api_ios'
     WHEN lower(child_channel) = 'twitter' AND dd.platform = 'android' THEN 'twitter_api_android'
     WHEN lower(child_channel) = 'twitter' AND dd.platform = 'ios' THEN 'twitter_api_ios'
     WHEN lower(child_channel) = 'liftoff_int' AND dd.platform = 'android' THEN 'liftoff_api_android'
     WHEN lower(child_channel) = 'liftoff_int' AND dd.platform = 'ios' THEN 'liftoff_api_ios'
     WHEN lower(child_channel) = 'tapjoy_int' AND dd.platform = 'android' THEN 'tapjoy_api_android'
     WHEN lower(child_channel) = 'tapjoy_int' AND dd.platform = 'ios' THEN 'tapjoy_api_ios'
     WHEN lower(child_channel) = 'admitad1_int' AND dd.platform = 'android' THEN 'admitad_api_android'
     WHEN lower(child_channel) = 'admitad1_int' AND dd.platform = 'ios' THEN 'admitad_api_ios'
     WHEN lower(child_channel) = 'revcontent_int' AND dd.platform = 'android' THEN 'revcontent_api_android'
     WHEN lower(child_channel) = 'revcontent_int' AND dd.platform = 'ios' THEN 'revcontent_api_ios'
     WHEN lower(child_channel) = 'applovin_int' AND dd.platform = 'android' THEN 'applovin_api_android'
     WHEN lower(child_channel) = 'applovin_int' AND dd.platform = 'ios' THEN 'applovin_api_ios'
     WHEN lower(child_channel) = 'taboola_int' AND dd.platform = 'android' THEN 'taboola_api_android'
     WHEN lower(child_channel) = 'taboola_int' AND dd.platform = 'ios' THEN 'taboola_api_ios'
     WHEN lower(child_channel) = 'snapchat_int' AND dd.platform = 'android' THEN 'snapchat_api_android'
     WHEN lower(child_channel) = 'snapchat_int' AND dd.platform = 'ios' THEN 'snapchat_api_ios'
     WHEN lower(child_channel) = 'pinterest_int' AND dd.platform = 'android' THEN 'pinterest_api_android'
     WHEN lower(child_channel) = 'pinterest_int' AND dd.platform = 'ios' THEN 'pinterest_api_ios'
     WHEN lower(child_channel) = 'apple search ads' THEN 'apple_search_api'
     WHEN lower(child_channel) REGEXP 'facebook ads' AND dd.platform = 'android' THEN 'facebook_api_android'
     WHEN lower(child_channel) REGEXP 'facebook ads' AND dd.platform = 'ios' THEN 'facebook_api_ios'
     WHEN lower(child_channel) REGEXP 'googleadwords_int' AND dd.platform = 'android' THEN 'google_api_android'
     WHEN lower(child_channel) REGEXP 'googleadwords_int' AND dd.platform = 'ios' THEN 'google_api_ios'
     ELSE 'api_other'
END AS ga_channel,
case
when fp.platform = 'ios' then 'ios'
when fp.platform = 'android' then 'android'
else 'others' end AS platform,
fp.region_code,
fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv,
dd.activate_time,
fp.pay_time
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id AND fp.datasource = dd.datasource
WHERE fp.datasource in ('vova', 'airyclub')
and fp.from_domain like '%api%'
and date(dd.activate_time) >= '2020-01-01'
and date(dd.activate_time) <= date(fp.pay_time)
and date(fp.pay_time) >= '2020-01-01'
) dd
group by cube (date_format(from_utc_timestamp(dd.activate_time,'GMT+8'),'yyyy-MM-dd'), date_format(from_utc_timestamp(dd.pay_time,'GMT+8'),'yyyy-MM-dd'), dd.datasource, dd.ga_channel, dd.platform, nvl(dd.region_code, 'NA'))
HAVING activate_date != 'all' AND pay_date != 'all'
;

drop table if exists tmp.tmp_dwb_vova_ad_gmv_30_180d;
create table tmp.tmp_dwb_vova_ad_gmv_30_180d as
select
/*+ REPARTITION(1) */
d2.event_date,
d.datasource,
d.ga_channel,
d.platform,
d.region_code,
sum(gmv) AS gmv
from
(
select
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
dd.activate_date,
sum(gmv) AS gmv
from
tmp.tmp_dwb_vova_ad_gmv_base dd
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 180
group by
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
dd.activate_date
) d
inner join (
select distinct date(pay_time) AS event_date
from dwd.dwd_vova_fact_pay
where date_sub(date(pay_time), 210) >= '2020-01-01'
)d2 on date_sub(d2.event_date, 210) <= d.activate_date and date_sub(d2.event_date, 180) > d.activate_date
group by
d2.event_date,
d.datasource,
d.ga_channel,
d.platform,
d.region_code
;

drop table if exists tmp.tmp_dwb_vova_ad_gmv_30_7d;
create table tmp.tmp_dwb_vova_ad_gmv_30_7d as
select
/*+ REPARTITION(1) */
d2.event_date,
d.datasource,
d.ga_channel,
d.platform,
d.region_code,
sum(gmv) AS gmv
from
(
select
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
dd.activate_date,
sum(gmv) AS gmv
from
tmp.tmp_dwb_vova_ad_gmv_base dd
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 7
group by
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
dd.activate_date
) d
inner join (
select distinct date(pay_time) AS event_date from dwd.dwd_vova_fact_pay
where date_sub(date(pay_time), 210) >= '2020-01-01'
)d2 on date_sub(d2.event_date, 210) <= d.activate_date and date_sub(d2.event_date, 180) > d.activate_date
group by
d2.event_date,
d.datasource,
d.ga_channel,
d.platform,
d.region_code
;

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_ad_cost PARTITION (pt)
select
/*+ REPARTITION(1) */
cost.event_date,
cost.datasource,
cost.region_code,
cost.ga_channel,
cost.platform,
nvl(install.activate_dau, 0) AS activate_dau,
nvl(cost.tot_gmv, 0) AS tot_gmv,
nvl(cost.tot_cost, 0) AS tot_cost,
nvl(cost2.tot_gmv, 0) AS tot_region_gmv,
nvl(cost2.tot_cost, 0) AS tot_region_cost,
nvl(gmv7.gmv, 0) AS gmv_7d,
nvl(gmv180.gmv, 0) AS gmv_180d,
nvl(est.est_gmv_7d, 0) AS est_gmv_7d,
cost.event_date AS pt
from
tmp.tmp_dwb_vova_ad_cost cost
left join tmp.tmp_dwb_vova_ad_install install
on cost.datasource = install.datasource
and cost.event_date = install.activate_date
and cost.region_code = install.region_code
and cost.ga_channel = install.ga_channel
and cost.platform = install.platform

left join tmp.tmp_dwb_vova_ad_gmv_30_180d gmv180
on cost.datasource = gmv180.datasource
and cost.event_date = gmv180.event_date
and cost.region_code = gmv180.region_code
and cost.ga_channel = gmv180.ga_channel
and cost.platform = gmv180.platform

left join tmp.tmp_dwb_vova_ad_gmv_30_7d gmv7
on cost.datasource = gmv7.datasource
and cost.event_date = gmv7.event_date
and cost.region_code = gmv7.region_code
and cost.ga_channel = gmv7.ga_channel
and cost.platform = gmv7.platform

left join tmp.tmp_dwb_vova_ad_est est
on cost.datasource = est.datasource
and cost.event_date = est.event_date
and cost.region_code = est.region_code
and cost.ga_channel = est.ga_channel
and cost.platform = est.platform

left join tmp.tmp_dwb_vova_ad_cost cost2
on cost.datasource = cost2.datasource
and cost.event_date = cost2.event_date
and cost.region_code = cost2.region_code
and cost2.platform = 'all'
and cost2.ga_channel = 'all'
WHERE cost.event_date >= '2021-02-01'
AND cost.event_date != 'all'

UNION ALL

select
/*+ REPARTITION(1) */
base.event_date,
base.datasource,
base.region_code,
'all-ads' AS ga_channel,
base.platform,
base.activate_dau,
base.tot_gmv,
base.tot_cost,
t2.tot_region_gmv,
t2.tot_region_cost,
base.gmv_7d,
base.gmv_180d,
base.est_gmv_7d,
base.event_date AS pt
from
(
select
event_date,
datasource,
region_code,
platform,
sum(activate_dau) AS activate_dau,
sum(tot_gmv) AS tot_gmv,
sum(tot_cost) AS tot_cost,
sum(gmv_7d) AS gmv_7d,
sum(gmv_180d) AS gmv_180d,
sum(est_gmv_7d) AS est_gmv_7d,
event_date AS pt
from
(
select
cost.event_date,
cost.datasource,
cost.region_code,
cost.ga_channel,
cost.platform,
nvl(install.activate_dau, 0) AS activate_dau,
nvl(cost.tot_gmv, 0) AS tot_gmv,
nvl(cost.tot_cost, 0) AS tot_cost,
nvl(gmv7.gmv, 0) AS gmv_7d,
nvl(gmv180.gmv, 0) AS gmv_180d,
nvl(est.est_gmv_7d, 0) AS est_gmv_7d,
cost.event_date AS pt
from
tmp.tmp_dwb_vova_ad_cost cost
inner join
(
select
distinct ga_channel
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
where cost> 0
) cost_ga_channel ON cost_ga_channel.ga_channel = cost.ga_channel
left join tmp.tmp_dwb_vova_ad_install install
on cost.datasource = install.datasource
and cost.event_date = install.activate_date
and cost.region_code = install.region_code
and cost.ga_channel = install.ga_channel
and cost.platform = install.platform

left join tmp.tmp_dwb_vova_ad_gmv_30_180d gmv180
on cost.datasource = gmv180.datasource
and cost.event_date = gmv180.event_date
and cost.region_code = gmv180.region_code
and cost.ga_channel = gmv180.ga_channel
and cost.platform = gmv180.platform

left join tmp.tmp_dwb_vova_ad_gmv_30_7d gmv7
on cost.datasource = gmv7.datasource
and cost.event_date = gmv7.event_date
and cost.region_code = gmv7.region_code
and cost.ga_channel = gmv7.ga_channel
and cost.platform = gmv7.platform

left join tmp.tmp_dwb_vova_ad_est est
on cost.datasource = est.datasource
and cost.event_date = est.event_date
and cost.region_code = est.region_code
and cost.ga_channel = est.ga_channel
and cost.platform = est.platform
) fin
group by
event_date,
datasource,
region_code,
platform
) base
left join
(
select
cost2.event_date,
cost2.datasource,
cost2.region_code,
sum(tot_gmv) AS tot_region_gmv,
sum(tot_cost) AS tot_region_cost
from
tmp.tmp_dwb_vova_ad_cost cost2
inner join
(
select
distinct ga_channel
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
where cost> 0
) cost_ga_channel ON cost_ga_channel.ga_channel = cost2.ga_channel
WHERE cost2.event_date >= '2021-02-01'
AND cost2.event_date != 'all'
AND cost2.platform = 'all'
group by
cost2.event_date,
cost2.datasource,
cost2.region_code
) t2 on base.event_date = t2.event_date
AND base.datasource = t2.datasource
AND base.region_code = t2.region_code
WHERE base.event_date >= '2021-02-01'
AND base.event_date != 'all'
;

insert overwrite table dwb.dwb_vova_ad_gmv PARTITION (pt)
select
/*+ REPARTITION(1) */
dd.datasource,
dd.region_code,
dd.ga_channel,
dd.platform,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 1,gmv,0)) AS gmv_1d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 7,gmv,0)) AS gmv_7d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 30,gmv,0)) AS gmv_30d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 90,gmv,0)) AS gmv_90d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 180,gmv,0)) AS gmv_180d,
trunc(dd.activate_date, 'MM') AS pt
from
tmp.tmp_dwb_vova_ad_gmv_base dd
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 180
group by
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
trunc(dd.activate_date, 'MM')

UNION ALL

select
/*+ REPARTITION(1) */
dd.datasource,
dd.region_code,
'all-ads' AS ga_channel,
dd.platform,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 1,gmv,0)) AS gmv_1d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 7,gmv,0)) AS gmv_7d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 30,gmv,0)) AS gmv_30d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 90,gmv,0)) AS gmv_90d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 180,gmv,0)) AS gmv_180d,
trunc(dd.activate_date, 'MM') AS pt
from
tmp.tmp_dwb_vova_ad_gmv_base dd
inner join
(
select
distinct ga_channel
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
where cost> 0
) cost_ga_channel ON cost_ga_channel.ga_channel = dd.ga_channel
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 180
group by
dd.datasource,
dd.platform,
dd.region_code,
trunc(dd.activate_date, 'MM')
;


"



#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_ad" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

