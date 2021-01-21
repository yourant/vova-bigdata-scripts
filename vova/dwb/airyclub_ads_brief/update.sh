#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_airyclub_daily_ratio_req4999_chenkai_${cur_date}"

###逻辑sql
sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_airyclub_daily_ratio PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
  t1.total_gmv total_gmv,
  t2.total_cost total_cost,
  if(t1.total_gmv=0 or round(t2.total_cost/t1.total_gmv, 4)>5, 5, round(t2.total_cost/t1.total_gmv, 4)) total_ratio,
  t1.fb_ios_gmv fb_ios_gmv,
  t2.fb_ios_cost fb_ios_cost,
  if(t1.fb_ios_gmv=0 or round(t2.fb_ios_cost/t1.fb_ios_gmv, 4)>5, 5, round(t2.fb_ios_cost/t1.fb_ios_gmv, 4)) fb_ios_ratio,
  t1.fb_andr_gmv fb_andr_gmv,
  t2.fb_andr_cost fb_andr_cost,
  if(t1.fb_andr_gmv=0 or round(t2.fb_andr_cost/t1.fb_andr_gmv, 4)>5, 5, round(t2.fb_andr_cost/t1.fb_andr_gmv, 4)) fb_andr_ratio,
  t1.gc_ios_gmv gc_ios_gmv,
  t2.gc_ios_cost gc_ios_cost,
  if(t1.gc_ios_gmv=0 or round(t2.gc_ios_cost/t1.gc_ios_gmv, 4)>5, 5, round(t2.gc_ios_cost/t1.gc_ios_gmv, 4)) gc_ios_ratio,
  t1.gc_andr_gmv gc_andr_gmv,
  t2.gc_andr_cost gc_andr_cost,
  if(t1.gc_andr_gmv=0 or round(t2.gc_andr_cost/t1.gc_andr_gmv, 4)>5, 5, round(t2.gc_andr_cost/t1.gc_andr_gmv, 4)) gc_andr_ratio
from
(
  select
    date pt,
    sum(gmv) total_gmv,
    sum(if(ga_channel = 'facebook_api_ios', gmv, 0)) fb_ios_gmv,
    sum(if(ga_channel = 'facebook_api_android', gmv, 0)) fb_andr_gmv,
    sum(if(ga_channel = 'google_api_ios', gmv, 0)) gc_ios_gmv,
    sum(if(ga_channel = 'google_api_android', gmv, 0)) gc_andr_gmv
  from
    ods_yx_cy.ods_yx_ads_ga_channel_daily_gmv_flat_report
  where date = '${cur_date}' and ads_site_code = 'AC'
  group by date
) t1
left join
(
  select
    date pt,
    sum(cost) total_cost,
    sum(if(ga_channel = 'facebook_api_ios', cost, 0)) fb_ios_cost,
    sum(if(ga_channel = 'facebook_api_android', cost, 0)) fb_andr_cost,
    sum(if(ga_channel = 'google_api_ios', cost, 0)) gc_ios_cost,
    sum(if(ga_channel = 'google_api_android', cost, 0)) gc_andr_cost
  from
    ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
  where date = '${cur_date}' and ads_site_code = 'AC'
  group by date
) t2
on t1.pt = t2.pt
;

INSERT OVERWRITE TABLE dwb.dwb_vova_fb_ads_ctr PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
t3.campaignCountry campaign_contry,
t3.campaignChannel campaign_device,
t1.ad_name ad_name,
t2.carousel_name carousel_card,
t1.impressions impressions,
t1.clicks clicks,
t1.ctr ctr,
t2.link_clicks link_clicks
from
ods_yx_cy.ods_yx_adwords_ad_performance_daily_report t1
left join
ods_yx_cy.ods_yx_adwords_ad_carousel t2
on t1.ad_id = t2.ad_id and t1.start_date = t2.start_date
inner join
ods_yx_cy.ods_yx_campaign_mapping t3
on t1.campaign_id = t3.campaignid and t1.account_name=t3.AccountDescriptiveName
where t2.start_date='${cur_date}' and t3.adssitecode = 'AC'
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.app.name=${job_name}" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`