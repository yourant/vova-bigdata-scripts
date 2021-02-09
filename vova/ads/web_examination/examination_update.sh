#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date +%Y-%m-%d`
fi
cur_date2=`date -d "+1 day ${cur_date}" +%Y-%m-%d`
echo "$cur_date"
echo "$cur_date2"

sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ads.ads_vova_web_goods_examination_collector_data PARTITION (pt)
select
/*+ REPARTITION(1) */
goods_id,
original_name,
datasource,
domain_userid,
collector_ts,
pt
from
(
select
dg.goods_id,
'goods_impression' as original_name,
log.datasource,
log.domain_userid,
log.collector_ts,
log.pt
FROM dwd.dwd_vova_log_impressions_arc log
         INNER JOIN dim.dim_vova_goods dg ON log.element_id = dg.virtual_goods_id
         INNER JOIN ads.ads_vova_web_examination_poll test_poll on test_poll.goods_id = dg.goods_id
WHERE log.datasource = 'vova'
  AND log.platform IN ('web', 'pc')
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
  AND log.event_type = 'goods'

UNION ALL

select
dg.goods_id,
'goods_impression' as original_name,
log.datasource,
log.domain_userid,
log.collector_ts,
log.pt
FROM dwd.dwd_vova_log_goods_impression_arc log
         INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
         INNER JOIN ads.ads_vova_web_examination_poll test_poll on test_poll.goods_id = dg.goods_id
WHERE log.datasource = 'vova'
  AND log.platform IN ('web', 'pc')
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'

UNION ALL

select
dg.goods_id,
'goods_click' as original_name,
log.datasource,
log.domain_userid,
log.collector_ts,
log.pt
FROM dwd.dwd_vova_log_click_arc log
         INNER JOIN dim.dim_vova_goods dg ON log.element_id = dg.virtual_goods_id
         INNER JOIN ads.ads_vova_web_examination_poll test_poll on test_poll.goods_id = dg.goods_id
WHERE log.datasource = 'vova'
  AND log.platform IN ('web', 'pc')
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
  AND log.event_type = 'goods'

UNION ALL

select
dg.goods_id,
'goods_click' as original_name,
log.datasource,
log.domain_userid,
log.collector_ts,
log.pt
FROM dwd.dwd_vova_log_goods_click_arc log
         INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
         INNER JOIN ads.ads_vova_web_examination_poll test_poll on test_poll.goods_id = dg.goods_id
WHERE log.datasource = 'vova'
  AND log.platform IN ('web', 'pc')
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'

UNION ALL

select
dg.goods_id,
'pdAddToCartSuccess' as original_name,
log.datasource,
log.domain_userid,
log.collector_ts,
log.pt
FROM dwd.dwd_vova_log_data_arc log
         INNER JOIN dim.dim_vova_goods dg ON log.element_id = dg.virtual_goods_id
         INNER JOIN ads.ads_vova_web_examination_poll test_poll on test_poll.goods_id = dg.goods_id
WHERE log.datasource = 'vova'
  AND log.platform IN ('web', 'pc')
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
  AND log.element_name = 'pdAddToCartSuccess'
) fin
;

INSERT OVERWRITE TABLE ads.ads_vova_web_goods_examination_behave
select
/*+ REPARTITION(1) */
'vova' as datasource,
goods_id,
sum(impressions) as impressions,
sum(impressions_uv) as impressions_uv,
sum(clicks) as clicks,
sum(clicks_uv) as clicks_uv,
sum(add_cart_cnt) as add_cart_cnt,
sum(gmv) as gmv,
sum(sales_order) as sales_order,
round(nvl(sum(clicks) / sum(impressions), 0), 6) as ctr,
round(nvl(sum(gmv) / sum(clicks_uv) * sum(clicks) / sum(impressions) * 10000, 0), 6) as gcr,
round(nvl(sum(gmv) / sum(impressions) * 10000, 0), 6) as gmv_cr,
round(nvl((sum(clicks) + sum(add_cart_cnt) * 3 + sum(sales_order) * 5 ) * 100 / sum(impressions), 0), 6) as goods_score
from
(
select
poll.goods_id,
count(*) as impressions,
count(distinct tcd.domain_userid) as impressions_uv,
0 AS clicks,
0 AS clicks_uv,
0 AS add_cart_cnt,
0 as gmv,
0 as sales_order
from
ads.ads_vova_web_examination_poll poll
inner join ads.ads_vova_web_goods_examination_collector_data tcd on tcd.goods_id = poll.goods_id
where tcd.collector_ts >= poll.add_test_time
and tcd.original_name = 'goods_impression'
and tcd.datasource = 'vova'
group by poll.goods_id

UNION ALL

select
poll.goods_id,
0 AS impressions,
0 AS impressions_uv,
count(*) as clicks,
count(distinct tcd.domain_userid) as clicks_uv,
0 AS add_cart_cnt,
0 as gmv,
0 as sales_order
from
ads.ads_vova_web_examination_poll poll
inner join ads.ads_vova_web_goods_examination_collector_data tcd on tcd.goods_id = poll.goods_id
where tcd.collector_ts >= poll.add_test_time
and tcd.original_name = 'goods_click'
and tcd.datasource = 'vova'
group by poll.goods_id

UNION ALL

select
poll.goods_id,
0 AS impressions,
0 AS impressions_uv,
0 AS clicks,
0 AS clicks_uv,
count(*) as add_cart_cnt,
0 as gmv,
0 as sales_order
from
ads.ads_vova_web_examination_poll poll
inner join ads.ads_vova_web_goods_examination_collector_data tcd on tcd.goods_id = poll.goods_id
where tcd.collector_ts >= poll.add_test_time
and tcd.original_name = 'pdAddToCartSuccess'
and tcd.datasource = 'vova'
group by poll.goods_id

UNION ALL

SELECT
og.goods_id,
0 AS impressions,
0 AS impressions_uv,
0 AS clicks,
0 AS clicks_uv,
0 AS add_cart_cnt,
sum(og.goods_number * og.shop_price + og.shipping_fee) as gmv,
COUNT(DISTINCT oi.order_id) AS sales_order
FROM ods_vova_vts.ods_vova_order_info_h oi
         INNER JOIN ods_vova_vts.ods_vova_order_goods_h og ON oi.order_id = og.order_id
         inner join ads.ads_vova_web_examination_poll poll on poll.goods_id = og.goods_id
WHERE oi.pay_status >= 1
  AND oi.from_domain NOT LIKE '%api%'
  AND oi.project_name = 'vova'
  AND oi.pay_time >= poll.add_test_time
  AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  AND oi.parent_order_id = 0
group by og.goods_id
) fin
group by fin.goods_id
;

INSERT OVERWRITE TABLE tmp.tmp_ads_vova_web_goods_examination_pre
SELECT
/*+ REPARTITION(1) */
datasource,
goods_id,
impressions,
ctr,
gcr,
gmv_cr,
goods_score,
case
when impressions >= 5000 and ctr >=0.02 and goods_score >= 4
then 4
when impressions > 2000 and ctr >=0.02
then 3
when impressions > 500
then 2
else 1 end as test_goods_status,
case
when impressions >= 5000 and ctr >=0.02 and goods_score >= 4 and gcr >= 60
then 6
when impressions >= 5000 and ctr >=0.02 and goods_score >= 4 and gcr < 60
then 7
when impressions > 2000 and ctr >=0.02 and goods_score >= 4
then 4
when impressions > 2000 and ctr >=0.02 and goods_score < 4
then 5
when impressions > 500 and ctr >=0.02
then 2
when impressions > 500 and ctr <0.02
then 3
else 1 end as test_goods_result_status,
current_timestamp() AS status_change_time,
add_test_time
FROM
(
select
'vova' as datasource,
test_poll.goods_id,
nvl(b1.impressions, 0) as impressions,
nvl(b1.ctr, 0) as ctr,
nvl(b1.gcr, 0) as gcr,
nvl(b1.gmv_cr, 0) as gmv_cr,
nvl(b1.goods_score, 0) as goods_score,
test_poll.add_test_time
from
ads.ads_vova_web_examination_poll test_poll
LEFT JOIN ads.ads_vova_web_goods_examination_behave b1 on test_poll.goods_id = b1.goods_id
) t1
;


INSERT OVERWRITE TABLE tmp.tmp_ads_vova_web_goods_examination_diff
SELECT
/*+ REPARTITION(1) */
fin.datasource,
fin.goods_id,
fin.impressions,
fin.ctr,
fin.gcr,
fin.gmv_cr,
fin.goods_score,
fin.test_goods_status,
fin.test_goods_result_status,
fin.status_change_time,
fin.add_test_time
from
ads.ads_vova_web_goods_examination_summary fin
inner join tmp.tmp_ads_vova_web_goods_examination_pre pre on pre.goods_id = fin.goods_id
where fin.test_goods_result_status in (3,5,6,7) and fin.add_test_time = pre.add_test_time
;

INSERT OVERWRITE TABLE tmp.tmp_ads_vova_web_goods_examination_not_change
SELECT
/*+ REPARTITION(1) */
'vova' as datasource,
fin.goods_id,
fin.status_change_time
from
tmp.tmp_ads_vova_web_goods_examination_pre pre
inner join ads.ads_vova_web_goods_examination_summary fin on pre.goods_id = fin.goods_id
where fin.test_goods_result_status not in (3,5,6,7) and pre.test_goods_result_status = fin.test_goods_result_status
;


INSERT OVERWRITE TABLE ads.ads_vova_web_goods_examination_summary
select
/*+ REPARTITION(1) */
pre.datasource,
pre.goods_id,
pre.impressions,
pre.ctr,
pre.gcr,
pre.gmv_cr,
pre.goods_score,
pre.test_goods_status,
pre.test_goods_result_status,
if(dc.goods_id is not null, dc.status_change_time, pre.status_change_time) as status_change_time,
pre.add_test_time
from
tmp.tmp_ads_vova_web_goods_examination_pre pre
left join tmp.tmp_ads_vova_web_goods_examination_diff d1 on d1.goods_id = pre.goods_id
left join tmp.tmp_ads_vova_web_goods_examination_not_change dc on dc.goods_id = pre.goods_id
where d1.goods_id is null

UNION ALL

select
d1.datasource,
d1.goods_id,
d1.impressions,
d1.ctr,
d1.gcr,
d1.gmv_cr,
d1.goods_score,
d1.test_goods_status,
d1.test_goods_result_status,
d1.status_change_time,
d1.add_test_time
from
tmp.tmp_ads_vova_web_goods_examination_diff d1
;

INSERT OVERWRITE TABLE ads.ads_vova_web_goods_examination_summary_history_export PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
ges.datasource,
ges.goods_id,
nvl(dg.cat_id, 0) as cat_id,
nvl(dg.first_cat_id, 0) as first_cat_id,
nvl(dg.second_cat_id, 0) as second_cat_id,
ges.impressions,
ges.ctr,
ges.gcr,
ges.gmv_cr,
ges.goods_score,
nvl(ep.gcr, 0) AS gcr_1w,
nvl(ep.gmv_cr, 0) AS gmv_cr_1w,
nvl(ep.impressions, 0) AS impressions_1w,
ges.test_goods_status,
case
when
ges.test_goods_status = 1
then 'first_level_testing'
when
ges.test_goods_status = 2
then 'first_level_finished'
when
ges.test_goods_status = 3
then 'second_level_finished'
when
ges.test_goods_status = 4
then 'third_level_finished'
else 'unknown' end as test_goods_status_comment,
ges.test_goods_result_status,
case
when
ges.test_goods_result_status = 1
then 'pending'
when
ges.test_goods_result_status = 2
then 'first_level_success'
when
ges.test_goods_result_status = 3
then 'first_level_failure'
when
ges.test_goods_result_status = 4
then 'second_level_success'
when
ges.test_goods_result_status = 5
then 'second_level_failure'
when
ges.test_goods_result_status = 6
then 'third_level_success'
when
ges.test_goods_result_status = 7
then 'third_level_failure'
else 'unknown' end as test_goods_result_comment,
poll.add_test_time,
ges.status_change_time
from
ads.ads_vova_web_goods_examination_summary ges
LEFT JOIN ads.ads_vova_web_examination_poll poll on poll.goods_id = ges.goods_id
left join dim.dim_vova_goods dg on dg.goods_id = ges.goods_id
LEFT JOIN (
select
ep1.goods_id,
ep1.impressions,
ep1.gcr,
ep1.gmv_cr
from
(
select
max(pt) as max_pt
from ads.ads_vova_web_examination_1w_pre ep2
) ep2
inner join ads.ads_vova_web_examination_1w_pre ep1 on ep1.pt = ep2.max_pt
) ep on ep.goods_id = ges.goods_id
;

INSERT OVERWRITE TABLE ads.ads_vova_web_goods_examination_summary_history_display
SELECT
/*+ REPARTITION(1) */
datasource,
goods_id,
cat_id,
first_cat_id,
second_cat_id,
impressions,
ctr,
gcr,
gmv_cr,
goods_score,
gcr_1w,
gmv_cr_1w,
impressions_1w,
test_goods_status,
test_goods_status_comment,
test_goods_result_status,
test_goods_result_comment,
add_test_time,
status_change_time
FROM
ads.ads_vova_web_goods_examination_summary_history_export
WHERE pt = '${cur_date}'
;


INSERT OVERWRITE TABLE dwb.dwb_vova_web_goods_examination PARTITION (pt = '${cur_date}')
select
'${cur_date}' AS event_date,
sum(if(test_goods_status = 1, 1, 0)) AS first_level_testing_cnt,
sum(if(test_goods_status = 2, 1, 0)) AS first_level_finished_cnt,
sum(if(test_goods_status = 3, 1, 0)) AS second_level_finished_cnt,
sum(if(test_goods_status = 4, 1, 0)) AS third_level_finished_cnt,
sum(if(test_goods_status = 2 AND date(status_change_time) = '${cur_date}', 1, 0)) AS first_level_cnt,
sum(if(test_goods_status = 2 AND test_goods_result_status = 2 AND date(status_change_time) = '${cur_date}', 1, 0)) AS first_level_success_cnt,
sum(if(test_goods_status = 2 AND test_goods_result_status = 3 AND date(status_change_time) = '${cur_date}', 1, 0)) AS first_level_failure_cnt,
sum(if(test_goods_status = 3 AND test_goods_result_status = 4 AND date(status_change_time) = '${cur_date}', 1, 0)) AS second_level_success_cnt,
sum(if(test_goods_status = 3 AND test_goods_result_status = 5 AND date(status_change_time) = '${cur_date}', 1, 0)) AS second_level_failure_cnt,
sum(if(test_goods_status = 4 AND test_goods_result_status = 6 AND date(status_change_time) = '${cur_date}', 1, 0)) AS third_level_success_cnt,
sum(if(test_goods_status = 4 AND test_goods_result_status = 7 AND date(status_change_time) = '${cur_date}', 1, 0)) AS third_level_failure_cnt
from
ads.ads_vova_web_goods_examination_summary_history_export
where pt = '${cur_date}'

;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=ads_vova_web_examination_pre" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

