#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_buyer_portrait_to_app PARTITION (pt = '${pre_date}')
select
/*+ repartition(20) */
t1.buyer_id as user_id,
t1.os_type,
t1.reg_age_group,
t1.reg_channel,
t1.reg_ctry,
t1.reg_gender,
t1.reg_time,
t1.first_cat_likes,
t1.second_cat_likes,
t1.first_order_time,
t1.last_order_time,
t1.order_cnt,
t1.avg_price,
t1.price_range,
t1.buyer_act,
t1.trade_act,
t1.last_logint_type,
t1.last_buyer_type,
t1.buy_times_type,
t1.email_act,
t2.pay_cnt_his,
t2.ship_cnt_his,
t2.max_visits_cnt_cw,
t2.gmv_stage,
t2.sub_new_buyers,
t2.is_order_complete,
t3.pm as rfm_pm,
t3.pf as rfm_pf,
t3.pr as rfm_pr,
t3.pn as rfm_pn,
t3.pimp as rfm_pimp
from
ads.ads_vova_buyer_stat_feature t1
inner join ads.ads_vova_buyer_portrait_d  t2 on t1.buyer_id = t2.user_id and t2.pt='${pre_date}'
left join ads.ads_vova_rfm90_tag t3 on t1.buyer_id = t3.user_id and t3.pt='${pre_date}'
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_buyer_portrait_to_app" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

res=$(curl https://ares-p.vovaapi.com/services/v1/notify/getUserPortraitData?param=warehouse/ads/ads_vova_buyer_portrait_to_app/pt=$pre_date/)
code=$(echo $res | jq '.code'| sed $'s/\"//g')
echo $code
if [ $code -ne 200 ];then
  exit 1
fi
