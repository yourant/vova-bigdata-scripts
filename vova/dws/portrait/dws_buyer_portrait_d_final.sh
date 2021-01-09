#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

###逻辑sql
#依赖的表,dwd.dwd_vova_fact_buyer_portrait_base，dim.dim_vova_category，dim.dim_vova_goods
sql="
insert overwrite table dws.dws_vova_buyer_portrait partition(pt='$pre_date')
select
/*+ REPARTITION(30) */
t1.datasource,
t1.buyer_id,
t1.gender,
t1.user_age_group as age_range,
if(t2.country is null,t1.region_code,t2.country) country,
if(t3.language is null,t1.language_code,t3.language) language,
t1.platform,
t1.email,
t1.reg_method,
t1.reg_time,
if(size(t4.first_cat_prefer_1w)!=0,t4.first_cat_prefer_1w,null) first_cat_prefer_1w,
if(size(t5.first_cat_prefer_1m)!=0,t5.first_cat_prefer_1m,null) first_cat_prefer_1m,
if(size(t6.first_cat_prefer_his)!=0,t6.first_cat_prefer_his,null) first_cat_prefer_his,
if(size(t7.second_cat_prefer_1w)!=0,t7.second_cat_prefer_1w,null) second_cat_prefer_1w,
if(size(t8.second_cat_prefer_1m)!=0,t8.second_cat_prefer_1m,null) second_cat_prefer_1m,
if(size(t9.second_cat_prefer_his)!=0,t9.second_cat_prefer_his,null) second_cat_prefer_his,

nvl(t10.second_cat_max_click_1m,0),
nvl(t11.second_cat_max_collect_1m,0),
nvl(t12.second_cat_max_cart_1m,0),
nvl(t13.second_cat_max_order_1m,0),

if(size(t14.brand_prefer_1w)!=0,t14.brand_prefer_1w,null) brand_prefer_1w,
if(size(t15.brand_prefer_1m)!=0,t15.brand_prefer_1m,null) brand_prefer_1m,
if(size(t16.brand_prefer_his)!=0,t16.brand_prefer_his,null) brand_prefer_his,
nvl(t17.brand_max_click_1m,0),
nvl(t18.brand_max_collect_1m,0),
nvl(t19.brand_max_cart_1m,0),
nvl(t20.brand_max_order_1m,0),
t21.active_day_1m,
t22.active_week_his,
t23.active_month_his,
t24.price_prefer_1w,
nvl(t25.pay_cnt_his,0) pay_cnt_his,
nvl(t26.ship_cnt_his,0) ship_cnt_his,
nvl(t27.max_visits_cnt_cw,0) max_visits_cnt_cw
from dim.dim_vova_buyers t1
left outer join
(
select datasource,buyer_id,country from
(
select datasource,buyer_id,country,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
(
select datasource,buyer_id,tag_name country,sum(cnt) cnt from dwd.dwd_vova_fact_buyer_portrait_base
where datediff(current_date(),pt)<=30 and act_type_id=8 group by datasource,buyer_id,tag_name
) a
) b where b.rank=1
) t2
on t1.datasource=t2.datasource and t1.buyer_id=t2.buyer_id
left outer join
(
select datasource,buyer_id,language from
(
select datasource,buyer_id,language,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
(
select datasource,buyer_id,tag_id language,sum(cnt) cnt from dwd.dwd_vova_fact_buyer_portrait_base
where datediff(current_date(),pt)<=30 and act_type_id=8 group by datasource,buyer_id,tag_id
) a
) b where b.rank=1
) t3
on t1.datasource=t3.datasource and t1.buyer_id=t3.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_cat7_result t4
on t1.datasource=t4.datasource and t1.buyer_id=t4.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_cat30_result t5
on t1.datasource=t5.datasource and t1.buyer_id=t5.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_first_cat_his_result t6
on t1.datasource=t6.datasource and t1.buyer_id=t6.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat7_result t7
on t1.datasource=t7.datasource and t1.buyer_id=t7.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat30_result t8
on t1.datasource=t8.datasource and t1.buyer_id=t8.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat_his_result t9
on t1.datasource=t9.datasource and t1.buyer_id=t9.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_click_result t10
on t1.datasource=t10.datasource and t1.buyer_id=t10.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_collect_result t11
on t1.datasource=t11.datasource and t1.buyer_id=t11.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_cart_result t12
on t1.datasource=t12.datasource and t1.buyer_id=t12.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_order_result t13
on t1.datasource=t13.datasource and t1.buyer_id=t13.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand7_result t14
on t1.datasource=t14.datasource and t1.buyer_id=t14.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand30_result t15
on t1.datasource=t15.datasource and t1.buyer_id=t15.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand_his_result t16
on t1.datasource=t16.datasource and t1.buyer_id=t16.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand_max30_click_result t17
on t1.datasource=t17.datasource and t1.buyer_id=t17.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand_max30_collect_result t18
on t1.datasource=t18.datasource and t1.buyer_id=t18.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand_max30_cart_result t19
on t1.datasource=t19.datasource and t1.buyer_id=t19.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_brand_max30_order_result t20
on t1.datasource=t20.datasource and t1.buyer_id=t20.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_active_day_result t21
on t1.datasource=t21.datasource and t1.buyer_id=t21.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_active_week_result t22
on t1.datasource=t22.datasource and t1.buyer_id=t22.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_active_month_result t23
on t1.datasource=t23.datasource and t1.buyer_id=t23.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_price_result t24
on t1.datasource=t24.datasource and t1.buyer_id=t24.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_pay_order t25
on t1.datasource=t25.datasource and t1.buyer_id=t25.buyer_id
left outer join
tmp.tmp_vova_dws_buyer_portrait_ship t26
on t1.datasource=t26.datasource and t1.buyer_id=t26.buyer_id
left outer join
tmp.tmp_vova_buyer_max_visits_cw t27
on t1.datasource=t27.datasource and t1.buyer_id=t27.buyer_id
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 --num-executors 100 --driver-memory 8G \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.app.name=dws_buyer_portrait" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=400" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.driver.maxResultSize=3G" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo 'base sql error'
  exit 1
fi
##复制到s3，算法使用
#hadoop fs -rm -r s3://vova-bd-offline/warehouse/dws/dws_buyer_portrait/pt=${pre_date}
#hadoop distcp -Dmapreduce.map.memory.mb=10240 -Dmapreduce.reduce.memory.mb=10240 hdfs:///user/hive/warehouse/dws.db/dws_buyer_portrait/pt=${pre_date} s3://vova-bd-offline/warehouse/dws/dws_buyer_portrait
#if [ $? -ne 0 ]; then
#  echo 'distcp error'
#  exit 1
#fi
