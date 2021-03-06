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
--category品类偏好基础表，计算权重值
drop table if exists tmp.tmp_vova_dws_buyer_portrait_cat;
create table tmp.tmp_vova_dws_buyer_portrait_cat STORED AS PARQUETFILE
 as
select
/*+ REPARTITION(200) */
f1.datasource,f1.buyer_id,f1.tag_id goods_id,
f2.act_weight_detail*cnt/ln(datediff(current_date(),f1.pt)) reduce_cnt,
if(f1.act_type_id = 2,0,f2.act_weight_detail*cnt) his_cnt,
cnt,
case when datediff(current_date(),f1.pt) <=7 then 7
when datediff(current_date(),f1.pt) >7 and datediff(current_date(),f1.pt) <=30 then 30
when datediff(current_date(),f1.pt) >30 then 40
end as day_type
from dwd.dwd_vova_fact_buyer_portrait_base f1
left outer join
dwd.dwd_vova_buyer_portrait_weight f2
on f1.act_type_id=f2.act_type_id
where datediff(current_date(),f1.pt)<=365 and f1.act_type_id in (2,4,6,1) and f2.prefer_type=2 and f1.buyer_id<>0;


--一级品类临时表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_first_cat;
create table tmp.tmp_vova_dws_buyer_portrait_first_cat STORED AS PARQUETFILE as
select
/*+ REPARTITION(20) */
t1.datasource,
t1.buyer_id,
t2.first_cat_id,
nvl(sum(t1.reduce_cnt),0) cnt,
nvl(sum(t1.his_cnt),0) his_cnt,
t1.day_type from
tmp.tmp_vova_dws_buyer_portrait_cat t1
left outer join
dim.dim_vova_goods t2
on t1.goods_id=t2.goods_id
group by t1.datasource,t1.buyer_id,t2.first_cat_id,t1.day_type;

--category近一周一级类目偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_cat7_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(first_cat_id) first_cat_prefer_1w from
(
select datasource,buyer_id,first_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_first_cat where day_type=7
) a where rank <=10 group by datasource,buyer_id;


--category近一月一级类目偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_cat30_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(first_cat_id) first_cat_prefer_1m from
(
select datasource,buyer_id,first_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_first_cat where day_type in(7,30)
) a where rank <=10 group by datasource,buyer_id;


--category历史一级类目偏好结果表
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_first_cat_his_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(first_cat_id) first_cat_prefer_his from
(
select datasource,buyer_id,first_cat_id, row_number() over(partition by datasource,buyer_id order by his_cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_first_cat
) a where rank <=10 group by datasource,buyer_id;

--二级类目临时表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat STORED AS PARQUETFILE as
select
/*+ REPARTITION(50) */
t1.datasource,
t1.buyer_id,
if(t2.second_cat_id is null,t2.first_cat_id,t2.second_cat_id) second_cat_id,
nvl(sum(t1.reduce_cnt),0) cnt ,
nvl(sum(t1.his_cnt),0) his_cnt ,
nvl(sum(t1.cnt),0) cnt_only ,
t1.day_type
from
tmp.tmp_vova_dws_buyer_portrait_cat t1
left outer join
dim.dim_vova_goods t2
on t1.goods_id=t2.goods_id
group by t1.datasource,t1.buyer_id,if(t2.second_cat_id is null,t2.first_cat_id,t2.second_cat_id),t1.day_type;

--category近一周二级类目偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_second_cat7_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(second_cat_id) second_cat_prefer_1w from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat where day_type = 7
) a where rank <=10 group by datasource,buyer_id;

--category近一月二级类目偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_second_cat30_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(second_cat_id) second_cat_prefer_1m from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat where day_type in(7,30)
) a where rank <=10 group by datasource,buyer_id;

--历史二级类目偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_second_cat_his_result
select
/*+ REPARTITION(4) */
datasource,buyer_id,collect_list(second_cat_id) second_cat_prefer_his from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by his_cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat
) a where rank <=10 group by datasource,buyer_id;

--月最多二级品类ID临时表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat_max30;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat_max30 STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ f1.datasource,
f1.buyer_id,
if(t2.second_cat_id is null,t2.first_cat_id,t2.second_cat_id) second_cat_id,
act_type_id,
sum(cnt) cnt from dwd.dwd_vova_fact_buyer_portrait_base f1
left outer join
dim.dim_vova_goods t2
on f1.tag_id=t2.goods_id
where datediff(current_date(),f1.pt)<=30 and f1.act_type_id in (2,4,6,1) and f1.buyer_id<>0
group by f1.datasource,
f1.buyer_id,
if(t2.second_cat_id is null,t2.first_cat_id,t2.second_cat_id),
act_type_id;

--月点击最多二级品类ID
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_click_result;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_click_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,second_cat_id second_cat_max_click_1m from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30 where act_type_id=2
) a where rank =1 ;
--月收藏最多二级品类ID
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_collect_result;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_collect_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,second_cat_id second_cat_max_collect_1m from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30 where act_type_id=4
) a where rank =1 ;
--月加购最多二级品类ID
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_cart_result;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_cart_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,second_cat_id second_cat_max_cart_1m from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30 where act_type_id=6
) a where rank =1 ;
--月下单最多二级品类ID
drop table if exists tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_order_result;
create table tmp.tmp_vova_dws_buyer_portrait_second_cat_max30_order_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,second_cat_id second_cat_max_order_1m from
(
select datasource,buyer_id,second_cat_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_second_cat_max30 where act_type_id=1
) a where rank =1 ;


--品牌偏好基础表，计算权重值
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand;
create table tmp.tmp_vova_dws_buyer_portrait_brand STORED AS PARQUETFILE as
select
/*+ REPARTITION(200) */
f1.datasource,f1.buyer_id,f1.tag_id goods_id,
f2.act_weight_detail*cnt/ln(datediff(current_date(),f1.pt)) reduce_cnt,
if(f1.act_type_id = 2,0,f2.act_weight_detail*cnt) his_cnt,
cnt,
case when datediff(current_date(),f1.pt) <=7 then 7
when datediff(current_date(),f1.pt) >7 and datediff(current_date(),f1.pt) <=30 then 30
when datediff(current_date(),f1.pt) >30 then 40
end as day_type
from dwd.dwd_vova_fact_buyer_portrait_base f1
left outer join
dwd.dwd_vova_buyer_portrait_weight f2
on f1.act_type_id=f2.act_type_id
where datediff(current_date(),f1.pt)<=365 and f1.act_type_id in (2,4,6,1) and f2.prefer_type=1 and f1.buyer_id<>0;

--品牌临时表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_tmp;
create table tmp.tmp_vova_dws_buyer_portrait_brand_tmp STORED AS PARQUETFILE as
select
/*+ REPARTITION(50) */
t1.datasource,
t1.buyer_id,
t2.brand_id,
nvl(sum(t1.reduce_cnt),0) cnt,
nvl(sum(t1.his_cnt),0) his_cnt,
t1.day_type from
tmp.tmp_vova_dws_buyer_portrait_brand t1
left outer join
dim.dim_vova_goods t2
on t1.goods_id=t2.goods_id
where t2.brand_id<>0
group by t1.datasource,t1.buyer_id,t2.brand_id,t1.day_type;

--近一周品牌偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_brand7_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(brand_id) brand_prefer_1w from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_tmp where day_type=7
) a where rank <=10 group by datasource,buyer_id;

--近一月品牌偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_brand30_result
select /*+ REPARTITION(20) */ datasource,buyer_id,collect_list(brand_id) brand_prefer_1m from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_tmp where day_type in (7,30)
) a where rank <=10 group by datasource,buyer_id;

--历史品牌偏好结果表，取top10
INSERT OVERWRITE TABLE tmp.tmp_vova_dws_buyer_portrait_brand_his_result
select
/*+ REPARTITION(4) */
datasource,buyer_id,collect_list(brand_id) brand_prefer_his from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_tmp
) a where rank <=10 group by datasource,buyer_id;

--月最多品牌临时表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_max30;
create table tmp.tmp_vova_dws_buyer_portrait_brand_max30 STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ f1.datasource,
f1.buyer_id,
t2.brand_id,
act_type_id,
sum(cnt) cnt from dwd.dwd_vova_fact_buyer_portrait_base f1
left outer join
dim.dim_vova_goods t2
on f1.tag_id=t2.goods_id
where datediff(current_date(),f1.pt)<=30 and f1.act_type_id in (2,4,6,1) and f1.buyer_id<>0 and t2.brand_id<>0
group by  f1.datasource,
f1.buyer_id,
t2.brand_id,
act_type_id;

--月点击最多品牌
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_max30_click_result;
create table tmp.tmp_vova_dws_buyer_portrait_brand_max30_click_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,brand_id brand_max_click_1m from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_max30 where act_type_id=2
) a where rank =1 ;
--月收藏最多品牌
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_max30_collect_result;
create table tmp.tmp_vova_dws_buyer_portrait_brand_max30_collect_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,brand_id brand_max_collect_1m from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_max30 where act_type_id=4
) a where rank =1 ;
--月加购最多品牌
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_max30_cart_result;
create table tmp.tmp_vova_dws_buyer_portrait_brand_max30_cart_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,brand_id brand_max_cart_1m from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_max30 where act_type_id=6
) a where rank =1 ;
--月下单最多品牌
drop table if exists tmp.tmp_vova_dws_buyer_portrait_brand_max30_order_result;
create table tmp.tmp_vova_dws_buyer_portrait_brand_max30_order_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,brand_id brand_max_order_1m from
(
select datasource,buyer_id,brand_id, row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_brand_max30 where act_type_id=1
) a where rank =1 ;


--每日活跃时段
drop table if exists tmp.tmp_vova_dws_buyer_portrait_active_day_result;
create table tmp.tmp_vova_dws_buyer_portrait_active_day_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,tag_id active_day_1m from
(
select datasource,buyer_id,tag_id,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
(
select datasource,buyer_id,tag_id,count(1) cnt
from dwd.dwd_vova_fact_buyer_portrait_base
where datediff(current_date(),pt)<=30 and tag_name='active_day' and act_type_id=9 and tag_id is not null
group by datasource,buyer_id,tag_id
) a
) b where b.rank=1;
--按周的活跃日期
drop table if exists tmp.tmp_vova_dws_buyer_portrait_active_week_result;
create table tmp.tmp_vova_dws_buyer_portrait_active_week_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,tag_id active_week_his from
(
select datasource,buyer_id,tag_id,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
(
select datasource,buyer_id,tag_id,count(1) cnt
from dwd.dwd_vova_fact_buyer_portrait_base
where datediff(current_date(),pt)<=365 and tag_name='active_week' and act_type_id=9 and tag_id is not null
group by datasource,buyer_id,tag_id
) a
) b where b.rank=1;

--按月的活跃日期
drop table if exists tmp.tmp_vova_dws_buyer_portrait_active_month_result;
create table tmp.tmp_vova_dws_buyer_portrait_active_month_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,tag_id active_month_his from
(
select datasource,buyer_id,tag_id,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
(
select datasource,buyer_id,tag_id,count(1) cnt
from dwd.dwd_vova_fact_buyer_portrait_base
where datediff(current_date(),pt)<=365 and tag_name='active_month' and act_type_id=9 and tag_id is not null
group by datasource,buyer_id,tag_id
) a
) b where b.rank=1;


--价格偏好基础表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_price;
create table tmp.tmp_vova_dws_buyer_portrait_price STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ f1.datasource,f1.buyer_id,
case when t2.shop_price >0 and  t2.shop_price<=5 then '0-5'
when t2.shop_price >5 and  t2.shop_price<=10 then '5-10'
when t2.shop_price >10 and  t2.shop_price<=20 then '10-20'
when t2.shop_price >20 and  t2.shop_price<=30 then '20-30'
when t2.shop_price >30 and  t2.shop_price<=50 then '30-50'
when t2.shop_price >50 and  t2.shop_price<=100 then '50-100'
when t2.shop_price >100 and  t2.shop_price<=200 then '100-200'
when t2.shop_price >200 then '200-'
end as price_range,
sum(cnt) cnt
from dwd.dwd_vova_fact_buyer_portrait_base f1
left outer join
dim.dim_vova_goods t2
on f1.tag_id=t2.goods_id
where datediff(current_date(),f1.pt)<=7 and f1.act_type_id in (2,4,6,1) and f1.buyer_id<>0
group by f1.datasource,f1.buyer_id,
case when t2.shop_price >0 and  t2.shop_price<=5 then '0-5'
when t2.shop_price >5 and  t2.shop_price<=10 then '5-10'
when t2.shop_price >10 and  t2.shop_price<=20 then '10-20'
when t2.shop_price >20 and  t2.shop_price<=30 then '20-30'
when t2.shop_price >30 and  t2.shop_price<=50 then '30-50'
when t2.shop_price >50 and  t2.shop_price<=100 then '50-100'
when t2.shop_price >100 and  t2.shop_price<=200 then '100-200'
when t2.shop_price >200 then '200-'
end;
--价格偏好结果表
drop table if exists tmp.tmp_vova_dws_buyer_portrait_price_result;
create table tmp.tmp_vova_dws_buyer_portrait_price_result STORED AS PARQUETFILE as
select /*+ REPARTITION(20) */ datasource,buyer_id,price_range price_prefer_1w from
(
select datasource,buyer_id,price_range,row_number() over(partition by datasource,buyer_id order by cnt desc ) rank from
tmp.tmp_vova_dws_buyer_portrait_price
) b where b.rank=1;


drop table if exists tmp.tmp_vova_dws_buyer_portrait_pay_order;
create table tmp.tmp_vova_dws_buyer_portrait_pay_order STORED AS PARQUETFILE as
select
/*+ REPARTITION(20) */
datasource,
buyer_id,
count(distinct order_id) pay_cnt_his
from dwd.dwd_vova_fact_pay group by datasource,buyer_id;


drop table if exists tmp.tmp_vova_dws_buyer_portrait_ship;
create table tmp.tmp_vova_dws_buyer_portrait_ship STORED AS PARQUETFILE as
select
/*+ REPARTITION(20) */
datasource,
buyer_id,
count(distinct order_id) ship_cnt_his
from dim.dim_vova_order_goods
where sku_shipping_status >0
group by datasource,buyer_id;


drop table if exists tmp.tmp_vova_buyer_max_visits_cw;
create table tmp.tmp_vova_buyer_max_visits_cw STORED AS PARQUETFILE as
select
/*+ REPARTITION(20) */
datasource,
buyer_id,
max(cnt) max_visits_cnt_cw
from
(
select
datasource,
buyer_id,
cnt
from tmp.tmp_vova_buyer_max_visits_cw_his
union all
select
datasource,
buyer_id,
count(distinct pt) cnt
from
dwd.dwd_vova_log_screen_view
where year(pt) = year('${pre_date}') and weekofyear(pt) = weekofyear('${pre_date}')
group by datasource,buyer_id
) t1
group by datasource,buyer_id;
--更新最新的自然周访问次数历史表
drop table if exists tmp.tmp_vova_buyer_max_visits_cw_his;
create table tmp.tmp_vova_buyer_max_visits_cw_his STORED AS PARQUETFILE as
select
/*+ REPARTITION(4) */
datasource,
buyer_id,
max_visits_cnt_cw cnt
from tmp.tmp_vova_buyer_max_visits_cw;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 --num-executors 100 --driver-memory 5G \
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
