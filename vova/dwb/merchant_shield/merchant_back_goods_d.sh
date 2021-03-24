#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
###逻辑sql
sql="
--step1 click,impression
drop table if exists tmp.merchant_back_log_clickimpression;
create table tmp.merchant_back_log_clickimpression as
select t2.goods_sn,t1.session_id,t1.click_type from
(
select virtual_goods_id,session_id,'click' click_type from dwd.fact_log_goods_click
where datediff(CURRENT_DATE,pt) <= 7
and datasource='vova'
and list_type in('/detail_also_like', 'detail-also-like', 'list-search', '/search_result','detail-also-like-cart', 'favorites', '/favorites', 'cart-save-for-later','/dynamic_bubble', '/merchant_store', '', '/cart_also_like')
union all
select virtual_goods_id,session_id,'impression' click_type from dwd.fact_log_goods_impression
where datediff(CURRENT_DATE,pt) <= 7
and datasource='vova'
and list_type in('/detail_also_like', 'detail-also-like', 'list-search', '/search_result','detail-also-like-cart', 'favorites', '/favorites', 'cart-save-for-later','/dynamic_bubble', '/merchant_store', '', '/cart_also_like')
) t1
left outer join
dwd.dim_goods t2
on t1.virtual_goods_id=t2.virtual_goods_id;
--计算users
drop table if exists tmp.merchant_back_log_users;
create table tmp.merchant_back_log_users as
select t2.goods_sn,t1.session_id,t1.click_type from
(
select virtual_goods_id,session_id,'click' click_type from dwd.fact_log_page_view
where datediff(CURRENT_DATE,pt) <= 7 and virtual_goods_id is not null
and page_code ='product'
union all
select virtual_goods_id,session_id,'click' click_type from dwd.fact_log_goods_click
where datediff(CURRENT_DATE,pt) <= 7
and datasource='vova'
and page_code in ('product_detail', '', 'search_result', 'my_favorites', 'merchant_store')
) t1
left outer join
dwd.dim_goods t2
on t1.virtual_goods_id=t2.virtual_goods_id;


--step2 计算指标
insert overwrite table rpt.rpt_goods_sn_1d partition(pt='$pre_date')
select t1.goods_sn,
nvl(t3.pv_goods_impression_1w,0),
nvl(t2.pv_goods_click_1w,0),
nvl(t1.uv_1w,0),
nvl(t4.gmv_1w,0),
nvl(t2.pv_goods_click_1w*t4.gmv_1w/(t3.pv_goods_impression_1w*t1.uv_1w),0) gcr_1w ,
nvl(t2.pv_goods_click_1w/t3.pv_goods_impression_1w,0) ctr_1w,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP() from
(
select goods_sn,count(distinct session_id) uv_1w from tmp.merchant_back_log_users group by goods_sn
) t1
left outer join
(
select goods_sn,count(1) pv_goods_click_1w from tmp.merchant_back_log_clickimpression where click_type='click' group by goods_sn
) t2
on t1.goods_sn=t2.goods_sn
left outer join
(
select goods_sn,count(1) pv_goods_impression_1w from tmp.merchant_back_log_clickimpression where click_type='impression' group by goods_sn
) t3
on t1.goods_sn=t3.goods_sn
left outer join
(
select goods_sn,sum(shipping_fee)+sum(goods_number*shop_price) gmv_1w from dwd.fact_pay where datediff(CURRENT_DATE,to_date(order_time)) <= 7 group by goods_sn
) t4
on  t1.goods_sn=t4.goods_sn
where t1.goods_sn is not null and t1.goods_sn <>'NULL' and t1.goods_sn <>'';
"
new_sql="
insert overwrite table rpt.rpt_goods_sn_1d partition(pt='$pre_date')
select
t1.goods_sn,
nvl(sum(t2.impressions),0) pv_goods_impression_1w,
nvl(sum(t2.clicks),0)  pv_goods_click_1w,
nvl(sum(t2.users),0) uv_1w,
nvl(sum(t2.gmv),0) gmv_1w,
nvl(nvl(sum(t2.clicks),0)*nvl(sum(t2.gmv),0)/(nvl(sum(t2.impressions),0)*nvl(sum(t2.users),0)),0) gcr_1w,
nvl(nvl(sum(t2.clicks),0)/nvl(sum(t2.impressions),0),0) ctr_1w,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP()
from  dwd.dim_goods t1
left outer join
ods.vova_goods_display_sort t2
on t1.goods_id=t2.goods_id
where t1.goods_sn is not null and t1.goods_sn <> ''
group by t1.goods_sn
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=rpt_merchant_shield" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$new_sql"
spark-sql -e "$new_sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

grr_sql="
insert overwrite table rpt.rpt_goods_id_1d partition(pt='$pre_date')
select
t1.goods_id,
(sum(t1.nlrf_order_cnt_5_8w)+0.1*5)/(count(t1.order_goods_id)+5) as refund_rate_nonlogistics_8w
from
(
select
og.goods_id,
og.order_goods_id,
case when fr.refund_reason_type_id != 8 and fr.refund_type_id=2 then 1 else 0 end nlrf_order_cnt_5_8w
from dwd.dim_order_goods og
left join dwd.fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.fact_logistics fl on fr.order_goods_id=fl.order_goods_id
where datediff('${pre_date}', date(og.confirm_time)) between 35 and 56
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1
group by t1.goods_id
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=rpt_merchant_shield" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$grr_sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
#sqoop eval \
#  -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
#  -Dmapreduce.job.queuename=default \
#  --connect jdbc:mysql://vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis \
#  --username dbg20191029 --password lz5KtWHH8tIgGEYU5hYUbPGpkufmsfup \
#  -e "truncate table themis.rpt_goods_sn_1d"
#
#if [ $? -ne 0 ]; then
#  echo "truncate table failed"
#  exit 1
#fi

sqoop export \
  -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
  -Dmapreduce.job.queuename=default \
  --connect jdbc:mysql://vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis?rewriteBatchedStatements=true \
  --username dbg20191029 --password lz5KtWHH8tIgGEYU5hYUbPGpkufmsfup \
  --table rpt_goods_sn_1d \
  --m 1 \
  --update-key goods_sn \
  --update-mode allowinsert \
  --hcatalog-database rpt \
  --hcatalog-table rpt_goods_sn_1d \
  --hcatalog-partition-keys pt \
  --hcatalog-partition-values ${pre_date} \
  --fields-terminated-by '\001' \
  --batch

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "export  tale failed"
  exit 1
fi

sqoop export \
  -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
  -Dmapreduce.job.queuename=default \
  --connect jdbc:mysql://vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis?rewriteBatchedStatements=true \
  --username dbg20191029 --password lz5KtWHH8tIgGEYU5hYUbPGpkufmsfup \
  --m 1 \
  --table rpt_goods_id_1d \
  --update-key goods_id \
  --update-mode allowinsert \
  --hcatalog-database rpt \
  --hcatalog-table rpt_goods_id_1d \
  --hcatalog-partition-keys pt \
  --hcatalog-partition-values ${pre_date} \
  --columns goods_id,refund_rate_nonlogistics_8w \
  --fields-terminated-by '\001' \
  --batch

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "export  tale failed"
  exit 1
fi