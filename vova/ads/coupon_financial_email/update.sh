#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_month=$(date -d "-1 month" +%Y%m)
fi
sql="
-- req9855
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_coupon_financial_email partition(event_date)
SELECT
/*+ repartition(1) */
             datasource,
             region_code,
             cpn_cfg_type_id,
             cpn_cfg_type_name,
             cpn_cfg_type,
             currency,
             sum (bonus) AS use_amount,
             COUNT (order_id) AS use_num,
             COUNT (DISTINCT user_id) AS use_user,
             sum (goods_amount + shipping_fee) AS gmv,
             abs(sum (bonus)/sum (goods_amount + shipping_fee)*100),
             pay_date AS event_date
             from
(SELECT      from_unixtime(unix_timestamp(oi.pay_time),'yyyyMM')   AS pay_date,
             nvl(dc.cpn_cfg_type_id, '-1') AS cpn_cfg_type_id,
             dc.cpn_cfg_type_name,
             dc.cpn_cfg_type,
             nvl(byr.region_code, 'NA') AS region_code,
             nvl(byr.datasource, 'NA') AS datasource,
             nvl(dc.currency, 'NA') AS currency,
             dc.cpn_id,
             nvl(dc.cpn_cfg_val, 0) AS cpn_cfg_val,
             nvl(dc.buyer_id, 0) AS buyer_id,
             oi.order_id,
             oi.bonus,
             oi.goods_amount,
             oi.shipping_fee,
             oi.user_id
from
ods_vova_vts.ods_vova_order_info oi
inner join dim.dim_vova_coupon dc
on dc.cpn_code = oi.coupon_code
INNER JOIN dim.dim_vova_buyers byr ON byr.buyer_id = dc.buyer_id
where  date(oi.pay_time)>='2021-01-01' and from_unixtime(unix_timestamp(oi.pay_time),'yyyyMM') = '${pre_month}'
             and oi.pay_status = 2
             and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
             and oi.parent_order_id = 0)
group by pay_date,datasource,region_code,cpn_cfg_type_id,cpn_cfg_type_name,cpn_cfg_type,currency
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_coupon_financial_email" \
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
if [ $? -ne 0 ]; then
  exit 1
fi


spark-submit \
--deploy-mode client \
--name 'ads_vova_coupon_financial_email' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select event_date,datasource,region_code,cpn_cfg_type_id,cpn_cfg_type_name,cpn_cfg_type,currency,use_amount,use_num,use_user,gmv,coupon_rate from ads.ads_vova_coupon_financial_email where event_date='${pre_month}' order by  event_date desc,cpn_cfg_type_id desc"  \
-head "event_date,datasource,region_code,cpn_cfg_type_id,cpn_cfg_type_name,cpn_cfg_type,currency,use_amount(USD),use_num,use_user,gmv,coupon_rate"  \
-receiver "yuange@i9i8.com,ted.wan@vova.com.hk" \
-title "财务优惠券邮件" \
--type attachment \
--fileName "财务优惠券邮件"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi