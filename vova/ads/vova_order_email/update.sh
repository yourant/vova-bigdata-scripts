#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=`date -d "+1 day" +%Y-%m-%d`
fi

last_7_week=`date -d $cur_date"-7 week" +%Y-%m-%d`
src_weekday=`date -d $last_7_week +%w`
if [ $src_weekday == 0 ]
then
src_weekday=7
fi
src_day=`date -d "$last_7_week - $((src_weekday - 1)) days" +%F`
end_day=`date -d $src_day"+6 day" +%Y-%m-%d`
echo "start time:$src_day,end time:$end_day"

sql="
INSERT OVERWRITE TABLE ads.ads_vova_order_email PARTITION(pt='${cur_date}')
SELECT
    db.email,
    vr.region_name_cn,
    db.language_code
FROM
    dim.dim_vova_buyers db
    INNER JOIN (
SELECT
    fp.buyer_id,
    fp.region_id,
    row_number ( ) over ( PARTITION BY fp.buyer_id ORDER BY fp.pay_time DESC ) rk
FROM
    dwd.dwd_vova_fact_pay fp
    LEFT JOIN dim.dim_vova_order_goods og ON fp.order_goods_id = og.order_goods_id
WHERE
    date( fp.pay_time ) >= '${src_day}'
    AND date( fp.pay_time ) <= '${end_day}'
    AND og.sku_shipping_status > 0
    AND fp.datasource='vova'
    ) tmp_buyer ON tmp_buyer.buyer_id = db.buyer_id
    LEFT JOIN ods_vova_vts.ods_vova_region vr
    ON tmp_buyer.region_id = vr.region_id
WHERE
    tmp_buyer.rk = 1
    AND db.email not regexp '@vovaopen|@airyclub'
    AND NOT EXISTS ( SELECT 1 FROM ods_vova_vts.ods_vova_cms_shopping_blacklist csb WHERE csb.VALUE = db.email AND type = 'email' AND csb.rule_type = 1 )
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=vova_order_email" \
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
--name 'vova_order_email_send' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select email,region_name_cn,language_code from ads.ads_vova_order_email where pt='${cur_date}'"  \
-head "邮箱,国家,语言"  \
-receiver "jing.zhang@vova.com.hk,ted.wan@vova.com.hk" \
-title "vovav客服支付邮箱(${src_day}-${end_day})" \
--type attachment \
--fileName "vovav客服支付邮箱(${src_day}-${end_day})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi