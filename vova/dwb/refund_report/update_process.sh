#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_refund_process_req_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table dwb.dwb_vova_refund_report_detail
SELECT /*+ REPARTITION(3) */
       temp_fact_refund.create_time,
       temp_fact_refund.order_goods_id,
       temp_fact_refund.refund_type_id,
       temp_fact_refund.refund_type,
       temp_fact_refund.refund_reason_type_id,
       temp_fact_refund.refund_reason,
       temp_fact_refund.refund_amount,
       temp_fact_refund.bonus,
       temp_fact_refund.exec_refund_time,
       dog.sku_shipping_status,
       dog.order_tag,
       dog.order_goods_tag,
       dog.region_code,
       dog.platform,
       og.mct_shop_price_amount + og.mct_shipping_fee                              AS threshold_amount,
       if(dog.delivery_time_max IS NULL, dog.delivery_time, dog.delivery_time_max) AS final_delivery_time,
       dog.receive_time,
       dbu.first_refund_time,
       dog.buyer_id,
       og.shop_price_amount + og.shipping_fee                                    AS gmv,
       g.brand_id,
       oge.storage_type,
       dog.datasource,
       nvl(if(date(dd.activate_time) = date(temp_fact_refund.create_time),'Y','N'),'N') as is_new_activate
FROM (SELECT fc.create_time,
             fc.order_goods_id,
             fc.refund_type_id,
             fc.refund_type,
             fc.refund_reason_type_id,
             fc.refund_reason,
             fc.refund_amount,
             fc.bonus,
             fc.exec_refund_time
      FROM dwd.dwd_vova_fact_refund fc
      WHERE fc.create_time >= '2019-01-01'
        AND fc.sku_pay_status = 4) temp_fact_refund
         INNER JOIN dim.dim_vova_order_goods dog ON dog.order_goods_id = temp_fact_refund.order_goods_id
         LEFT JOIN ods_vova_vts.ods_vova_order_goods og ON og.rec_id = temp_fact_refund.order_goods_id
         LEFT JOIN ods_vova_vts.ods_vova_goods g ON g.goods_id = dog.goods_id
         LEFT JOIN dim.dim_vova_buyers dbu ON dbu.buyer_id = dog.buyer_id
         LEFT JOIN ods_vova_vts.ods_vova_order_goods_extra oge on oge.order_goods_id = dog.order_goods_id
         LEFT JOIN dim.dim_vova_devices dd on dd.device_id = dog.device_id AND dd.datasource = dog.datasource
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=${job_name}" \
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

#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
