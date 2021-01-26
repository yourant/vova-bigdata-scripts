#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_pay_refund_report_req_chenkai_${cur_date}"

###逻辑sql
sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_pay_refund_report PARTITION (pt)
SELECT /*+ REPARTITION(1) */
       pay_order.action_date,
       pay_order.region_code,
       'all' AS activity,
       pay_order.platform,
       pay_order.threshold,
       pay_order.is_first_refund,
       pay_order.over_delivery_days,
       pay_order.shipping_status_note,
       pay_order.storage_type,
       pay_order.datasource,
       pay_order.is_new_activate,
       pay_order.user_number,
       pay_order.order_goods_number,
       refund_total.refund_amount,
       refund_total.refund_buyer_number,
       refund_total.refund_order_number,
       pay_order.gmv,
       refund_total.refund_reason_order_1,
       refund_total.refund_reason_order_2,
       refund_total.refund_reason_order_3,
       refund_total.refund_reason_order_4,
       refund_total.refund_reason_order_5,
       refund_total.refund_reason_order_6,
       refund_total.refund_reason_order_7,
       refund_total.refund_reason_order_8,
       refund_total.refund_reason_order_9,
       refund_total.refund_reason_order_10,
       refund_total.refund_reason_order_11,
       refund_total.refund_reason_order_12,
       refund_total.refund_reason_order_13,
       custom_refund.customer_reason_order_1,
       custom_refund.customer_reason_order_2,
       custom_refund.customer_reason_order_3,
       custom_refund.customer_reason_order_4,
       custom_refund.customer_reason_order_5,
       custom_refund.customer_reason_order_6,
       custom_refund.customer_reason_order_7,
       custom_refund.customer_reason_order_8,
       custom_refund.customer_reason_order_9,
       custom_refund.customer_reason_order_10,
       refund_total.refund_reason_gmv_1,
       refund_total.refund_reason_gmv_2,
       refund_total.refund_reason_gmv_3,
       refund_total.refund_reason_gmv_4,
       refund_total.refund_reason_gmv_5,
       refund_total.refund_reason_gmv_6,
       refund_total.refund_reason_gmv_7,
       refund_total.refund_reason_gmv_8,
       refund_total.refund_reason_gmv_9,
       refund_total.refund_reason_gmv_10,
       refund_total.refund_reason_gmv_11,
       refund_total.refund_reason_gmv_12,
       refund_total.refund_reason_gmv_13,
       custom_refund.customer_reason_gmv_1,
       custom_refund.customer_reason_gmv_2,
       custom_refund.customer_reason_gmv_3,
       custom_refund.customer_reason_gmv_4,
       custom_refund.customer_reason_gmv_5,
       custom_refund.customer_reason_gmv_6,
       custom_refund.customer_reason_gmv_7,
       custom_refund.customer_reason_gmv_8,
       custom_refund.customer_reason_gmv_9,
       custom_refund.customer_reason_gmv_10,
       pay_order.pt
FROM (
         SELECT nvl(date(pay_time), 'all')       AS action_date,
                nvl(region_code, 'all')          AS region_code,
                nvl(platform, 'all')             AS platform,
                nvl(threshold, 'all')            AS threshold,
                nvl(is_first_refund, 'all')      AS is_first_refund,
                nvl(over_delivery_days, 'all')   AS over_delivery_days,
                nvl(shipping_status_note, 'all') AS shipping_status_note,
                nvl(storage_type, 'all')         AS storage_type,
                nvl(datasource, 'all')           AS datasource,
                nvl(is_new_activate, 'all')      AS is_new_activate,
                count(DISTINCT buyer_id)         AS user_number,
                count(order_goods_id)            AS order_goods_number,
                SUM(gmv)                         AS gmv,
                nvl(date(pay_time), 'all')       AS pt
         FROM dwb.dwb_vova_pay_refund_detail
         WHERE pay_time >= date_sub('${cur_date}', 40)
         GROUP BY CUBE (region_code, platform, threshold,
                        date(pay_time), is_first_refund, over_delivery_days,
                        datasource, storage_type, shipping_status_note, is_new_activate)) pay_order
         LEFT JOIN
     (SELECT nvl(final.action_date, 'all')          AS action_date,
             nvl(final.region_code, 'all')          AS region_code,
             nvl(final.platform, 'all')             AS platform,
             nvl(final.threshold, 'all')            AS threshold,
             nvl(final.is_first_refund, 'all')      AS is_first_refund,
             nvl(final.over_delivery_days, 'all')   AS over_delivery_days,
             nvl(final.shipping_status_note, 'all') AS shipping_status_note,
             nvl(final.storage_type, 'all')         AS storage_type,
             nvl(final.datasource, 'all')           AS datasource,
             nvl(final.is_new_activate, 'all')      AS is_new_activate,
             SUM(refund_amount)                     AS refund_amount,
             count(DISTINCT refund_buyer_id)        AS refund_buyer_number,
             count(DISTINCT refund_id)              AS refund_order_number,
             count(DISTINCT refund_reason_order_1)  AS refund_reason_order_1,
             count(DISTINCT refund_reason_order_2)  AS refund_reason_order_2,
             count(DISTINCT refund_reason_order_3)  AS refund_reason_order_3,
             count(DISTINCT refund_reason_order_4)  AS refund_reason_order_4,
             count(DISTINCT refund_reason_order_5)  AS refund_reason_order_5,
             count(DISTINCT refund_reason_order_6)  AS refund_reason_order_6,
             count(DISTINCT refund_reason_order_7)  AS refund_reason_order_7,
             count(DISTINCT refund_reason_order_8)  AS refund_reason_order_8,
             count(DISTINCT refund_reason_order_9)  AS refund_reason_order_9,
             count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
             count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
             count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
             count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
             nvl(SUM(refund_reason_gmv_1), 0)       AS refund_reason_gmv_1,
             nvl(SUM(refund_reason_gmv_2), 0)       AS refund_reason_gmv_2,
             nvl(SUM(refund_reason_gmv_3), 0)       AS refund_reason_gmv_3,
             nvl(SUM(refund_reason_gmv_4), 0)       AS refund_reason_gmv_4,
             nvl(SUM(refund_reason_gmv_5), 0)       AS refund_reason_gmv_5,
             nvl(SUM(refund_reason_gmv_6), 0)       AS refund_reason_gmv_6,
             nvl(SUM(refund_reason_gmv_7), 0)       AS refund_reason_gmv_7,
             nvl(SUM(refund_reason_gmv_8), 0)       AS refund_reason_gmv_8,
             nvl(SUM(refund_reason_gmv_9), 0)       AS refund_reason_gmv_9,
             nvl(SUM(refund_reason_gmv_10), 0)      AS refund_reason_gmv_10,
             nvl(SUM(refund_reason_gmv_11), 0)      AS refund_reason_gmv_11,
             nvl(SUM(refund_reason_gmv_12), 0)      AS refund_reason_gmv_12,
             nvl(SUM(refund_reason_gmv_13), 0)      AS refund_reason_gmv_13
      FROM (
               SELECT date(pay_time)                                AS action_date,
                      region_code,
                      platform,
                      threshold,
                      is_first_refund,
                      over_delivery_days,
                      shipping_status_note,
                      storage_type,
                      datasource,
                      is_new_activate,
                      if(refund_type_id = 1, order_goods_id, NULL)  AS refund_reason_order_1,
                      if(refund_type_id = 2, order_goods_id, NULL)  AS refund_reason_order_2,
                      if(refund_type_id = 3, order_goods_id, NULL)  AS refund_reason_order_3,
                      if(refund_type_id = 4, order_goods_id, NULL)  AS refund_reason_order_4,
                      if(refund_type_id = 5, order_goods_id, NULL)  AS refund_reason_order_5,
                      if(refund_type_id = 6, order_goods_id, NULL)  AS refund_reason_order_6,
                      if(refund_type_id = 7, order_goods_id, NULL)  AS refund_reason_order_7,
                      if(refund_type_id = 8, order_goods_id, NULL)  AS refund_reason_order_8,
                      if(refund_type_id = 9, order_goods_id, NULL)  AS refund_reason_order_9,
                      if(refund_type_id = 10, order_goods_id, NULL) AS refund_reason_order_10,
                      if(refund_type_id = 11, order_goods_id, NULL) AS refund_reason_order_11,
                      if(refund_type_id = 12, order_goods_id, NULL) AS refund_reason_order_12,
                      if(refund_type_id = 13, order_goods_id, NULL) AS refund_reason_order_13,
                      if(refund_type_id = 1, refund_amount, 0)      AS refund_reason_gmv_1,
                      if(refund_type_id = 2, refund_amount, 0)      AS refund_reason_gmv_2,
                      if(refund_type_id = 3, refund_amount, 0)      AS refund_reason_gmv_3,
                      if(refund_type_id = 4, refund_amount, 0)      AS refund_reason_gmv_4,
                      if(refund_type_id = 5, refund_amount, 0)      AS refund_reason_gmv_5,
                      if(refund_type_id = 6, refund_amount, 0)      AS refund_reason_gmv_6,
                      if(refund_type_id = 7, refund_amount, 0)      AS refund_reason_gmv_7,
                      if(refund_type_id = 8, refund_amount, 0)      AS refund_reason_gmv_8,
                      if(refund_type_id = 9, refund_amount, 0)      AS refund_reason_gmv_9,
                      if(refund_type_id = 10, refund_amount, 0)     AS refund_reason_gmv_10,
                      if(refund_type_id = 11, refund_amount, 0)     AS refund_reason_gmv_11,
                      if(refund_type_id = 12, refund_amount, 0)     AS refund_reason_gmv_12,
                      if(refund_type_id = 13, refund_amount, 0)     AS refund_reason_gmv_13,
                      buyer_id                                      AS refund_buyer_id,
                      refund_amount,
                      refund_id
               FROM dwb.dwb_vova_pay_refund_detail
               WHERE pay_time >= date_sub('${cur_date}', 40)
                 AND refund_id IS NOT NULL) final
      GROUP BY CUBE (final.region_code, final.platform, final.threshold,
                     final.action_date, final.is_first_refund, final.over_delivery_days,
                     final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)) refund_total
     ON pay_order.region_code = refund_total.region_code
         AND pay_order.datasource = refund_total.datasource
         AND pay_order.platform = refund_total.platform
         AND pay_order.threshold = refund_total.threshold
         AND pay_order.action_date = refund_total.action_date
         AND pay_order.is_first_refund = refund_total.is_first_refund
         AND pay_order.over_delivery_days = refund_total.over_delivery_days
         AND pay_order.storage_type = refund_total.storage_type
         AND pay_order.shipping_status_note = refund_total.shipping_status_note
         AND pay_order.is_new_activate = refund_total.is_new_activate
         LEFT JOIN
     (SELECT nvl(final.action_date, 'all')            AS action_date,
             nvl(final.region_code, 'all')            AS region_code,
             nvl(final.platform, 'all')               AS platform,
             nvl(final.threshold, 'all')              AS threshold,
             nvl(final.is_first_refund, 'all')        AS is_first_refund,
             nvl(final.over_delivery_days, 'all')     AS over_delivery_days,
             nvl(final.shipping_status_note, 'all')   AS shipping_status_note,
             nvl(final.storage_type, 'all')           AS storage_type,
             nvl(final.datasource, 'all')             AS datasource,
             nvl(final.is_new_activate, 'all')        AS is_new_activate,
             count(DISTINCT customer_reason_order_1)  AS customer_reason_order_1,
             count(DISTINCT customer_reason_order_2)  AS customer_reason_order_2,
             count(DISTINCT customer_reason_order_3)  AS customer_reason_order_3,
             count(DISTINCT customer_reason_order_4)  AS customer_reason_order_4,
             count(DISTINCT customer_reason_order_5)  AS customer_reason_order_5,
             count(DISTINCT customer_reason_order_6)  AS customer_reason_order_6,
             count(DISTINCT customer_reason_order_7)  AS customer_reason_order_7,
             count(DISTINCT customer_reason_order_8)  AS customer_reason_order_8,
             count(DISTINCT customer_reason_order_9)  AS customer_reason_order_9,
             count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
             nvl(SUM(customer_reason_gmv_1), 0)       AS customer_reason_gmv_1,
             nvl(SUM(customer_reason_gmv_2), 0)       AS customer_reason_gmv_2,
             nvl(SUM(customer_reason_gmv_3), 0)       AS customer_reason_gmv_3,
             nvl(SUM(customer_reason_gmv_4), 0)       AS customer_reason_gmv_4,
             nvl(SUM(customer_reason_gmv_5), 0)       AS customer_reason_gmv_5,
             nvl(SUM(customer_reason_gmv_6), 0)       AS customer_reason_gmv_6,
             nvl(SUM(customer_reason_gmv_7), 0)       AS customer_reason_gmv_7,
             nvl(SUM(customer_reason_gmv_8), 0)       AS customer_reason_gmv_8,
             nvl(SUM(customer_reason_gmv_9), 0)       AS customer_reason_gmv_9,
             nvl(SUM(customer_reason_gmv_10), 0)      AS customer_reason_gmv_10
      FROM (
               SELECT date(pay_time)                                                          AS action_date,
                      region_code,
                      platform,
                      threshold,
                      is_first_refund,
                      over_delivery_days,
                      shipping_status_note,
                      storage_type,
                      datasource,
                      is_new_activate,
                      if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id,
                         NULL)                                                                AS customer_reason_order_1,
                      if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id,
                         NULL)                                                                AS customer_reason_order_2,
                      if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id,
                         NULL)                                                                AS customer_reason_order_3,
                      if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id,
                         NULL)                                                                AS customer_reason_order_4,
                      if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id,
                         NULL)                                                                AS customer_reason_order_5,
                      if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id,
                         NULL)                                                                AS customer_reason_order_6,
                      if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id,
                         NULL)                                                                AS customer_reason_order_7,
                      if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id,
                         NULL)                                                                AS customer_reason_order_8,
                      if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id,
                         NULL)                                                                AS customer_reason_order_9,
                      if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id,
                         NULL)                                                                AS customer_reason_order_10,
                      if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)  AS customer_reason_gmv_1,
                      if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)  AS customer_reason_gmv_2,
                      if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)  AS customer_reason_gmv_3,
                      if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)  AS customer_reason_gmv_4,
                      if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)  AS customer_reason_gmv_5,
                      if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)  AS customer_reason_gmv_6,
                      if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)  AS customer_reason_gmv_7,
                      if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)  AS customer_reason_gmv_8,
                      if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)  AS customer_reason_gmv_9,
                      if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0) AS customer_reason_gmv_10
               FROM dwb.dwb_vova_pay_refund_detail
               WHERE pay_time >= date_sub('${cur_date}', 40)
                 AND refund_type_id = 2) final
      GROUP BY CUBE (final.region_code, final.platform, final.threshold,
                     final.action_date, final.is_first_refund, final.over_delivery_days,
                     final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)) custom_refund
     ON pay_order.region_code = custom_refund.region_code
         AND pay_order.datasource = custom_refund.datasource
         AND pay_order.platform = custom_refund.platform
         AND pay_order.threshold = custom_refund.threshold
         AND pay_order.action_date = custom_refund.action_date
         AND pay_order.is_first_refund = custom_refund.is_first_refund
         AND pay_order.over_delivery_days = custom_refund.over_delivery_days
         AND pay_order.storage_type = custom_refund.storage_type
         AND pay_order.shipping_status_note = custom_refund.shipping_status_note
         AND pay_order.is_new_activate = custom_refund.is_new_activate
         WHERE pay_order.action_date != 'all'
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
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
--conf "spark.sql.codegen.wholeStage=false" \
-e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
