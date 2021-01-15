#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_refund_report_req_chenkai_${cur_date}"

###逻辑sql
sql="
REFRESH table dwb.dwb_vova_refund_report_detail;
insert overwrite table dwb.dwb_vova_refund_report  PARTITION (pt='${cur_date}')
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       'all' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       '除活动订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and order_tag is null
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       'brand订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and brand_id > 0
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       'flashsale订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and order_goods_tag = '[is_flash_sale]'
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       'freesale订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and order_tag = '[is_free_sale]'
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       '夺宝订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and order_tag = '[luckystar_activity_id]'
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
UNION
SELECT
       nvl(final.action_date, 'all')           AS action_date,
       nvl(final.region_code, 'all')           AS region_code,
       '拍卖订单' as activity,
       nvl(final.platform, 'all')              AS platform,
       nvl(final.threshold, 'all')             AS threshold,
       nvl(final.is_first_refund, 'all')        AS is_first_refund,
       nvl(final.over_delivery_days, 'all')    AS over_delivery_days,
       nvl(final.shipping_status_note, 'all')    AS shipping_status_note,
       nvl(final.storage_type, 'all')          AS storage_type,
       nvl(final.datasource, 'all')            AS datasource,
       nvl(final.is_new_activate, 'all')            AS is_new_activate,
       count(DISTINCT buyer_id)       AS user_number,
       count(DISTINCT order_goods_id) AS order_goods_number,
       SUM(refund_amount) AS refund_amount,
       count(DISTINCT refund_reason_order_1) AS refund_reason_order_1,
       count(DISTINCT refund_reason_order_2) AS refund_reason_order_2,
       count(DISTINCT refund_reason_order_3) AS refund_reason_order_3,
       count(DISTINCT refund_reason_order_4) AS refund_reason_order_4,
       count(DISTINCT refund_reason_order_5) AS refund_reason_order_5,
       count(DISTINCT refund_reason_order_6) AS refund_reason_order_6,
       count(DISTINCT refund_reason_order_7) AS refund_reason_order_7,
       count(DISTINCT refund_reason_order_8) AS refund_reason_order_8,
       count(DISTINCT refund_reason_order_9) AS refund_reason_order_9,
       count(DISTINCT refund_reason_order_10) AS refund_reason_order_10,
       count(DISTINCT refund_reason_order_11) AS refund_reason_order_11,
       count(DISTINCT refund_reason_order_12) AS refund_reason_order_12,
       count(DISTINCT refund_reason_order_13) AS refund_reason_order_13,
       count(DISTINCT customer_reason_order_1) AS customer_reason_order_1,
       count(DISTINCT customer_reason_order_2) AS customer_reason_order_2,
       count(DISTINCT customer_reason_order_3) AS customer_reason_order_3,
       count(DISTINCT customer_reason_order_4) AS customer_reason_order_4,
       count(DISTINCT customer_reason_order_5) AS customer_reason_order_5,
       count(DISTINCT customer_reason_order_6) AS customer_reason_order_6,
       count(DISTINCT customer_reason_order_7) AS customer_reason_order_7,
       count(DISTINCT customer_reason_order_8) AS customer_reason_order_8,
       count(DISTINCT customer_reason_order_9) AS customer_reason_order_9,
       count(DISTINCT customer_reason_order_10) AS customer_reason_order_10,
       SUM(refund_reason_gmv_1) AS refund_reason_gmv_1,
       SUM(refund_reason_gmv_2) AS refund_reason_gmv_2,
       SUM(refund_reason_gmv_3) AS refund_reason_gmv_3,
       SUM(refund_reason_gmv_4) AS refund_reason_gmv_4,
       SUM(refund_reason_gmv_5) AS refund_reason_gmv_5,
       SUM(refund_reason_gmv_6) AS refund_reason_gmv_6,
       SUM(refund_reason_gmv_7) AS refund_reason_gmv_7,
       SUM(refund_reason_gmv_8) AS refund_reason_gmv_8,
       SUM(refund_reason_gmv_9) AS refund_reason_gmv_9,
       SUM(refund_reason_gmv_10) AS refund_reason_gmv_10,
       SUM(refund_reason_gmv_11) AS refund_reason_gmv_11,
       SUM(refund_reason_gmv_12) AS refund_reason_gmv_12,
       SUM(refund_reason_gmv_13) AS refund_reason_gmv_13,
       SUM(customer_reason_gmv_1) AS customer_reason_gmv_1,
       SUM(customer_reason_gmv_2) AS customer_reason_gmv_2,
       SUM(customer_reason_gmv_3) AS customer_reason_gmv_3,
       SUM(customer_reason_gmv_4) AS customer_reason_gmv_4,
       SUM(customer_reason_gmv_5) AS customer_reason_gmv_5,
       SUM(customer_reason_gmv_6) AS customer_reason_gmv_6,
       SUM(customer_reason_gmv_7) AS customer_reason_gmv_7,
       SUM(customer_reason_gmv_8) AS customer_reason_gmv_8,
       SUM(customer_reason_gmv_9) AS customer_reason_gmv_9,
       SUM(customer_reason_gmv_10) AS customer_reason_gmv_10
FROM (
         SELECT nvl(temp.region_code, 'NONE')               AS region_code,
                nvl(temp.platform, 'NA')                      AS platform,
                nvl(temp.threshold, 'NA')                     AS threshold,
                nvl(temp.action_date, 'NA')                   AS action_date,
                nvl(temp.is_first_refund, 'N')                AS is_first_refund,
                nvl(temp.storage_type, 'N')                        AS storage_type,
                nvl(temp.datasource, 'NA')                    AS datasource,
                nvl(temp.shipping_status_note, 'ERROR')                    AS shipping_status_note,
                nvl(is_new_activate,'N') as is_new_activate,
                CASE
                    WHEN final_delivery_time IS NULL
                        THEN '没有交期'
                    WHEN ddiff IS NULL
                        THEN '异常'
                    WHEN ddiff < 0
                        THEN '交期内'
                    WHEN ddiff < 1
                        THEN '超交期1日内'
                    WHEN ddiff < 2
                        THEN '超交期2日内'
                    WHEN ddiff < 3
                        THEN '超交期3日内'
                    WHEN ddiff < 7
                        THEN '超交期7日内'
                    WHEN ddiff < 14
                        THEN '超交期14日内'
                    WHEN ddiff < 30
                        THEN '超交期30日内'
                    WHEN ddiff < 45
                        THEN '超交期45日内'
                    ELSE '异常'
                    END                                       AS over_delivery_days,
                if(refund_type_id = 1, order_goods_id, NULL)       AS refund_reason_order_1,
                if(refund_type_id = 2, order_goods_id, NULL)       AS refund_reason_order_2,
                if(refund_type_id = 3, order_goods_id, NULL)       AS refund_reason_order_3,
                if(refund_type_id = 4, order_goods_id, NULL)       AS refund_reason_order_4,
                if(refund_type_id = 5, order_goods_id, NULL)       AS refund_reason_order_5,
                if(refund_type_id = 6, order_goods_id, NULL)       AS refund_reason_order_6,
                if(refund_type_id = 7, order_goods_id, NULL)       AS refund_reason_order_7,
                if(refund_type_id = 8, order_goods_id, NULL)       AS refund_reason_order_8,
                if(refund_type_id = 9, order_goods_id, NULL)       AS refund_reason_order_9,
                if(refund_type_id = 10, order_goods_id, NULL)       AS refund_reason_order_10,
                if(refund_type_id = 11, order_goods_id, NULL)       AS refund_reason_order_11,
                if(refund_type_id = 12, order_goods_id, NULL)       AS refund_reason_order_12,
                if(refund_type_id = 13, order_goods_id, NULL)       AS refund_reason_order_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, order_goods_id, NULL)       AS customer_reason_order_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, order_goods_id, NULL)       AS customer_reason_order_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, order_goods_id, NULL)       AS customer_reason_order_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, order_goods_id, NULL)       AS customer_reason_order_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, order_goods_id, NULL)       AS customer_reason_order_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, order_goods_id, NULL)       AS customer_reason_order_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, order_goods_id, NULL)       AS customer_reason_order_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, order_goods_id, NULL)       AS customer_reason_order_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, order_goods_id, NULL)       AS customer_reason_order_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, order_goods_id, NULL)       AS customer_reason_order_10,
                if(refund_type_id = 1, refund_amount, 0)       AS refund_reason_gmv_1,
                if(refund_type_id = 2, refund_amount, 0)       AS refund_reason_gmv_2,
                if(refund_type_id = 3, refund_amount, 0)       AS refund_reason_gmv_3,
                if(refund_type_id = 4, refund_amount, 0)       AS refund_reason_gmv_4,
                if(refund_type_id = 5, refund_amount, 0)       AS refund_reason_gmv_5,
                if(refund_type_id = 6, refund_amount, 0)       AS refund_reason_gmv_6,
                if(refund_type_id = 7, refund_amount, 0)       AS refund_reason_gmv_7,
                if(refund_type_id = 8, refund_amount, 0)       AS refund_reason_gmv_8,
                if(refund_type_id = 9, refund_amount, 0)       AS refund_reason_gmv_9,
                if(refund_type_id = 10, refund_amount, 0)       AS refund_reason_gmv_10,
                if(refund_type_id = 11, refund_amount, 0)       AS refund_reason_gmv_11,
                if(refund_type_id = 12, refund_amount, 0)       AS refund_reason_gmv_12,
                if(refund_type_id = 13, refund_amount, 0)       AS refund_reason_gmv_13,
                if(refund_type_id = 2 AND refund_reason_type_id = 1, refund_amount, 0)       AS customer_reason_gmv_1,
                if(refund_type_id = 2 AND refund_reason_type_id = 2, refund_amount, 0)       AS customer_reason_gmv_2,
                if(refund_type_id = 2 AND refund_reason_type_id = 3, refund_amount, 0)       AS customer_reason_gmv_3,
                if(refund_type_id = 2 AND refund_reason_type_id = 4, refund_amount, 0)       AS customer_reason_gmv_4,
                if(refund_type_id = 2 AND refund_reason_type_id = 5, refund_amount, 0)       AS customer_reason_gmv_5,
                if(refund_type_id = 2 AND refund_reason_type_id = 6, refund_amount, 0)       AS customer_reason_gmv_6,
                if(refund_type_id = 2 AND refund_reason_type_id = 7, refund_amount, 0)       AS customer_reason_gmv_7,
                if(refund_type_id = 2 AND refund_reason_type_id = 8, refund_amount, 0)       AS customer_reason_gmv_8,
                if(refund_type_id = 2 AND refund_reason_type_id = 9, refund_amount, 0)       AS customer_reason_gmv_9,
                if(refund_type_id = 2 AND refund_reason_type_id = 10, refund_amount, 0)       AS customer_reason_gmv_10,
                order_goods_id,
                buyer_id,
                refund_amount
         FROM (
                  SELECT IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
                         datediff(create_time, final_delivery_time)                             AS ddiff,
                         date(create_time)                                                      AS action_date,
                         if(first_refund_time = create_time,'Y','N') AS is_first_refund,
                         CASE sku_shipping_status
                         WHEN 0 THEN 'PROCESSING'
                         WHEN 1 THEN 'SHIPPED'
                         WHEN 2 THEN 'RECEIVED'
                         ELSE 'ERROR' END shipping_status_note,
                         if(storage_type = 2, 'Y', 'N')                          AS storage_type,
                         buyer_id,
                         order_goods_id,
                         region_code,
                         platform,
                         datasource,
                         refund_reason_type_id,
                         refund_type_id,
                         refund_amount,
                         receive_time,
                         final_delivery_time,
                         is_new_activate
                  FROM dwb.dwb_vova_refund_report_detail
                  WHERE create_time IS NOT NULL
                  and date(create_time) = '${cur_date}'
                  and order_tag = '[auction_activity_id]'
              ) AS temp) final
GROUP BY CUBE(final.region_code, final.platform, final.threshold,
              final.action_date, final.is_first_refund, final.over_delivery_days,
              final.datasource, final.storage_type, final.shipping_status_note, final.is_new_activate)
HAVING action_date != 'all'
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=${job_name}" \
--conf "spark.default.parallelism=380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.codegen=true" \
-e "$sql"
#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# # https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html
# sqoop export \
# -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
# -Dmapreduce.job.queuename=default \
# --connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
# --username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS --connection-manager org.apache.sqoop.manager.MySQLManager \
# --table rpt_refund_report \
# --update-key "action_date,platform,region_code,threshold,is_first_refund,over_delivery_days,datasource,storage_type,is_new_activate,shipping_status_note,activity" \
# --update-mode allowinsert \
# --hcatalog-database rpt \
# --hcatalog-table rpt_refund_report \
# --hcatalog-partition-keys pt \
# --hcatalog-partition-values ${cur_date} \
# --fields-terminated-by '\001'
#
# #如果脚本失败，则报错
# if [ $? -ne 0 ];then
#   exit 1
# fi