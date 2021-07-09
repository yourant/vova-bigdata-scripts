#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_pay_refund_detail_req_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table dwb.dwb_vova_pay_refund_detail
SELECT /*+ REPARTITION(10) */
       pay_time,
       create_time,
       refund_id,
       order_goods_id,
       refund_type_id,
       refund_type,
       refund_reason_type_id,
       refund_reason,
       refund_amount,
       bonus,
       exec_refund_time,
       sku_shipping_status,
       order_tag,
       order_goods_tag,
       region_code,
       platform,
       threshold_amount,
       final_delivery_time,
       receive_time,
       first_refund_time,
       buyer_id,
       gmv,
       brand_id,
       storage_type,
       datasource,
       is_new_activate,
       IF(threshold_amount >= 10, 'above_the_threshold', 'below_the_threshold') AS threshold,
       if(first_refund_time = create_time, 'Y', 'N')                            AS is_first_refund,
       CASE sku_shipping_status
           WHEN 0 THEN 'PROCESSING'
           WHEN 1 THEN 'SHIPPED'
           WHEN 2 THEN 'RECEIVED'
           ELSE 'ERROR' END                                                        shipping_status_note,
       CASE
           WHEN create_time IS NULL
               THEN '未退款订单'
           WHEN receive_time IS NULL
               THEN '没有财务收款时间'
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
           END                                                                  AS over_delivery_days
FROM (
         SELECT fp.pay_time,
                fr.create_time,
                fr.refund_id,
                fp.order_goods_id,
                fr.refund_type_id,
                fr.refund_type,
                fr.refund_reason_type_id,
                fr.refund_reason,
                fr.refund_amount,
                fr.bonus,
                fr.exec_refund_time,
                dog.sku_shipping_status,
                dog.order_tag,
                dog.order_goods_tag,
                dog.region_code,
                dog.platform,
                og.mct_shop_price_amount + og.mct_shipping_fee                                                        AS threshold_amount,
                if(dog.delivery_time_max IS NULL, dog.delivery_time,
                   dog.delivery_time_max)                                                                             AS final_delivery_time,
                dog.receive_time,
                dbu.first_refund_time,
                dog.buyer_id,
                og.shop_price_amount + og.shipping_fee                                                                AS gmv,
                g.brand_id,
                if(oge.storage_type = 2, 'Y', 'N')                                           AS storage_type,
                dog.datasource,
                nvl(if(date(dd.activate_time) = date(fp.pay_time), 'Y', 'N'),
                    'N')                                                                                              AS is_new_activate,
                datediff(fr.create_time, if(dog.delivery_time_max IS NULL, dog.delivery_time,
                                            dog.delivery_time_max))                                                   AS ddiff
         FROM dwd.dwd_vova_fact_pay fp
                  LEFT JOIN dwd.dwd_vova_fact_refund fr ON fr.order_goods_id = fp.order_goods_id AND fr.sku_pay_status = 4
                  INNER JOIN dim.dim_vova_order_goods dog ON dog.order_goods_id = fp.order_goods_id
                  LEFT JOIN ods_vova_vts.ods_vova_order_goods og ON og.rec_id = fp.order_goods_id
                  LEFT JOIN ods_vova_vts.ods_vova_goods g ON g.goods_id = dog.goods_id
                  LEFT JOIN dim.dim_vova_buyers dbu ON dbu.buyer_id = dog.buyer_id
                  LEFT JOIN ods_vova_vts.ods_vova_order_goods_extra oge ON oge.order_goods_id = dog.order_goods_id
                  LEFT JOIN dim.dim_vova_devices dd ON dd.device_id = dog.device_id AND dd.datasource = dog.datasource
         WHERE fp.pay_time >= date_sub('2019-12-05', 90)
     ) temp
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.dynamicAllocation.initialExecutors=40" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
