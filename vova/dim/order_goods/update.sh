#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_vova_order_goods
SELECT CASE
           WHEN oi.from_domain LIKE '%vova%' THEN 'vova'
           WHEN oi.from_domain LIKE '%airyclub%' THEN 'airyclub'
           ELSE 'NA'
           END                                                                   AS datasource,
       CASE
           WHEN oi.from_domain LIKE '%api%' THEN 'app'
           ELSE 'web'
           END                                                                   AS order_source,
       CASE
           WHEN ore.device_type IN (0, 23, 24, 25) THEN 'pc'
           WHEN ore.device_type IN (21, 22, 26) THEN 'mob'
           WHEN ore.device_type = 11 THEN 'ios'
           WHEN ore.device_type = 12 THEN 'android'
           ELSE 'unknown'
           END                                                                   AS platform,
       ore.device_id,
       oi.from_domain,
       oi.user_id                                                                AS buyer_id,
       oi.order_id,
       oi.order_sn,
       oi.coupon_code,
       oi.gender,
       oi.email,
       oi.parent_order_id,
       oi.payment_id,
       oi.payment_name,
       oi.order_currency_id,
       oi.base_currency_id,
       oi.order_time,
       oi.pay_time,
       oi.receive_time,
       oi.pay_status,
       r.region_id,
       r.region_code,
       og.rec_id                                                                 AS order_goods_id,
       og.order_goods_sn,
       og.parent_rec_id,
       og.goods_id,
       og.goods_sn,
       og.goods_name,
       og.sku_id,
       og.goods_number,
       og.shop_price,
       og.shipping_fee,
       og.goods_weight,
       og.bonus,
       og.mct_shop_price,
       og.mct_shipping_fee,
       ogs.sku_order_status,
       ogs.sku_pay_status,
       ogs.sku_shipping_status,
       ogs.sku_collecting_status,
       ogs.confirm_time,
       ogs.shipping_time,
       ogs.collecting_time,
       g.cat_id,
       g.virtual_goods_id,
       g.first_cat_name,
       g.second_cat_name,
       g.brand_id,
       g.mct_id,
       ot.order_tag                                                              AS order_tag,
       temp_order_goods_extension_tag.order_goods_tag                            AS order_goods_tag,
       from_unixtime(int(temp_order_extension.ext_value))                        AS delivery_time,
       date_add(oi.receive_time, int(temp_order_goods_extension.extension_info)) AS delivery_time_max,
       if(ogex.storage_type != 2, 'not_fbv', 'is_fbv')                        AS lgst_way,
       ogex.collection_plan_id,
       temp_transportation_shipping_fee.container_transportation_shipping_fee AS container_transportation_shipping_fee
FROM ods_vova_vts.ods_vova_order_goods og
         LEFT JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = og.order_id
         LEFT JOIN dim.dim_vova_goods g ON g.goods_id = og.goods_id
         LEFT JOIN ods_vova_vts.ods_vova_order_goods_status ogs ON ogs.order_goods_id = og.rec_id
         LEFT JOIN ods_vova_vts.ods_vova_order_relation ore ON ore.order_id = oi.order_id
         LEFT JOIN ods_vova_vts.ods_vova_region r ON r.region_id = oi.country
         LEFT JOIN (SELECT collect_set(oe.ext_name) AS order_tag,
                           oe.order_id
                    FROM ods_vova_vts.ods_vova_order_extension oe
                    WHERE oe.ext_name IN
                          ('daily_gift_activity_id', 'is_free_sale', 'affiliate_activity_id', 'luckystar_activity_id',
                           'auction_activity_id')
                    GROUP BY oe.order_id) ot ON ot.order_id = og.order_id
         LEFT JOIN (SELECT collect_set(oge.ext_name) AS order_goods_tag,
                           oge.rec_id
                    FROM ods_vova_vts.ods_vova_order_goods_extension oge
                    WHERE oge.ext_name IN ('is_flash_sale', 'ranking_list_id')
                    GROUP BY oge.rec_id) temp_order_goods_extension_tag
                   ON temp_order_goods_extension_tag.rec_id = og.rec_id
         LEFT JOIN (SELECT oge.rec_id, collect_set(oge.extension_info)[0] AS extension_info
                    FROM ods_vova_vts.ods_vova_order_goods_extension oge
                    WHERE oge.ext_name = 'max'
                    GROUP BY oge.rec_id) temp_order_goods_extension ON temp_order_goods_extension.rec_id = og.rec_id
         LEFT JOIN (SELECT oe.order_id, collect_set(oe.ext_value)[0] AS ext_value
                    FROM ods_vova_vts.ods_vova_order_extension oe
                    WHERE oe.ext_name = 'delivery_time'
                    GROUP BY oe.order_id) temp_order_extension ON temp_order_extension.order_id = og.order_id
         LEFT JOIN ods_vova_vts.ods_vova_order_goods_extra ogex ON ogex.order_goods_id = og.rec_id AND ogex.collection_plan_id IN (1, 2)
         LEFT JOIN (
         SELECT oget.rec_id,
                first(oget.extension_info) as container_transportation_shipping_fee
                FROM ods_vova_vts.ods_vova_order_goods_extension oget
                WHERE oget.ext_name = 'container_transportation_shipping_fee'
                group by oget.rec_id
         ) temp_transportation_shipping_fee ON temp_transportation_shipping_fee.rec_id = og.rec_id
WHERE oi.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_order_goods" --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=40" -e "$sql"
#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi
