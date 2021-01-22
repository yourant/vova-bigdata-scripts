#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#rpt_self
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
INSERT OVERWRITE TABLE dwb.dwb_vova_self_merchant PARTITION (pt)
SELECT          /*+ REPARTITION(1) */
                nvl(date(pay_time), 'all')                          AS pay_date,
                nvl(nvl(datasource, 'NA'), 'all')                      AS datasource,
                nvl(nvl(region_code, 'NALL'), 'all')                   AS region_code,
                nvl(nvl(platform, 'NA'), 'all')                        AS platform,
                nvl(nvl(first_cat_name, 'NA'), 'all')                  AS first_cat_name,
                sum(gmv) AS gmv,
                sum(purchase_total_amount) AS purchase_total_amount,
                count(DISTINCT purchase_order_goods_id_order_0) AS purchase_order_goods_id_order_0,
                count(DISTINCT purchase_order_goods_id_order_1) AS purchase_order_goods_id_order_1,
                count(DISTINCT purchase_order_goods_id_order_2) AS purchase_order_goods_id_order_0,
                count(DISTINCT purchase_order_goods_id_order_4) AS purchase_order_goods_id_order_4,
                count(DISTINCT purchase_order_goods_id_pay_0) AS purchase_order_goods_id_pay_0,
                count(DISTINCT purchase_order_goods_id_pay_1) AS purchase_order_goods_id_pay_1,
                count(DISTINCT purchase_order_goods_id_ship_0) AS purchase_order_goods_id_ship_0,
                count(DISTINCT purchase_order_goods_id_ship_1) AS purchase_order_goods_id_ship_1,
                count(DISTINCT collect_0) AS collect_0,
                count(DISTINCT collect_1) AS collect_1,
                count(DISTINCT collect_2) AS collect_2,
                count(DISTINCT order_goods_id) AS order_goods_cnt,
                count(DISTINCT purchase_order_goods_id) AS purchase_order_goods_cnt,
                nvl(self_mct_name, 'all')                          AS self_mct_name,
                nvl(if(brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
                nvl(date(pay_time), 'all') AS pt
                from
(
         SELECT fp.datasource,
         fp.region_code,
         fp.platform,
         fp.first_cat_name,
         fp.pay_time,
         fp.shop_price * fp.goods_number + fp.shipping_fee as gmv,
         fp.order_goods_id,
         dg.brand_id,
         CASE when fp.mct_id = 26414 THEN 'Airyclub'
         when fp.mct_id = 11630 THEN 'SuperAC'
         when fp.mct_id = 36655 THEN 'VogueFD'
         when fp.mct_id = 61017 THEN 'dearbuys'
         when fp.mct_id = 61028 THEN 'shejoys'
         when fp.mct_id = 61235 THEN 'vvshein'
         when fp.mct_id = 61310 THEN 'SuperEC'
         else 'NA' end as self_mct_name,
         null purchase_order_goods_id,
         null as purchase_order_goods_id_order_0,
         null as purchase_order_goods_id_order_1,
         null as purchase_order_goods_id_order_2,
         null as purchase_order_goods_id_order_4,
         null as purchase_order_goods_id_pay_0,
         null as purchase_order_goods_id_pay_1,
         0 as purchase_total_amount,
         null as purchase_order_goods_id_ship_0,
         null as purchase_order_goods_id_ship_1,
         if(oge.collection_plan_id = 2 and ogs.sku_shipping_status=1 and ostd.weight_record_time = '',fp.order_goods_id,null) as collect_0,
         if(oge.collection_plan_id = 2 and ogs.sku_shipping_status=1 and ostd.weight_record_time != '' and valid_tracking_date < '2018-01-01',fp.order_goods_id,null) as collect_1,
         if(oge.collection_plan_id = 2 and ogs.sku_shipping_status>=1 and ostd.weight_record_time != '' and valid_tracking_date > '2018-01-01',fp.order_goods_id,null) as collect_2

         FROM dwd.dwd_vova_fact_pay fp
                  inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
                  left join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = fp.order_goods_id
                  left join ods_vova_vts.ods_vova_order_goods_extra oge on oge.order_goods_id = fp.order_goods_id
                  left join ods_vova_vts.ods_vova_order_shipping_tracking ost on ost.order_goods_id = fp.order_goods_id
                  left join ods_vova_vts.ods_vova_order_shipping_tracking_detail ostd on ost.tracking_id = ostd.tracking_id
                  -- LEFT JOIN ods_vova_vts.ods_vova_sales_order_goods sog ON sog.order_goods_sn = fp.order_goods_sn
                  -- LEFT JOIN ods_vova_vts.ods_vova_purchase_order_goods pog ON pog.purchase_order_goods_id = sog.purchase_order_goods_id
                  -- LEFT JOIN ods_vova_vts.ods_vova_purchase_order_goods_status pogs ON pogs.purchase_order_goods_id = pog.purchase_order_goods_id
         WHERE date(fp.pay_time) <= '${cur_date}'
           AND date(fp.pay_time) >= date_sub('${cur_date}', 30)
           AND fp.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
     ) paid_data
              GROUP BY CUBE (nvl(datasource, 'NA'), nvl(region_code, 'NALL'), nvl(platform, 'NA'), nvl(first_cat_name, 'NA'),
                        date(pay_time), self_mct_name, if(brand_id >0 ,'Y', 'N'))
HAVING pay_date != 'all';
"


spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_self_merchant" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



