#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
set hive.mapred.mode=nonstrict;
insert overwrite table rpt.rpt_mct_shield PARTITION (pt = '${cur_date}')
select 'vova'                          as datasource,
       '${cur_date}'                   as event_date,
       mct.mct_id,
       cat.first_cat_id,
       cat.first_cat_name,
       ao.total_order_3to6             as normal_order_3to6,
       ao1.threshold_normal_order_5to8 as threshold_normal_order_5to8,
       ao3.total_order_5to8            as normal_order_5to8,
       ao2.total_order_9to12           as normal_order_9to12,
       vo.valid_order_3to6             as valid_order_3to6,
       deo.delivered_order_5to8        as delivered_order_5to8,
       lr.logistic_refund_order_9to12,
       nlr.not_logistic_refund_order_5to8
       -- 3-6周总的订单
from dwd.dim_merchant mct
         cross join (select cat.first_cat_id, cat.first_cat_name
                     from dwd.dim_category cat
                     group by cat.first_cat_id, cat.first_cat_name) as cat
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as total_order_3to6
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                    where og.sku_pay_status > 1
                      and datediff('${cur_date}', date(og.confirm_time)) between 21 and 42
                      and og.confirm_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as ao
                   on ao.mct_id = mct.mct_id and cat.first_cat_id = ao.first_cat_id
    -- 5-8周总的订单阈值上
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as threshold_normal_order_5to8
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                             inner join dwd.fact_pay fp on og.order_goods_id = fp.order_goods_id
                    where og.sku_pay_status > 1
                      and datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
                      and og.confirm_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                      and fp.shipping_fee + fp.shop_price * fp.goods_number > 10
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as ao1
                   on mct.mct_id = ao1.mct_id and cat.first_cat_id = ao1.first_cat_id
    -- 5-8周总的订单
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as total_order_5to8
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                    where og.sku_pay_status > 1
                      and datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
                      and og.confirm_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as ao3
                   on mct.mct_id = ao3.mct_id and cat.first_cat_id = ao3.first_cat_id
    -- 9-12周总的订单
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as total_order_9to12
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                    where og.sku_pay_status > 1
                      and datediff('${cur_date}', date(og.confirm_time)) between 63 and 84
                      and og.confirm_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name) as ao2
                   on mct.mct_id = ao2.mct_id and cat.first_cat_id = ao2.first_cat_id
    -- 3-6周7天有效订单
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as valid_order_3to6
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                             inner join dwd.fact_logistics fl on fl.order_goods_id = og.order_goods_id
                    where datediff(fl.valid_tracking_date, date(fl.confirm_time)) <= 7
                      and datediff('${cur_date}', date(og.confirm_time)) between 21 and 42
                      and og.confirm_time > '2018-03-01'
                      and fl.valid_tracking_date > '2018-03-01'
                      and og.sku_pay_status > 1
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as vo
                   on vo.mct_id = mct.mct_id and vo.first_cat_id = cat.first_cat_id
    -- 5-8周物流妥投订单 阈值以上
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as delivered_order_5to8
                    from dwd.dim_order_goods og
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                             inner join dwd.fact_logistics fl on fl.order_goods_id = og.order_goods_id
                             inner join dwd.fact_pay fp on og.order_goods_id = fp.order_goods_id
                    where fl.process_tag = 'Delivered'
                      and datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
                      and fl.delivered_date > '2018-03-01'
                      and og.sku_shipping_status > 0
                      and fp.shipping_fee + fp.shop_price * fp.goods_number > 10
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as deo
                   on deo.mct_id = mct.mct_id and deo.first_cat_id = cat.first_cat_id
    -- 5-8周非物流退款订单
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as not_logistic_refund_order_5to8
                    from dwd.fact_refund fr
                             inner join dwd.dim_order_goods og on og.order_goods_id = fr.order_goods_id
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                    where og.sku_pay_status > 1
                      AND fr.refund_type_id = 2
                      and fr.refund_reason_type_id != 8
                      and datediff('${cur_date}', date(og.confirm_time)) between 35 and 56
                      and og.confirm_time > '2018-03-01'
                      and fr.exec_refund_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name
             ) as nlr
                   on mct.mct_id = nlr.mct_id and nlr.first_cat_id = cat.first_cat_id
    -- 9-12周物流退款订单
         left join (select og.mct_id,
                           cat.first_cat_id,
                           og.first_cat_name,
                           count(distinct og.order_goods_id) as logistic_refund_order_9to12
                    from dwd.fact_refund fr
                             inner join dwd.dim_order_goods og on og.order_goods_id = fr.order_goods_id
                             inner join dwd.dim_category cat on og.cat_id = cat.cat_id
                    where og.sku_pay_status > 1
                      AND fr.refund_type_id = 2
                      and fr.refund_reason_type_id = 8
                      and datediff('${cur_date}', date(og.confirm_time)) between 63 and 84
                      and fr.exec_refund_time > '2018-03-01'
                      and og.confirm_time > '2018-03-01'
                      and og.sku_shipping_status > 0
                    group by og.mct_id, cat.first_cat_id, og.first_cat_name) as lr
                   on mct.mct_id = lr.mct_id and lr.first_cat_id = cat.first_cat_id;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=merchant_shield" \
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


hadoop fs -rm -r  s3://vova-bd-prod/warehouse/rpt/rpt_mct_shield/
hadoop distcp  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_mct_shield/ s3://vova-bd-prod/warehouse/rpt/
if [ $? -ne 0 ];then
  exit 1
fi