#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"
###逻辑sql
#优惠券使用
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

with tmp_use_num as (
select
    t1.datasource,
    t1.region_code,
    t1.pay_date,
    dc.cpn_cfg_type,
    dc.cpn_cfg_type_id,
    dc.currency,
    oi.order_id,
    oi.bonus,
    oi.user_id,
    oi.goods_amount,
    oi.shipping_fee,
    oi.coupon_code
FROM (
         SELECT first (fp.datasource) AS datasource,
             first (fp.region_code) AS region_code,
             fp.order_id,
             date (fp.pay_time) AS pay_date
         FROM dwd.dwd_vova_fact_pay fp
         WHERE date (fp.pay_time) >= '${cur_date}'
           and date (fp.pay_time) <= date_add('${cur_date}', 31)
         GROUP BY fp.order_id, date (fp.pay_time)
     ) t1
         inner JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = t1.order_id
         LEFT JOIN dim.dim_vova_coupon dc ON dc.cpn_code = oi.coupon_code
),
tmp_use_num_30 as (
select '${cur_date}'               AS event_date,
       nvl(nvl(t1.region_code, 'NA'), 'all') AS region_code,
       nvl(nvl(t1.datasource, 'NA'), 'all')  AS datasource,
    first (t1.cpn_cfg_type) AS cpn_cfg_type,
    nvl(nvl(t1.cpn_cfg_type_id, '-1'), 'all') AS cpn_cfg_type_id,
    nvl(nvl(t1.currency, 'NA'), 'all') AS currency,
    0 AS give_num,
    0 AS give_amount,
    0 AS give_user,
    0 AS use_num,
    0 AS use_amount,
    0 AS use_user,
    0 AS gmv,
    0 AS use_num_3,
    0 AS use_num_7,
    0 AS use_num_15,
    count (distinct order_id) AS use_num_30
from tmp_use_num t1
where pay_date >= '${cur_date}'
  and pay_date <= date_add('${cur_date}'
    , 29)
and t1.coupon_code != ''
GROUP BY CUBE ( nvl(t1.region_code, 'NA'), nvl(t1.datasource, 'NA'), nvl(t1.cpn_cfg_type_id, '-1'), nvl(t1.currency, 'NA'))
HAVING cpn_cfg_type_id != 'all' AND currency != 'all'
),
tmp_use_num_15 as (
select '${cur_date}'               AS event_date,
       nvl(nvl(t1.region_code, 'NA'), 'all') AS region_code,
       nvl(nvl(t1.datasource, 'NA'), 'all')  AS datasource,
    first (t1.cpn_cfg_type) AS cpn_cfg_type,
    nvl(nvl(t1.cpn_cfg_type_id, '-1'), 'all') AS cpn_cfg_type_id,
    nvl(nvl(t1.currency, 'NA'), 'all') AS currency,
    0 AS give_num,
    0 AS give_amount,
    0 AS give_user,
    0 AS use_num,
    0 AS use_amount,
    0 AS use_user,
    0 AS gmv,
    0 AS use_num_3,
    0 AS use_num_7,
    count (distinct order_id) AS use_num_15,
    0 AS use_num_30
from tmp_use_num t1
where pay_date >= '${cur_date}'
  and pay_date <= date_add('${cur_date}'
    , 14)
and t1.coupon_code != ''
GROUP BY CUBE (nvl(t1.region_code, 'NA'), nvl(t1.datasource, 'NA'), nvl(t1.cpn_cfg_type_id, '-1'), nvl(t1.currency, 'NA'))
HAVING cpn_cfg_type_id != 'all' AND currency != 'all'
),
tmp_use_num_7 as (
select '${cur_date}'               AS event_date,
       nvl(nvl(t1.region_code, 'NA'), 'all') AS region_code,
       nvl(nvl(t1.datasource, 'NA'), 'all')  AS datasource,
    first (t1.cpn_cfg_type) AS cpn_cfg_type,
    nvl(nvl(t1.cpn_cfg_type_id, '-1'), 'all') AS cpn_cfg_type_id,
    nvl(nvl(t1.currency, 'NA'), 'all') AS currency,
    0 AS give_num,
    0 AS give_amount,
    0 AS give_user,
    0 AS use_num,
    0 AS use_amount,
    0 AS use_user,
    0 AS gmv,
    0 AS use_num_3,
    count (distinct order_id) AS use_num_7,
    0 AS use_num_15,
    0 AS use_num_30
from tmp_use_num t1
where pay_date >= '${cur_date}'
  and pay_date <= date_add('${cur_date}'
    , 6)
and t1.coupon_code != ''
GROUP BY CUBE (nvl(t1.region_code, 'NA'), nvl(t1.datasource, 'NA'), nvl(t1.cpn_cfg_type_id, '-1'), nvl(t1.currency, 'NA'))
HAVING cpn_cfg_type_id != 'all' AND currency != 'all'
),
tmp_use_num_3 as (
select '${cur_date}'               AS event_date,
       nvl(nvl(t1.region_code, 'NA'), 'all') AS region_code,
       nvl(nvl(t1.datasource, 'NA'), 'all')  AS datasource,
    first (t1.cpn_cfg_type) AS cpn_cfg_type,
    nvl(nvl(t1.cpn_cfg_type_id, '-1'), 'all') AS cpn_cfg_type_id,
    nvl(nvl(t1.currency, 'NA'), 'all') AS currency,
    0 AS give_num,
    0 AS give_amount,
    0 AS give_user,
    0 AS use_num,
    0 AS use_amount,
    0 AS use_user,
    0 AS gmv,
    count (distinct order_id) AS use_num_3,
    0 AS use_num_7,
    0 AS use_num_15,
    0 AS use_num_30
from tmp_use_num t1
where pay_date >= '${cur_date}'
  and pay_date <= date_add('${cur_date}'
    , 2)
and t1.coupon_code != ''
GROUP BY CUBE (nvl(t1.region_code, 'NA'), nvl(t1.datasource, 'NA'), nvl(t1.cpn_cfg_type_id, '-1'), nvl(t1.currency, 'NA'))
HAVING cpn_cfg_type_id != 'all' AND currency != 'all'
)

insert overwrite table dwb.dwb_vova_coupon PARTITION (pt)
SELECT  /*+ REPARTITION(10) */
       result.event_date,
       result.datasource,
       result.region_code,
       result.cpn_cfg_type,
       result.cpn_cfg_type_id,
       occt.config_type_name,
       result.currency,
       result.give_num,
       result.give_amount,
       result.give_user,
       result.use_num,
       result.use_amount,
       result.use_user,
       result.gmv,
       result.use_num_3,
       result.use_num_7,
       result.use_num_15,
       result.use_num_30,
       result.event_date AS pt
FROM (
         SELECT temp1.event_date,
                temp1.datasource,
                temp1.region_code,
                temp1.cpn_cfg_type_id,
             first (temp1.cpn_cfg_type) as cpn_cfg_type,
             temp1.currency,
             sum (give_num) AS give_num,
             sum (give_amount) AS give_amount,
             sum (give_user) AS give_user,
             sum (use_num) AS use_num,
             sum (use_amount) AS use_amount,
             sum (use_user) AS use_user,
             sum (gmv) AS gmv,
             sum (use_num_3) AS use_num_3,
             sum (use_num_7) AS use_num_7,
             sum (use_num_15) AS use_num_15,
             sum (use_num_30) AS use_num_30
         FROM (
             SELECT nvl(final.cpn_create_date, 'all') AS event_date,
             nvl(final.region_code, 'all') AS region_code,
             nvl(final.datasource, 'all') AS datasource,
             first (final.cpn_cfg_type) AS cpn_cfg_type,
             nvl(final.cpn_cfg_type_id, 'all') AS cpn_cfg_type_id,
             nvl(final.currency, 'all') AS currency,
             count (cpn_id) AS give_num,
             sum (cpn_cfg_val) AS give_amount,
             count (DISTINCT buyer_id) AS give_user,
             0 AS use_num,
             0 AS use_amount,
             0 AS use_user,
             0 AS gmv,
             0 AS use_num_3,
             0 AS use_num_7,
             0 AS use_num_15,
             0 AS use_num_30
             FROM (
             SELECT date (dc.cpn_create_time) AS cpn_create_date,
             nvl(dc.cpn_cfg_type_id, '-1') AS cpn_cfg_type_id,
             dc.cpn_cfg_type,
             nvl(byr.region_code, 'NA') AS region_code,
             nvl(byr.datasource, 'NA') AS datasource,
             nvl(dc.currency, 'NA') AS currency,
             dc.cpn_id,
             nvl(dc.cpn_cfg_val, 0) AS cpn_cfg_val,
             nvl(dc.buyer_id, 0) AS buyer_id
             FROM dim.dim_vova_coupon dc
             INNER JOIN dim.dim_vova_buyers byr ON byr.buyer_id = dc.buyer_id
             WHERE date (dc.cpn_create_time) = '${cur_date}'
             ) final
             GROUP BY CUBE (final.cpn_create_date, final.cpn_cfg_type_id, final.region_code, final.datasource, final.currency)
             HAVING event_date != 'all' AND cpn_cfg_type_id != 'all' AND currency != 'all'
             UNION ALL
             SELECT nvl(t1.pay_date, 'all') AS event_date,
             nvl(nvl(t1.region_code, 'NA'), 'all') AS region_code,
             nvl(nvl(t1.datasource, 'NA'), 'all') AS datasource,
             first (dc.cpn_cfg_type) AS cpn_cfg_type,
             nvl(nvl(dc.cpn_cfg_type_id, '-1'), 'all') AS cpn_cfg_type_id,
             nvl(nvl(dc.currency, 'NA'), 'all') AS currency,
             0 AS give_num,
             0 AS give_amount,
             0 AS give_user,
             COUNT (DISTINCT oi.order_id) AS use_num,
             sum (oi.bonus) AS use_amount,
             COUNT (DISTINCT oi.user_id) AS use_user,
             sum (oi.goods_amount + oi.shipping_fee) AS gmv,
             0 AS use_num_3,
             0 AS use_num_7,
             0 AS use_num_15,
             0 AS use_num_30
             FROM (
             SELECT first (fp.datasource) AS datasource,
             first (fp.region_code) AS region_code,
             fp.order_id,
             date (fp.pay_time) AS pay_date
             FROM dwd.dwd_vova_fact_pay fp
             WHERE date (fp.pay_time) = '${cur_date}'
             GROUP BY fp.order_id, date (fp.pay_time)
             ) t1
             INNER JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = t1.order_id
             LEFT JOIN dim.dim_vova_coupon dc ON dc.cpn_code = oi.coupon_code
             WHERE oi.coupon_code != ''
             GROUP BY CUBE (t1.pay_date, nvl(t1.region_code, 'NA'), nvl(t1.datasource, 'NA'), nvl(dc.cpn_cfg_type_id, '-1'), nvl(dc.currency, 'NA'))
             HAVING event_date != 'all' AND cpn_cfg_type_id != 'all' AND currency != 'all'
             UNION ALL
             select * from tmp_use_num_3
             UNION ALL
             select * from tmp_use_num_7
             UNION ALL
             select * from tmp_use_num_15
             UNION ALL
             select * from tmp_use_num_30
             ) temp1
         GROUP BY datasource, region_code, event_date, cpn_cfg_type_id, currency
     ) result
         LEFT JOIN ods_vova_vts.ods_vova_ok_coupon_config_type occt
                   on occt.coupon_config_type_id = result.cpn_cfg_type_id
;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_coupon" \
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
if [ $? -ne 0 ];then
  exit 1
fi

