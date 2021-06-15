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
select fp.datasource,
       fp.region_code,
       date(dc.cpn_create_time) as cur_date,
       date (fp.pay_time) AS pay_date,
       dc.cpn_cfg_type,
       dc.cpn_cfg_type_id,
       dc.currency,
       fp.buyer_id,
       fp.order_id
from
dim.dim_vova_coupon dc
inner join dim.dim_vova_order_goods dog
on dc.cpn_code = dog.coupon_code
inner join dwd.dwd_vova_fact_pay fp
on dog.order_goods_id = fp.order_goods_id
where  date (dc.cpn_create_time) <='${cur_date}' and date (dc.cpn_create_time) > date_sub('${cur_date}', 30)
and datediff(fp.pay_time, dc.cpn_create_time) >= 0 and  datediff(fp.pay_time, dc.cpn_create_time) < 30

union all


select 'app_group' as datasource,
       fp.region_code,
       date(dc.cpn_create_time) as cur_date,
       date (fp.pay_time) AS pay_date,
       dc.cpn_cfg_type,
       dc.cpn_cfg_type_id,
       dc.currency,
       fp.buyer_id,
       fp.order_id
from
dim.dim_vova_coupon dc
inner join dim.dim_vova_order_goods dog
on dc.cpn_code = dog.coupon_code
inner join dwd.dwd_vova_fact_pay fp
on dog.order_goods_id = fp.order_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app app ON lower(fp.datasource) = lower(app.app_name) and lower(app.app_name) != 'vova' and lower(app.app_name) != 'airyclub'
where  date (dc.cpn_create_time) <='${cur_date}' and date (dc.cpn_create_time) > date_sub('${cur_date}', 30)
and datediff(fp.pay_time, dc.cpn_create_time) >= 0 and  datediff(fp.pay_time, dc.cpn_create_time) < 30
),
tmp_use_num_res as (
select
cur_date as event_date,
nvl(region_code,'all') as region_code,
nvl(datasource,'all') as datasource,
first(cpn_cfg_type) as cpn_cfg_type,
nvl(cpn_cfg_type_id,'all') as cpn_cfg_type_id,
nvl(currency,'all') as currency,
0 AS give_num,
0 AS give_amount,
0 AS give_user,
0 AS use_num,
0 AS use_amount,
0 AS use_user,
0 AS gmv,
count(distinct(if(datediff(cur_date, pay_date)<3,order_id,null))) AS use_num_3,
count(distinct(if(datediff(cur_date, pay_date)<7,order_id,null))) AS use_num_7,
count(distinct(if(datediff(cur_date, pay_date)<15,order_id,null))) AS use_num_15,
count(distinct order_id) AS use_num_30
from
(select
nvl(t1.datasource, 'NA') as datasource,
nvl(t1.region_code, 'NA') as region_code,
t1.cur_date,
t1.pay_date,
t1.cpn_cfg_type,
nvl(t1.cpn_cfg_type_id, '-1') as cpn_cfg_type_id,
nvl(t1.currency, 'NA') as currency,
t1.buyer_id,
t1.order_id
from
tmp_use_num t1)
group by  cur_date, region_code, datasource, cpn_cfg_type_id, currency with cube
HAVING cpn_cfg_type_id != 'all' AND currency != 'all' AND cur_date != 'all'
)

insert overwrite table dwb.dwb_vova_coupon_v2 PARTITION (pt)
SELECT /*+ REPARTITION(10) */
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
             COUNT (order_id) AS use_num,
             sum (bonus) AS use_amount,
             COUNT (DISTINCT user_id) AS use_user,
             sum (goods_amount + shipping_fee) AS gmv,
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
             nvl(dc.buyer_id, 0) AS buyer_id,
             oi.order_id,
             oi.bonus,
             oi.goods_amount,
             oi.shipping_fee,
             oi.user_id
             FROM dim.dim_vova_coupon dc
             INNER JOIN dim.dim_vova_buyers byr ON byr.buyer_id = dc.buyer_id
             left join ods_vova_vts.ods_vova_order_info oi ON dc.cpn_code = oi.coupon_code
             and oi.pay_status >= 1
             and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
             and oi.parent_order_id = 0
             -- 限制是本日
             and date(oi.pay_time) =  date (dc.cpn_create_time)
             WHERE  date (dc.cpn_create_time) <='${cur_date}' and date (dc.cpn_create_time) > date_sub('${cur_date}', 30)
             -- date (dc.cpn_create_time) = '${cur_date}'
             ) final
             GROUP BY CUBE (final.cpn_create_date, final.cpn_cfg_type_id, final.region_code, final.datasource, final.currency)
             HAVING event_date != 'all' AND cpn_cfg_type_id != 'all' AND currency != 'all'


             UNION ALL
             SELECT nvl(final.cpn_create_date, 'all') AS event_date,
             nvl(final.region_code, 'all') AS region_code,
             'app_group' AS datasource,
             first (final.cpn_cfg_type) AS cpn_cfg_type,
             nvl(final.cpn_cfg_type_id, 'all') AS cpn_cfg_type_id,
             nvl(final.currency, 'all') AS currency,
             count (cpn_id) AS give_num,
             sum (cpn_cfg_val) AS give_amount,
             count (DISTINCT buyer_id) AS give_user,
             COUNT (order_id) AS use_num,
             sum (bonus) AS use_amount,
             COUNT (DISTINCT user_id) AS use_user,
             sum (goods_amount + shipping_fee) AS gmv,
             0 AS use_num_3,
             0 AS use_num_7,
             0 AS use_num_15,
             0 AS use_num_30
             FROM (
             SELECT date(dc.cpn_create_time) AS cpn_create_date,
             nvl(dc.cpn_cfg_type_id, '-1') AS cpn_cfg_type_id,
             dc.cpn_cfg_type,
             nvl(byr.region_code, 'NA') AS region_code,
             'app_group' AS datasource,
             nvl(dc.currency, 'NA') AS currency,
             dc.cpn_id,
             nvl(dc.cpn_cfg_val, 0) AS cpn_cfg_val,
             nvl(dc.buyer_id, 0) AS buyer_id,
             oi.order_id,
             oi.bonus,
             oi.goods_amount,
             oi.shipping_fee,
             oi.user_id
             FROM dim.dim_vova_coupon dc
             INNER JOIN dim.dim_vova_buyers byr ON byr.buyer_id = dc.buyer_id
             INNER JOIN ods_vova_vtsf.ods_vova_acg_app app ON lower (byr.datasource) = lower (app.app_name) and lower (app.app_name) != 'vova' and lower (app.app_name) != 'airyclub'
             left join ods_vova_vts.ods_vova_order_info oi ON dc.cpn_code = oi.coupon_code
             and oi.pay_status >= 1
             and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
             and oi.parent_order_id = 0
             -- 限制是本日
             and date(oi.pay_time) =  date (dc.cpn_create_time)
             WHERE date (dc.cpn_create_time) <='${cur_date}' and date (dc.cpn_create_time) > date_sub('${cur_date}', 30)
             ) final
             GROUP BY CUBE (final.cpn_create_date, final.cpn_cfg_type_id, final.region_code, final.currency)
             HAVING event_date != 'all' AND cpn_cfg_type_id != 'all' AND currency != 'all'

             UNION ALL
             select * from tmp_use_num_res
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
--conf "spark.dynamicAllocation.maxExecutors=150" \
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

