#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"
#dependence
#dim_vova_goods
#dwd_vova_log_goods_impression
#dwd_vova_fact_pay
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_brand_goods PARTITION (pt)
select
/*+ REPARTITION(1) */
tot.event_date,
tot.datasource,
'all' AS region_code,
tot.total_impressions AS impressions,
brand_data.total_impressions as brand_impressions,
tot_gmv.gmv as tot_gmv,
brand_gmv.gmv as brand_gmv,
tot.event_date AS pt
from
(
select
count(*) AS total_impressions,
nvl(log.pt, 'all') AS event_date,
nvl(nvl(log.datasource, 'NA'), 'all') AS datasource
from
dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt >= '${cur_date}'
and  log.pt <= '${cur_date}'
and log.datasource in ('vova', 'airyclub')
group by cube (log.pt, nvl(log.datasource, 'NA'))
HAVING event_date != 'all'
) tot
left join
(
select
count(*) AS total_impressions,
nvl(log.pt, 'all') AS event_date,
nvl(nvl(log.datasource, 'NA'), 'all') AS datasource
from
dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt >= '${cur_date}'
and  log.pt <= '${cur_date}'
and dg.brand_id >0
and log.datasource in ('vova', 'airyclub')
group by cube (log.pt, nvl(log.datasource, 'NA'))
) brand_data on tot.event_date = brand_data.event_date and tot.datasource = brand_data.datasource
left join
(
    SELECT
       nvl(fp.datasource, 'all') as datasource,
       nvl(date(fp.pay_time), 'all') as event_date,
       sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
       sum(fp.goods_number) as goods_cnt,
       count(DISTINCT fp.goods_id) as goods_din
FROM dwd.dwd_vova_fact_pay fp
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fp.goods_id
WHERE date(fp.pay_time) >= '${cur_date}'
  AND date(fp.pay_time) <= '${cur_date}'
  AND dg.brand_id > 0
    group by cube(date(fp.pay_time), fp.datasource)
) brand_gmv on tot.event_date = brand_gmv.event_date and tot.datasource = brand_gmv.datasource
left join
(
SELECT nvl(fp.datasource, 'all') as datasource,
       nvl(date(fp.pay_time), 'all') as event_date,
       sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
       sum(fp.goods_number) as goods_cnt,
       count(DISTINCT fp.goods_id) as goods_din
FROM dwd.dwd_vova_fact_pay fp
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fp.goods_id
WHERE date(fp.pay_time) >= '${cur_date}'
  AND date(fp.pay_time) <= '${cur_date}'
    group by cube(date(fp.pay_time), fp.datasource)
) tot_gmv on tot.event_date = tot_gmv.event_date and tot.datasource = tot_gmv.datasource
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_brand_goods" -e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi