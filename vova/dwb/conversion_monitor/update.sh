#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#dim_vova_goods
#dwd_vova_fact_pay
#dwd_vova_log_goods_impression
sql="
DROP TABLE if exists tmp.tmp_dwb_vova_conversion_monitor_base;
CREATE TABLE tmp.tmp_dwb_vova_conversion_monitor_base as
select
/*+ REPARTITION(1) */
date(dog.pay_time) AS pay_date,
dog.goods_id,
count(distinct dog.order_goods_id) as order_goods_cnt
from
dwd.dwd_vova_fact_pay dog
where dog.datasource = 'vova'
and date(dog.pay_time) = '${cur_date}'
group by date(dog.pay_time), dog.goods_id
having order_goods_cnt >= 5
;


INSERT OVERWRITE TABLE tmp.tmp_dwb_vova_conversion_monitor_sec_cat
select
/*+ REPARTITION(1) */
nvl(t1.second_cat_id, 0) AS second_cat_id,
nvl(t1.order_cnt, 0) AS order_cnt,
nvl(t1.buyer_cnt, 0) AS buyer_cnt,
nvl(t2.web_order_cnt, 0) AS web_order_cnt,
nvl(t3.expre, 0) AS expre,
nvl(t4.new_user_cnt, 0) AS new_user_cnt,
nvl(t4.new_user_order_cnt, 0) AS new_user_order_cnt,
round(t2.web_order_cnt / t1.order_cnt, 6) as web_order_rate,
round(t3.expre / t1.order_cnt, 6) as expre_efficiency,
round(t4.new_user_cnt / t1.buyer_cnt, 6) as new_user_rate,
round(t4.new_user_order_cnt / t1.order_cnt, 6) as new_user_order_rate
from
(
select
dg.second_cat_id,
count(distinct dog.order_goods_id) as order_cnt,
count(distinct dog.buyer_id) as buyer_cnt
from dwd.dwd_vova_fact_pay dog
inner join dim.dim_vova_goods dg on dg.goods_id = dog.goods_id
where dog.datasource = 'vova'
and date(dog.pay_time) = '${cur_date}'
and dg.second_cat_id is not null
group by dg.second_cat_id
) t1
left join
(
select
dg.second_cat_id,
count(distinct dog.order_goods_id) as web_order_cnt
from
dwd.dwd_vova_fact_pay dog
inner join dim.dim_vova_goods dg on dg.goods_id = dog.goods_id
where dog.datasource = 'vova'
and date(dog.pay_time) = '${cur_date}'
and dog.from_domain not like '%api%'
group by dg.second_cat_id
) t2 on t1.second_cat_id = t2.second_cat_id
left join
(
select
dg.second_cat_id,
count(*) as expre
from
dwd.dwd_vova_log_goods_impression log
  INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
where log.pt = '${cur_date}'
AND log.datasource = 'vova'
group by dg.second_cat_id
) t3 on t1.second_cat_id = t3.second_cat_id
LEFT JOIN
(
select
dg.second_cat_id,
count(DISTINCT fp.buyer_id) AS new_user_cnt,
count(DISTINCT fp.buyer_id) AS new_user_order_cnt
from
dwd.dwd_vova_fact_pay fp
INNER JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
inner join
(
select
t1.buyer_id
from
(
select
DISTINCT fp.buyer_id
from
dwd.dwd_vova_fact_pay fp
where fp.datasource = 'vova'
AND date(fp.pay_time) >=  date_sub('${cur_date}', 30)
AND date(fp.pay_time) <=  '${cur_date}'
) t1
left join
(
select
DISTINCT fp.buyer_id
from
dwd.dwd_vova_fact_pay fp
where fp.datasource = 'vova'
AND date(fp.pay_time) <  date_sub('${cur_date}', 30)
) t2 on t1.buyer_id = t2.buyer_id
WHERE t2.buyer_id is null
) is_new on is_new.buyer_id = fp.buyer_id
where fp.datasource = 'vova'
AND date(fp.pay_time) =  '${cur_date}'
GROUP BY dg.second_cat_id
) t4 on t1.second_cat_id = t4.second_cat_id
;


INSERT OVERWRITE TABLE dwb.dwb_vova_conversion_monitor PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
t1.goods_id,
nvl(t1.order_cnt, 0) as order_cnt,
nvl(t1.buyer_cnt, 0) as buyer_cnt,
nvl(t2.web_order_cnt, 0) as web_order_cnt,
nvl(t3.expre, 0) as expre,
nvl(t4.new_user_cnt, 0) as new_user_cnt,
nvl(t4.new_user_order_cnt, 0) as new_user_order_cnt,
sec.order_cnt AS sec_order_cnt,
sec.buyer_cnt AS sec_buyer_cnt,
sec.web_order_cnt AS sec_web_order_cnt,
sec.expre AS sec_expre,
sec.new_user_cnt AS sec_new_user_cnt,
sec.new_user_order_cnt AS sec_new_user_order_cnt,
nvl(round(t2.web_order_cnt / t1.order_cnt, 6), 0) as web_order_rate,
nvl(round(t2.web_order_cnt / t1.order_cnt * sec.order_cnt / sec.web_order_cnt, 6), 0) as web_order_rate_ratio,
nvl(round(t3.expre / t1.order_cnt, 6), 0) as expre_efficiency,
nvl(round(t3.expre / t1.order_cnt * sec.order_cnt / sec.expre, 6), 0) as expre_efficiency_ratio,
nvl(round(t4.new_user_cnt / t1.buyer_cnt, 6), 0) as new_user_rate,
nvl(round(t4.new_user_cnt / t1.buyer_cnt * sec.buyer_cnt / sec.new_user_cnt, 6), 0) as new_user_rate_ratio,
nvl(round(t4.new_user_order_cnt / t1.order_cnt, 6), 0) as new_user_order_rate,
nvl(round(t4.new_user_order_cnt / t1.order_cnt * sec.order_cnt / sec.new_user_order_cnt, 6), 0) as new_user_order_rate_ratio
from
(
select
dog.goods_id,
count(distinct dog.order_goods_id) as order_cnt,
count(distinct dog.buyer_id) as buyer_cnt
from
tmp.tmp_dwb_vova_conversion_monitor_base t1
inner join dwd.dwd_vova_fact_pay dog on dog.goods_id = t1.goods_id
where dog.datasource = 'vova'
and date(dog.pay_time) = '${cur_date}'
group by dog.goods_id
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
left join tmp.tmp_dwb_vova_conversion_monitor_sec_cat sec on sec.second_cat_id = dg.second_cat_id
left join
(
select
dog.goods_id,
count(distinct dog.order_goods_id) as web_order_cnt
from
tmp.tmp_dwb_vova_conversion_monitor_base t1
inner join dwd.dwd_vova_fact_pay dog on dog.goods_id = t1.goods_id
where dog.datasource = 'vova'
and date(dog.pay_time) = '${cur_date}'
and dog.from_domain not like '%api%'
group by dog.goods_id
) t2 on t1.goods_id = t2.goods_id
left join
(
select
dg.goods_id,
count(*) as expre
from
dwd.dwd_vova_log_goods_impression log
  INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
  INNER JOIN tmp.tmp_dwb_vova_conversion_monitor_base t1 ON t1.goods_id = dg.goods_id
where log.pt = '${cur_date}'
AND log.datasource = 'vova'
group by dg.goods_id
) t3 on t1.goods_id = t3.goods_id
LEFT JOIN
(
select
fp.goods_id,
count(DISTINCT fp.buyer_id) AS new_user_cnt,
count(DISTINCT fp.buyer_id) AS new_user_order_cnt
from
dwd.dwd_vova_fact_pay fp
INNER JOIN tmp.tmp_dwb_vova_conversion_monitor_base t1 ON t1.goods_id = fp.goods_id
inner join
(
select
t1.buyer_id
from
(
select
DISTINCT fp.buyer_id
from
dwd.dwd_vova_fact_pay fp
where fp.datasource = 'vova'
AND date(fp.pay_time) >=  date_sub('${cur_date}', 30)
AND date(fp.pay_time) <=  '${cur_date}'
) t1
left join
(
select
DISTINCT fp.buyer_id
from
dwd.dwd_vova_fact_pay fp
where fp.datasource = 'vova'
AND date(fp.pay_time) <  date_sub('${cur_date}', 30)
) t2 on t1.buyer_id = t2.buyer_id
WHERE t2.buyer_id is null
) is_new on is_new.buyer_id = fp.buyer_id
where fp.datasource = 'vova'
AND date(fp.pay_time) =  '${cur_date}'
GROUP BY fp.goods_id
) t4 on t1.goods_id = t4.goods_id
;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_conversion_monitor" \
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

