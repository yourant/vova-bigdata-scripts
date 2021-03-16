#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
drop table if exists tmp.tmp_dwb_vova_nps_email;
create table tmp.tmp_dwb_vova_nps_email as
select
/*+ REPARTITION(1) */
fp.buyer_id,
date(fp.pay_time) AS pay_date
from
dwd.dwd_vova_fact_pay fp
where fp.datasource = 'vova'
AND date(fp.pay_time) >= '2018-01-01'
group by fp.buyer_id, date(fp.pay_time)
;


set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_nps_email PARTITION (pt)
select
/*+ REPARTITION(1) */
nps.created_time AS nps_submit_time,
oi.email,
nps.order_sn,
nps.rate,
nps.reason,
case when nps.order_status = 2 then '已取消'
     when nps.shipping_status = 2 then '已收货'
     when nps.shipping_status = 0 then '未发货'
     when nps.shipping_status = 1 then '运输中'
     else '' end as order_type,
oi.order_time,
refund.order_goods_cnt,
refund.cancel_order_goods_cnt,
refund.ra_order_goods_cnt,
refund.ro_order_goods_cnt,
refund.fin_order_goods_cnt,
buyer_level.buyer_level,
his.min_pay_time,
his.max_pay_time,
his.his_gmv,
his.his_paid_order_cnt,
r.region_code,
date(nps.created_time) as pt
from
ods_vova_vts.ods_vova_order_nps nps
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_sn = nps.order_sn
LEFT JOIN ods_vova_vts.ods_vova_region r ON r.region_id = oi.country
left join (
select
dog.order_id,
count(distinct dog.order_goods_id) AS order_goods_cnt,
count(distinct if(fr.order_goods_id is not null and fr.refund_type_id != 2 and dog.sku_order_status = 2, dog.order_goods_id, null)) AS cancel_order_goods_cnt,
count(distinct if(fr.order_goods_id is not null and fr.refund_type_id = 2, dog.order_goods_id, null)) AS ra_order_goods_cnt,
count(distinct if(ogs.sku_return_status > 0, dog.order_goods_id, null)) AS ro_order_goods_cnt,
count(distinct if(dog.sku_pay_status = 4, dog.order_goods_id, null)) AS fin_order_goods_cnt
from
dim.dim_vova_order_goods dog
left join dwd.dwd_vova_fact_refund fr on dog.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = dog.order_goods_id
WHERE dog.parent_order_id = 0
AND dog.pay_status >= 1
AND dog.datasource = 'vova'
group by dog.order_id
) refund on refund.order_id = oi.order_id
left join
(
select
oi.user_id,
max(oi.pay_time) as max_pay_time,
min(oi.pay_time) as min_pay_time,
sum(oi.goods_amount + oi.shipping_fee) AS his_gmv,
count(distinct oi.order_id) AS his_paid_order_cnt
from
ods_vova_vts.ods_vova_order_info oi
where oi.pay_status >= 1
  and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  and oi.parent_order_id = 0
  and oi.project_name = 'vova'
group by oi.user_id
) his on his.user_id = oi.user_id
left join
(
SELECT
case
when buyer_level.lead_pay_date is null THEN '新用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 30  THEN '月复购用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 90  THEN '3个月内回流用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 180  THEN '6个月内回流用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) > 180 THEN '长期回流用户'
else '' END as buyer_level,
buyer_level.buyer_id,
buyer_level.pay_date,
buyer_level.lead_pay_date,
datediff(buyer_level.pay_date, buyer_level.lead_pay_date) as diff
FROM
(
select
buyer_id,
pay_date,
row_number() over (partition by buyer_id order by pay_date DESC) as rank1,
LEAD(pay_date, 1) OVER (PARTITION BY buyer_id ORDER BY pay_date DESC) lead_pay_date
from
tmp.tmp_dwb_vova_nps_email
) buyer_level
where rank1 = 1
) buyer_level on buyer_level.buyer_id = oi.user_id
where oi.pay_status >= 1
AND oi.parent_order_id = 0
AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND date(nps.created_time) = '${cur_date}'
AND oi.project_name = 'vova'
;

INSERT OVERWRITE TABLE dwb.dwb_vova_nps PARTITION (pt)
select
/*+ REPARTITION(1) */
nvl(nvl(r.region_code, 'NA'), 'all') AS region_code,
nvl(buyer_level.buyer_level, 'all') AS buyer_level,
count(distinct if(nps.rate >=0 and nps.rate <=6, nps.order_sn, null)) AS nps_rate_0_to_6,
count(distinct if(nps.rate >6 and nps.rate <=8, nps.order_sn, null)) AS nps_rate_6_to_8,
count(distinct if(nps.rate >8 and nps.rate <=10, nps.order_sn, null)) AS nps_rate_8_to_10,
count(distinct nps.order_sn) AS nps_rate_cnt,
count(distinct oi.order_id) AS paid_cnt,
nvl(date(oi.pay_time), 'all') AS pt
from
ods_vova_vts.ods_vova_order_info oi
left join ods_vova_vts.ods_vova_order_nps nps on oi.order_sn = nps.order_sn
LEFT JOIN ods_vova_vts.ods_vova_region r ON r.region_id = oi.country
left join
(
SELECT
case
when buyer_level.lead_pay_date is null THEN '新用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 30  THEN '月复购用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 90  THEN '3个月内回流用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) <= 180  THEN '6个月内回流用户'
when datediff(buyer_level.pay_date, buyer_level.lead_pay_date) > 180 THEN '长期回流用户'
else '' END as buyer_level,
buyer_level.buyer_id,
buyer_level.pay_date,
buyer_level.lead_pay_date,
datediff(buyer_level.pay_date, buyer_level.lead_pay_date) as diff
FROM
(
select
buyer_id,
pay_date,
row_number() over (partition by buyer_id order by pay_date DESC) as rank1,
LEAD(pay_date, 1) OVER (PARTITION BY buyer_id ORDER BY pay_date DESC) lead_pay_date
from
tmp.tmp_dwb_vova_nps_email
) buyer_level
where rank1 = 1
) buyer_level on buyer_level.buyer_id = oi.user_id
where oi.pay_status >= 1
AND oi.parent_order_id = 0
AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND oi.project_name = 'vova'
AND oi.pay_time >= '2020-01-01'
GROUP BY cube(date(oi.pay_time), nvl(r.region_code, 'NA'), buyer_level.buyer_level)
HAVING pt != 'all'
;

"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_nps" \
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

