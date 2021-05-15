#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
INSERT overwrite TABLE ads.ads_vova_rfm90_tag partition(pt='${pre_date}')
select
user_id,
pm,
pf,
pr,
pn,
case when pm = 1 and pf = 1 and pr = 1 then 1
     when pm = 2 and pf = 1 and pr = 1 then 2
     when pm = 1 and pf = 2 and pr = 1 then 3
     when pm = 2 and pf = 2 and pr = 1 then 4
     when pm = 1 and pf = 2 and pr = 2 then 5
     when pm = 2 and pf = 2 and pr = 2 then 6
     when pm = 1 and pf = 1 and pr = 2 then 7
     when pm = 2 and pf = 1 and pr = 2 then 8
     else 0 end as pimp
from
(select
db.buyer_id as user_id,
case when gmv_3m>=70 then 1
     when gmv_3m>0   then 2
     else 0 end as pm,
case when pay_day_cnt_3m>=3 then 1
     when pay_day_cnt_3m>0  then 2
     else 0 end as pf,
case when pay_cnt_1m>=1 then 1
     when gmv_3m>0  then 2
     else 0 end as pr,
case when nvl(gmv_3m,0)=0 and gmv>0 then 1
     when nvl(gmv_3m,0)=0 and datediff('${pre_date}',dd.activate_time)>=0 and datediff('${pre_date}',dd.activate_time)<7 then 2
     when nvl(gmv_3m,0)=0 and datediff('${pre_date}',dd.activate_time)>=7 and datediff('${pre_date}',dd.activate_time)<30 then 3
     when nvl(gmv_3m,0)=0 and datediff('${pre_date}',dd.activate_time)>=30 then 4
     else 0 end as pn
from
dim.dim_vova_buyers db
left join
(select
buyer_id,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv,
sum(if(datediff('${pre_date}',fp.pay_time)<90,fp.shop_price*fp.goods_number+fp.shipping_fee,0)) as gmv_3m,
count(distinct(if(datediff('${pre_date}',fp.pay_time)<90,date(fp.pay_time),null)) ) as pay_day_cnt_3m,
sum(distinct(if(datediff('${pre_date}',fp.pay_time)<30,1,0)) ) as pay_cnt_1m
from
dwd.dwd_vova_fact_pay fp
where date(pay_time)<='${pre_date}'
group by buyer_id
) rfm
on db.buyer_id = rfm.buyer_id
left join dim.dim_vova_devices dd on db.current_device_id = dd.device_id and dd.datasource = db.datasource)
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_rfm90_tag" \
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
if [ $? -ne 0 ]; then
  exit 1
fi
