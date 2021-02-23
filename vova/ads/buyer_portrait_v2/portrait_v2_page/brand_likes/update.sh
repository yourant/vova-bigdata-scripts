#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_buyer_page_brand_top_behave partition(bpt)
select
/*+ REPARTITION(1) */
tmp1.buyer_id,
db.datasource,
db.current_device_id,
db.email,
tmp1.type,
tmp1.day_gap,
tmp1.behave_top_array,
cast(substr(tmp1.buyer_id,4) as int)%200 as bpt
from
(select
buyer_id,
'click' as type,
7 as day_gap,
collect_list(concat(brand_name,':',clk_cnt_1w)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.clk_cnt_1w,
row_number() over(partition by cl.buyer_id order by cl.clk_cnt_1w desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and clk_cnt_1w>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'click' as type,
15 as day_gap,
collect_list(concat(brand_name,':',clk_cnt_15d)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.clk_cnt_15d,
row_number() over(partition by cl.buyer_id order by cl.clk_cnt_15d desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.clk_cnt_15d>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'click' as type,
30 as day_gap,
collect_list(concat(brand_name,':',clk_cnt_1m)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.clk_cnt_1m,
row_number() over(partition by cl.buyer_id order by cl.clk_cnt_1m desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.clk_cnt_1m>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'add_cat' as type,
7 as day_gap,
collect_list(concat(brand_name,':',add_cat_cnt_1w)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.add_cat_cnt_1w,
row_number() over(partition by cl.buyer_id order by cl.add_cat_cnt_1w desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.add_cat_cnt_1w>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'add_cat' as type,
15 as day_gap,
collect_list(concat(brand_name,':',add_cat_cnt_15d)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.add_cat_cnt_15d,
row_number() over(partition by cl.buyer_id order by cl.add_cat_cnt_15d desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.add_cat_cnt_15d>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'add_cat' as type,
30 as day_gap,
collect_list(concat(brand_name,':',add_cat_cnt_1m)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.add_cat_cnt_1m,
row_number() over(partition by cl.buyer_id order by cl.add_cat_cnt_1m desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.add_cat_cnt_1m>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'order' as type,
7 as day_gap,
collect_list(concat(brand_name,':',ord_cnt_1w)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.ord_cnt_1w,
row_number() over(partition by cl.buyer_id order by cl.ord_cnt_1w desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.ord_cnt_1w>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'order' as type,
15 as day_gap,
collect_list(concat(brand_name,':',ord_cnt_15d)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.ord_cnt_15d,
row_number() over(partition by cl.buyer_id order by cl.ord_cnt_15d desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.ord_cnt_15d>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'order' as type,
30 as day_gap,
collect_list(concat(brand_name,':',ord_cnt_1m)) as behave_top_array
from
(select
cl.buyer_id,
nvl(vb.brand_name,'nobrand') as brand_name,
cl.ord_cnt_1m,
row_number() over(partition by cl.buyer_id order by cl.ord_cnt_1m desc) rk
from
ads.ads_vova_buyer_portrait_brand_likes cl
left join ods_vova_vts.ods_vova_brand vb on cl.brand_id = vb.brand_id
where cl.pt='${pre_date}' and cl.ord_cnt_1m>0)
where rk <=20
group by
buyer_id)tmp1
left join dim.dim_vova_buyers db on tmp1.buyer_id = db.buyer_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_buyer_page_brand_top_behave" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
