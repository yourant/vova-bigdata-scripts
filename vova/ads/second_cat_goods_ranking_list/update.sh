#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
with tmp_all as(
select
dg.goods_id,
dg.goods_sn,
mpg.group_number,
dg.second_cat_id,
if(dg.brand_id>0,1,0) as is_brand,
nvl(tmp.region_id,0) as region_id,
nvl(tmp.add_cat_cnt,0) as add_cat_cnt,
nvl(tmp.collect_cnt,0) as collect_cnt,
nvl(tmp.sales_vol,0) as sales_vol,
nvl(tmp.add_cat_cnt,0)*10 +nvl(tmp.sales_vol,0)*1+nvl(tmp.well_comment_cnt,0 )*1+nvl(tmp.collect_cnt,0)*5 as popularity_likes,
nvl(tmp.gmv,0) gmv,
nvl(tmp.expre_cnt,0) expre_cnt,
nvl(tmp.well_comment_cnt,0 ) as well_comment_cnt
from
dim.dim_vova_goods dg
left join ads.ads_vova_min_price_goods_h mpg
ON mpg.goods_id = dg.goods_id
AND mpg.pt = '${pre_date}'
AND strategy = 'c'
left join
(select
nvl(tmp_goods_pre.goods_id,tmp_comment.goods_id) goods_id,
nvl(tmp_goods_pre.region_id,tmp_comment.region_id) region_id,
nvl(tmp_goods_pre.add_cat_cnt,0) add_cat_cnt,
nvl(tmp_goods_pre.collect_cnt,0) collect_cnt,
nvl(tmp_goods_pre.sales_vol,0) sales_vol,
nvl(tmp_goods_pre.gmv,0) gmv,
nvl(tmp_goods_pre.expre_cnt,0) expre_cnt,
nvl(tmp_comment.well_comment_cnt,0) well_comment_cnt
from
(SELECT
    gb.gs_id AS goods_id,
    nvl(db.region_id,0) as region_id,
    sum(add_cat_cnt) as add_cat_cnt,
    sum(collect_cnt) as collect_cnt,
    sum(sales_vol) as sales_vol,
    sum(gmv) as gmv,
    sum(expre_cnt) as expre_cnt
FROM
    dws.dws_vova_buyer_goods_behave gb
    LEFT JOIN dim.dim_vova_buyers db ON db.buyer_id = gb.buyer_id
WHERE
    gb.pt <= '${pre_date}' AND gb.pt > date_sub( '${pre_date}', 7 )
    AND db.region_id IS NOT NULL
GROUP BY
    gb.gs_id,
    db.region_id
    grouping sets(
      (gb.gs_id,db.region_id),
      (gb.gs_id)
      ))tmp_goods_pre
full join (
select
fc.goods_id,
nvl(db.region_id,0) as region_id,
sum(if(rating>=3,1,0)) as well_comment_cnt
from
dwd.dwd_vova_fact_comment fc
LEFT JOIN dim.dim_vova_buyers db ON db.buyer_id = fc.buyer_id
where db.region_id IS NOT NULL
GROUP BY
    fc.goods_id,
    db.region_id
    grouping sets(
      (fc.goods_id,db.region_id),
      (fc.goods_id)
      )
)tmp_comment
on tmp_goods_pre.goods_id = tmp_comment.goods_id
and tmp_goods_pre.region_id = tmp_comment.region_id) tmp
 on tmp.goods_id = dg.goods_id
 where dg.second_cat_id is not null
)


insert overwrite table ads.ads_vova_second_cat_goods_ranking_list partition(pt='${pre_date}')
select
goods_id,
region_id,
second_cat_id,
is_brand,
list_type,
list_val,
rank
from
(select
goods_id,
region_id,
second_cat_id,
is_brand,
list_type,
list_val,
row_number() over(partition by list_type,region_id,second_cat_id,is_brand order by rank) rank
from
(select
goods_id,
region_id,
second_cat_id,
is_brand,
1 as list_type,
sales_vol as list_val,
rank,
row_number() over(partition by region_id,second_cat_id,is_brand,goods_sn order by rank) goods_sn_rank,
group_number,
row_number() over(partition by region_id,second_cat_id,is_brand,group_number order by rank) group_number_rank
from
(select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.sales_vol,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by sales_vol desc,gmv desc, expre_cnt desc) rank
from
tmp_all
inner join ods_vova_vts.ods_vova_region vr
on tmp_all.region_id = vr.region_id and vr.parent_id=0
where vr.region_code in ('GB','FR','DE','IT','ES','TW')

union all

select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.sales_vol,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by sales_vol desc, gmv desc, expre_cnt desc) rank
from
tmp_all
where region_id = 0)

union all

select
goods_id,
region_id,
second_cat_id,
is_brand,
2 as list_type,
well_comment_cnt as list_val,
rank,
row_number() over(partition by region_id,second_cat_id,is_brand,goods_sn order by rank) goods_sn_rank,
group_number,
row_number() over(partition by region_id,second_cat_id,is_brand,group_number order by rank) group_number_rank
from
(select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.well_comment_cnt,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by well_comment_cnt desc,gmv desc, expre_cnt desc) rank
from
tmp_all
inner join ods_vova_vts.ods_vova_region vr
on tmp_all.region_id = vr.region_id and vr.parent_id=0
where vr.region_code in ('GB','FR','DE','IT','ES','TW')

union all

select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.well_comment_cnt,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by well_comment_cnt desc, gmv desc, expre_cnt desc) rank
from
tmp_all
where region_id = 0)

union all

select
goods_id,
region_id,
second_cat_id,
is_brand,
3 as list_type,
popularity_likes as list_val,
rank,
row_number() over(partition by region_id,second_cat_id,is_brand,goods_sn order by rank) goods_sn_rank,
group_number,
row_number() over(partition by region_id,second_cat_id,is_brand,group_number order by rank) group_number_rank
from
(select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.popularity_likes,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by popularity_likes desc,gmv desc, expre_cnt desc) rank
from
tmp_all
inner join ods_vova_vts.ods_vova_region vr
on tmp_all.region_id = vr.region_id and vr.parent_id=0
where vr.region_code in ('GB','FR','DE','IT','ES','TW')

union all

select
tmp_all.goods_id,
tmp_all.goods_sn,
tmp_all.group_number,
tmp_all.second_cat_id,
tmp_all.region_id,
tmp_all.is_brand,
tmp_all.popularity_likes,
row_number() over(partition by tmp_all.region_id,tmp_all.second_cat_id,tmp_all.is_brand order by popularity_likes desc, gmv desc, expre_cnt desc) rank
from
tmp_all
where region_id = 0))
where goods_sn_rank = 1 and (group_number is null or group_number_rank=1))
where rank<=30;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
    --driver-memory 8G \
    --executor-memory 8G --executor-cores 1 \
    --conf "spark.sql.parquet.writeLegacyFormat=true"  \
    --conf "spark.dynamicAllocation.minExecutors=30" \
    --conf "spark.dynamicAllocation.initialExecutors=30" \
    --conf "spark.app.name=ads_second_cat_goods_ranking_list" \
    --conf "spark.sql.crossJoin.enabled=true" \
    --conf "spark.default.parallelism=360" \
    --conf "spark.sql.shuffle.partitions=360" \
    --conf "spark.dynamicAllocation.maxExecutors=150" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.join.enabled=true" \
    --conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
    -e "${sql}"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
