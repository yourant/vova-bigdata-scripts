#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_group_goods_day7_top50_req7182_chenkai_${cur_date}"

###逻辑sql
sql="
with tmp1 (
  select
    group_number,
    second_cat_id,
    goods_id,
    gmv,
    goods_number,
    group_gmv
  from
  (
    select
      group_number,
      second_cat_id,
      goods_id,
      gmv,
      goods_number,
      group_goods_row,
      group_gmv,
      dense_rank() over(partition by second_cat_id order by group_gmv desc) group_row
    from
    (
      select
        group_number,
        nvl(second_cat_id, '0') second_cat_id,
        goods_id,
        gmv,
        goods_number,
        row_number() over(partition by group_number order by gmv desc) group_goods_row,
        sum(gmv) over(partition by group_number) group_gmv
      from
      (
        select
          ampgh.group_number group_number,
          dg.second_cat_id second_cat_id,
          ampgh.goods_id goods_id,
          sum(fp.shipping_fee+fp.shop_price*fp.goods_number) gmv,
          sum(fp.goods_number) goods_number
        from
          dim.dim_vova_goods dg
        left join
        (
          select
          distinct
            pt,
            group_number,
            goods_id
          from
            ads.ads_vova_min_price_goods_h
          where pt = '${cur_date}'
        ) ampgh
        on ampgh.goods_id = dg.goods_id
        left join
          dwd.dwd_vova_fact_pay fp
        on dg.goods_id = fp.goods_id
        where dg.first_cat_name in (
            'Women\'s Clothing',
            'Home & Garden',
            'Health & Beauty',
            'Bags, Watches & Accessories'
          )
          and fp.datasource = 'vova'
          and dg.second_cat_id is not null
          and ampgh.group_number is not null
          and fp.order_time >= date_sub('${cur_date}', 7)
          and fp.order_time <= '${cur_date}'
          and dg.datasource = 'vova'
        group by ampgh.group_number, ampgh.goods_id, dg.second_cat_id
      )
    ) where group_goods_row = 1
  ) where group_row <= 50
)

insert overwrite table dwb.dwb_vova_group_goods_day7_top50 PARTITION (pt='${cur_date}')
select
/*+ REPARTITION(1) */
  'vova',
  dg.virtual_goods_id virtual_goods_id,
  dg.first_cat_id first_cat_id,
  dg.first_cat_name first_cat_name,
  dg.second_cat_id second_cat_id,
  regexp_replace(dg.second_cat_name,'\'',' ') second_cat_name,
  dg.third_cat_id third_cat_id,
  dg.third_cat_name third_cat_name,
  case when vb.brand_name is not null and vb.brand_name != '' then vb.brand_name
    when dg.brand_id > 0 then 'Y'
    else 'NA'
  end brand_name,
  tmp1.gmv gmv,
  tmp1.goods_number goods_number,
  dm.mct_id mct_id,
  dm.mct_name mct_name,
  amr.rank first_cat_rank,
  dg.is_on_sale is_on_sale
from
  tmp1
left join
  dim.dim_vova_goods dg
on tmp1.goods_id = dg.goods_id
left join
  ods_vova_vts.ods_vova_brand vb
on dg.brand_id = vb.brand_id
left join
  dim.dim_vova_merchant dm
on dg.mct_id = dm.mct_id
left join
(
  select
    mct_id,
    max(rank) rank -- 流量等级
  from
    ads.ads_vova_mct_rank
  where pt ='${cur_date}'
  group by mct_id
) amr
on dg.mct_id = amr.mct_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
