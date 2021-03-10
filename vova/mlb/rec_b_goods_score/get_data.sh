#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="mlb_vova_rec_goods_scorebase_data_d_req8229_lijinle_chenkai"

###逻辑sql
sql="
insert overwrite table mlb.mlb_vova_rec_goods_scorebase_data_d PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(5) */
  t1.*
  ,nvl(t2.comment_cnt_6m,0)       AS comment_cnt_6m
  ,nvl(t2.comment_good_cnt_6m ,0) AS comment_good_cnt_6m
  ,nvl(t2.gmv_15d ,0)             AS gmv_15d
  ,nvl(t2.sales_vol_15d,0)        AS sales_vol_15d
  ,nvl(t2.expre_cnt_15d,0)        AS expre_cnt_15d
  ,nvl(t2.clk_cnt_15d,0)          AS clk_cnt_15d
  ,nvl(t2.collect_cnt_15d,0)      AS collect_cnt_15d
  ,nvl(t2.add_cat_cnt_15d,0)      AS add_cat_cnt_15d
  ,t2.inter_rate_3_6w             AS inter_rate_3_6w
  ,t2.lrf_rate_9_12w              AS lrf_rate_9_12w
  ,t2.nlrf_rate_5_8w              AS nlrf_rate_5_8w
  ,t2.bs_inter_rate_3_6w          AS bs_inter_rate_3_6w
  ,t2.bs_lrf_rate_9_12w           AS bs_lrf_rate_9_12w
  ,t2.bs_nlrf_rate_5_8w           AS bs_nlrf_rate_5_8w
  ,t2.clk_uv_15d                  AS clk_uv_15d
  ,nvl(t3.score,60)               AS mct_score
FROM
(
	SELECT  goods_id
	       ,first_cat_id
	       ,nvl(second_cat_id,first_cat_id) AS second_cat_id
	       ,(shop_price + shipping_fee)     AS total_price
	       ,mct_id
	FROM dim.dim_vova_goods
	WHERE is_on_sale = 1
) t1
LEFT JOIN
(
	SELECT *
	FROM ads.ads_vova_goods_portrait
	WHERE to_date(pt) = date_sub('${cur_date}', 1)
) t2
ON t1.goods_id = t2.gs_id
LEFT JOIN
(
	SELECT  *
	FROM ads.ads_vova_mct_rank
	WHERE to_date(pt) = date_sub('${cur_date}', 1)
) t3
ON t1.mct_id = t3.mct_id AND t1.first_cat_id = t3.first_cat_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
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
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


