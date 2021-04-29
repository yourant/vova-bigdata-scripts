#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="mlb_vova_rec_b_goods_score_all_d_req8229_chenkai"

###逻辑sql
sql="
msck repair table mlb.mlb_vova_rec_b_goods_score_d;
msck repair table mlb.mlb_vova_rec_b_catgoods_score_d;
msck repair table mlb.mlb_vova_rec_goods_scorebase_data_d;
msck repair table mlb.mlb_vova_b_goods_score_details_d;
msck repair table mlb.mlb_vova_b_goods_cat_score_details_d;

insert overwrite table mlb.mlb_vova_rec_b_goods_score_all_d PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(10) */
    goods_id            ,
    first_cat_id        ,
    second_cat_id       ,
    total_price         ,
    mct_id              ,
    comment_cnt_6m      ,
    comment_good_cnt_6m ,
    gmv_15d             ,
    sales_vol_15d       ,
    expre_cnt_15d       ,
    clk_cnt_15d         ,
    collect_cnt_15d     ,
    add_cat_cnt_15d     ,
    inter_rate_3_6w     ,
    lrf_rate_9_12w      ,
    nlrf_rate_5_8w      ,
    bs_inter_rate_3_6w  ,
    bs_lrf_rate_9_12w   ,
    bs_nlrf_rate_5_8w   ,
    clk_uv_15d          ,
    mct_score           ,

    price_score           ,
    good_cm_rate_score    ,
    good_cm_cnt_score     ,
    good_expre_cnt_score  ,
    good_clk_cnt_score    ,
    good_cart_cnt_score   ,
    good_collect_cnt_score,
    good_sale_vol_score   ,
    inter_rate_score      ,
    nlrf_rate_score       ,
    lrf_rate_score        ,
    gcr_score             ,
    gr_score              ,
    ctr_score                ,

    good_cm_rate_cat_score    ,
    good_cm_cnt_cat_score     ,
    good_expre_cnt_cat_score  ,
    good_clk_cnt_cat_score    ,
    good_cart_cnt_cat_score   ,
    good_collect_cnt_cat_score,
    good_sale_vol_cat_score   ,
    gcr_cat_score             ,
    gr_cat_score              ,
    ctr_cat_score           ,

    base_score          ,
    hot_score           ,
    conversion_score    ,
    honor_score         ,
    overall_score       ,

    base_cat_score      ,
    hot_cat_score       ,
    conversion_cat_score,
    honor_cat_score     ,
    overall_cat_score   ,
    if(inter_days_score > 100, 100, inter_days_score) inter_days_score -- 平滑上网天数评分
from
(
  SELECT
    t1.goods_id            ,
    t3.first_cat_id        ,
    t3.second_cat_id       ,
    t3.total_price         ,
    t3.mct_id              ,
    t3.comment_cnt_6m      ,
    t3.comment_good_cnt_6m ,
    t3.gmv_15d             ,
    t3.sales_vol_15d       ,
    t3.expre_cnt_15d       ,
    t3.clk_cnt_15d         ,
    t3.collect_cnt_15d     ,
    t3.add_cat_cnt_15d     ,
    t3.inter_rate_3_6w     ,
    t3.lrf_rate_9_12w      ,
    t3.nlrf_rate_5_8w      ,
    t3.bs_inter_rate_3_6w  ,
    t3.bs_lrf_rate_9_12w   ,
    t3.bs_nlrf_rate_5_8w   ,
    t3.clk_uv_15d          ,
    t3.mct_score           ,

    t4.price_score           ,
    -- mct_score          ,
    t4.good_cm_rate_score    ,
    t4.good_cm_cnt_score     ,
    t4.good_expre_cnt_score  ,
    t4.good_clk_cnt_score    ,
    t4.good_cart_cnt_score   ,
    t4.good_collect_cnt_score,
    t4.good_sale_vol_score   ,
    t4.inter_rate_score      ,
    t4.nlrf_rate_score       ,
    t4.lrf_rate_score        ,
    t4.gcr_score             ,
    t4.gr_score              ,
    t4.ctr_score                ,

    -- price_score            ,
    -- mct_score              ,
    t5.good_cm_rate_cat_score    ,
    t5.good_cm_cnt_cat_score     ,
    t5.good_expre_cnt_cat_score  ,
    t5.good_clk_cnt_cat_score    ,
    t5.good_cart_cnt_cat_score   ,
    t5.good_collect_cnt_cat_score,
    t5.good_sale_vol_cat_score   ,
    -- inter_rate_score       ,
    -- nlrf_rate_score        ,
    -- lrf_rate_score         ,
    t5.gcr_cat_score             ,
    t5.gr_cat_score              ,
    t5.ctr_cat_score           ,

    t1.base_score          ,
    t1.hot_score           ,
    t1.conversion_score    ,
    t1.honor_score         ,
    t1.overall_score       ,

    t2.base_cat_score      ,
    t2.hot_cat_score       ,
    t2.conversion_cat_score,
    t2.honor_cat_score     ,
    t2.overall_cat_score   ,
    case when t3.avg_inter_days_3_6w = 1 and t4.inter_rate_score > 0 then t4.inter_rate_score + 4
      when t3.avg_inter_days_3_6w = 2 and t4.inter_rate_score > 0 then t4.inter_rate_score + 3
      when t3.avg_inter_days_3_6w = 3 and t4.inter_rate_score > 0 then t4.inter_rate_score + 2
      when t3.avg_inter_days_3_6w = 4 and t4.inter_rate_score > 0 then t4.inter_rate_score + 1
      when t3.avg_inter_days_3_6w = 5 and t4.inter_rate_score > 0 then t4.inter_rate_score + 0.75
      when t3.avg_inter_days_3_6w = 6 and t4.inter_rate_score > 0 then t4.inter_rate_score + 0.5
      else t4.inter_rate_score
    end inter_days_score -- 平滑上网天数评分
  from
    mlb.mlb_vova_rec_b_goods_score_d t1
  inner join
    mlb.mlb_vova_rec_b_catgoods_score_d t2
  on t1.pt = t2.pt and t1.goods_id = t2.goods_id

  inner join
    mlb.mlb_vova_rec_goods_scorebase_data_d t3
  on t1.pt = t3.pt and t1.goods_id = t3.goods_id
  inner join
    mlb.mlb_vova_b_goods_score_details_d t4
  on t1.pt = t4.pt and t1.goods_id = t4.goods_id
  inner join
    mlb.mlb_vova_b_goods_cat_score_details_d t5
  on t1.pt = t5.pt and t1.goods_id = t5.goods_id
  where t1.pt='${cur_date}'
) t1
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
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`


