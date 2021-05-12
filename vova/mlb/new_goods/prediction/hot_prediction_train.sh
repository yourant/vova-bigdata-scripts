#!/bin/bash
#指定日期和引擎
cur_date2=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date2=`date -d "-1 day" +%Y-%m-%d`
fi
cur_date=`date -d "+1 day ${cur_date2}" +%Y-%m-%d`
echo "$cur_date2"
echo "$cur_date"

#dependence
#dim_vova_goods
#ads_vova_goods_portrait
#ads_vova_mct_profile

sql="
drop table if exists tmp.tmp_mlb_vova_new_goods_predicte_train_goods;
CREATE TABLE if not exists tmp.tmp_mlb_vova_new_goods_predicte_train_goods
SELECT
 t1.good_id
,t1.cat_id
,t1.first_on_sale_date
,t1.on_sale_date
,t1.test_start_date
,t1.test_end_date
,t1.is_test_scuess
,t1.is_hot
,t1.pt AS obs_date
,t2.first_cat_id
,t2.second_cat_id
,t2.mct_id
from
(
select
 good_id
,cat_id
,split(first_on_sale_date,' ')[0] AS first_on_sale_date
,split(on_sale_date,' ')[0]       AS on_sale_date
,split(test_start_date,' ')[0]    AS test_start_date
,split(test_end_date,' ')[0]      AS test_end_date
,is_test_scuess
,is_hot
,pt
,row_number() OVER (PARTITION BY good_id ORDER BY is_hot DESC ,test_end_date ASC ,pt ASC) AS rank_num
from
mlb.mlb_vova_hot_goods_prediction_base
WHERE to_date(test_end_date)>= date_sub('${cur_date}', 150)
AND to_date(test_end_date)<= date_sub('${cur_date}', 16)
AND pt <= date_add(to_date(test_end_date), 15)
AND pt >= date(test_end_date)
) t1
left join dim.dim_vova_goods t2 on t1.good_id = t2.goods_id
where t1.rank_num = 1
;


INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_prediction_model_feature_data PARTITION (pt = '${cur_date2}')
SELECT
/*+ REPARTITION(5) */
       t1.good_id
     , t1.cat_id
     , t1.first_on_sale_date
     , t1.on_sale_date
     , t1.test_start_date
     , t1.test_end_date
     , t1.is_test_scuess
     , t1.obs_date
     , t1.first_cat_id
     , t1.second_cat_id
     , t1.mct_id
     , agp.shop_price
     , agp.gs_discount
     , agp.shipping_fee
     , agp.comment_cnt_6m
     , agp.comment_good_cnt_6m
     , agp.comment_bad_cnt_6m
     , agp.gmv_1w
     , agp.gmv_15d
     , agp.gmv_1m
     , agp.sales_vol_1w
     , agp.sales_vol_15d
     , agp.sales_vol_1m
     , agp.expre_cnt_1w
     , agp.expre_cnt_15d
     , agp.expre_cnt_1m
     , agp.clk_cnt_1w
     , agp.clk_cnt_15d
     , agp.clk_cnt_1m
     , agp.collect_cnt_1w
     , agp.collect_cnt_15d
     , agp.collect_cnt_1m
     , agp.add_cat_cnt_1w
     , agp.add_cat_cnt_15d
     , agp.add_cat_cnt_1m
     , agp.clk_rate_1w
     , agp.clk_rate_15d
     , agp.clk_rate_1m
     , agp.pay_rate_1w
     , agp.pay_rate_15d
     , agp.pay_rate_1m
     , agp.add_cat_rate_1w
     , agp.add_cat_rate_15d
     , agp.add_cat_rate_1m
     , agp.cr_rate_1w
     , agp.cr_rate_15d
     , agp.cr_rate_1m
     , agp.gs_gender
     , agp.mp_clk_pv_1w
     , agp.mp_clk_pv_15d
     , agp.mp_clk_pv_1m
     , agp.mp_cart_pv_1w
     , agp.mp_cart_pv_15d
     , agp.mp_cart_pv_1m
     , agp.mp_clk_pv_1w_rk
     , agp.mp_clk_pv_15d_rk
     , agp.mp_clk_pv_1m_rk
     , agp.mp_cart_pv_1w_rk
     , agp.mp_cart_pv_15d_rk
     , agp.mp_cart_pv_1m_rk
     , amp.is_new
     , amp.reg_to_now_days
     , amp.average_price
     , amp.cur_gmv
     , amp.cur_uv
     , amp.cur_pv
     , amp.cur_payed_uv
     , amp.cur_ctr
     , amp.bs_cur_ctr
     , amp.cur_cr
     , amp.bs_cur_cr
     , amp.gmv_1m AS mct_gmv_1m
     , amp.atv_2m
     , amp.inter_rate_3_6w
     , amp.bs_inter_rate_3_6w
     , amp.lrf_rate_9_12w
     , amp.bs_lrf_rate_9_12w
     , amp.nlrf_rate_5_8w
     , amp.bs_nlrf_rate_5_8w
     , amp.rep_rate_1mth
     , amp.bs_rep_rate_1mth
     , amp.cohort_rate_1mth
     , amp.bs_cohort_rate_1mth
     , amp.rf_rate_1_3m
     , amp.bs_rf_rate_1_3m
     , amp.proper_rate_5_8w
     , amp.bs_proper_rate_5_8w
     , amp.uv_1m
     , amp.payed_uv_1m
     , amp.bs_avg_cr_1m
     , amp.sell_goods_cnt_1m
     , amp.on_sale_goods_cnt_1m
     , t1.is_hot
FROM tmp.tmp_mlb_vova_new_goods_predicte_train_goods t1
LEFT JOIN (SELECT * FROM ads.ads_vova_goods_portrait agp WHERE agp.pt >= date_sub('${cur_date}', 150)) agp ON t1.good_id = agp.gs_id AND t1.test_end_date = agp.pt
LEFT JOIN (select * from ads.ads_vova_mct_profile amp where amp.pt >= date_sub('${cur_date}', 150)) amp ON t1.mct_id = amp.mct_id AND t1.test_end_date = amp.pt AND t1.first_cat_id = amp.first_cat_id
;



DROP TABLE if exists tmp.tmp_mlb_vova_new_goods_predicte_goods;
CREATE TABLE if not exists tmp.tmp_mlb_vova_new_goods_predicte_goods
SELECT
 t1.good_id
,t1.cat_id
,t1.end_date
,t1.pt
,t2.first_cat_id
,t2.mct_id
from
(
select
 b1.good_id
,b1.cat_id
,date(b1.test_end_date) as end_date
,b1.pt
,row_number() OVER (PARTITION BY b1.good_id ORDER BY b1.is_hot DESC ,date(b1.test_end_date) ASC ,b1.pt ASC) AS rank_num
from
mlb.mlb_vova_hot_goods_prediction_base b1
inner join mlb.mlb_vova_hot_goods_prediction_pre pre on b1.good_id = pre.goods_id AND pre.pt = '${cur_date2}'
WHERE date(b1.test_end_date) = '${cur_date2}'
AND b1.pt = '${cur_date2}'
AND ( ( pre.test_type = 1 AND pre.impressions > 5000 ) or (pre.test_type = 2 and pre.test_goods_result_status IN (6, 7)) )
) t1
left join dim.dim_vova_goods t2 on t1.good_id = t2.goods_id
where t1.rank_num = 1
;

INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_prediction_data PARTITION (pt = '${cur_date2}')
SELECT
/*+ REPARTITION(5) */
       t1.good_id
     , t1.cat_id
     , agp.shop_price
     , agp.gs_discount
     , agp.shipping_fee
     , agp.comment_cnt_6m
     , agp.comment_good_cnt_6m
     , agp.comment_bad_cnt_6m
     , agp.gmv_1w
     , agp.gmv_15d
     , agp.gmv_1m
     , agp.sales_vol_1w
     , agp.sales_vol_15d
     , agp.sales_vol_1m
     , agp.expre_cnt_1w
     , agp.expre_cnt_15d
     , agp.expre_cnt_1m
     , agp.clk_cnt_1w
     , agp.clk_cnt_15d
     , agp.clk_cnt_1m
     , agp.collect_cnt_1w
     , agp.collect_cnt_15d
     , agp.collect_cnt_1m
     , agp.add_cat_cnt_1w
     , agp.add_cat_cnt_15d
     , agp.add_cat_cnt_1m
     , agp.clk_rate_1w
     , agp.clk_rate_15d
     , agp.clk_rate_1m
     , agp.pay_rate_1w
     , agp.pay_rate_15d
     , agp.pay_rate_1m
     , agp.add_cat_rate_1w
     , agp.add_cat_rate_15d
     , agp.add_cat_rate_1m
     , agp.cr_rate_1w
     , agp.cr_rate_15d
     , agp.cr_rate_1m
     , agp.gs_gender
     , agp.mp_clk_pv_1w
     , agp.mp_clk_pv_15d
     , agp.mp_clk_pv_1m
     , agp.mp_cart_pv_1w
     , agp.mp_cart_pv_15d
     , agp.mp_cart_pv_1m
     , agp.mp_clk_pv_1w_rk
     , agp.mp_clk_pv_15d_rk
     , agp.mp_clk_pv_1m_rk
     , agp.mp_cart_pv_1w_rk
     , agp.mp_cart_pv_15d_rk
     , agp.mp_cart_pv_1m_rk
     , amp.is_new
     , amp.reg_to_now_days
     , amp.average_price
     , amp.cur_gmv
     , amp.cur_uv
     , amp.cur_pv
     , amp.cur_payed_uv
     , amp.cur_ctr
     , amp.bs_cur_ctr
     , amp.cur_cr
     , amp.bs_cur_cr
     , amp.gmv_1m AS mct_gmv_1m
     , amp.atv_2m
     , amp.inter_rate_3_6w
     , amp.bs_inter_rate_3_6w
     , amp.lrf_rate_9_12w
     , amp.bs_lrf_rate_9_12w
     , amp.nlrf_rate_5_8w
     , amp.bs_nlrf_rate_5_8w
     , amp.rep_rate_1mth
     , amp.bs_rep_rate_1mth
     , amp.cohort_rate_1mth
     , amp.bs_cohort_rate_1mth
     , amp.rf_rate_1_3m
     , amp.bs_rf_rate_1_3m
     , amp.proper_rate_5_8w
     , amp.bs_proper_rate_5_8w
     , amp.uv_1m
     , amp.payed_uv_1m
     , amp.bs_avg_cr_1m
     , amp.sell_goods_cnt_1m
     , amp.on_sale_goods_cnt_1m
FROM tmp.tmp_mlb_vova_new_goods_predicte_goods t1
LEFT JOIN ads.ads_vova_goods_portrait agp ON t1.good_id = agp.gs_id AND t1.end_date = agp.pt AND agp.pt = '${cur_date2}'
LEFT JOIN ads.ads_vova_mct_profile amp ON t1.mct_id = amp.mct_id AND t1.end_date = amp.pt AND t1.first_cat_id = amp.first_cat_id AND amp.pt = '${cur_date2}'
;

"
echo "$sql"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=mlb_vova_hot_goods_prediction_data" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=180" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

hive -e "
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
select
*
from
mlb.mlb_vova_hot_goods_prediction_model_feature_data
where
pt = '${cur_date2}'
;
" | sed 's/[\t]/,/g' > /tmp/ads_hot_goods_prediction_model_feature_data_"${cur_date2}".csv

if [ $? -ne 0 ];then
  exit 1
fi

aws s3 mv /tmp/ads_hot_goods_prediction_model_feature_data_"${cur_date2}".csv  s3://vova-mlb/REC/data/hot_prediction/model_feature_data/pt="${cur_date2}"/model_feature_data.csv

if [ $? -ne 0 ];then
  exit 1
fi

hive -e "
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
select
*
from
mlb.mlb_vova_hot_goods_prediction_data
where
pt = '${cur_date2}'
;
" | sed 's/[\t]/,/g' > /tmp/ads_hot_goods_prediction_data_"${cur_date2}".csv

if [ $? -ne 0 ];then
  exit 1
fi

aws s3 mv /tmp/ads_hot_goods_prediction_data_"${cur_date2}".csv  s3://vova-mlb/REC/data/hot_prediction/predict_data/pt="${cur_date2}"/hot_goods_prediction_data.csv

if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=data_hot_prediction_vova --from=data --to=mlb --jtype=1D --retry=0

if [ $? -ne 0 ];then
  exit 1
fi