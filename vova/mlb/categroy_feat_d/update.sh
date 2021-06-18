#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi


spark-sql \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.app.name=mlb_vova_category_feat_d" \
-e"
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE mlb.mlb_vova_category_feat_d partition(pt='$cur_date',feat_type)
select  /*+ REPARTITION(6) */
feat, weight, feat_type
from (
         select first_cat_id feat,
                sum(clk_cnt_1m) weight,
                'first_cat_id' feat_type
         from ads.ads_vova_goods_portrait a
         where a.pt = '${cur_date}'
         group by first_cat_id
         union all
         select second_cat_id feat,
                sum(clk_cnt_1m) weight,
                'second_cat_id' feat_type
         from ads.ads_vova_goods_portrait a
         where a.pt = '${cur_date}'
         group by second_cat_id
         union all
         select cat_id feat,
                sum(clk_cnt_1m) weight,
                'cat_id' feat_type
         from ads.ads_vova_goods_portrait a
         where a.pt = '${cur_date}'
         group by cat_id
         union all
         select brand_id feat,
                sum(clk_cnt_1m) weight,
                'brand_id' feat_type
         from ads.ads_vova_goods_portrait a
         where a.pt = '${cur_date}'
         group by brand_id
         union all
         select mct_id feat,
                sum(clk_cnt_1m) weight,
                'mct_id' feat_type
         from ads.ads_vova_goods_portrait a
         where a.pt = '${cur_date}'
         group by mct_id
         union all
         select device_model feat,
                count(device_id) weight,
                'device_model' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
         group by device_model
         union all
         select language_id feat,
                count(device_id) weight,
                'language_id' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
         group by language_id
         union all
         select country_id feat,
                count(device_id) weight,
                'country_id' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
         group by country_id
         union all
         select geo_city feat,
                count(device_id) weight,
                'geo_city' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
         group by geo_city
         union all
         select clk_from feat,
                count(device_id) weight,
                'query' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
           and a.page_code = 'search_result'
         group by clk_from
         union all
         select imsi feat,
                count(device_id) weight,
                'imsi' feat_type
         from mlb.mlb_vova_user_behave_link_d a
         where a.pt <= '${cur_date}'
           and a.pt >= date_sub('${cur_date}', 29)
         group by imsi
     ) t

"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



