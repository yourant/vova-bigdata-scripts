#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
cur_date2=`date -d "+1 day ${cur_date}" +%Y-%m-%d`
echo "$cur_date"
echo "$cur_date2"

##dependance
#ods_vova_vbai.ods_vova_images_vector
#ads.ads_vova_min_price_goods_d
#ads.ads_vova_goods_portrait
#ads.ads_vova_mct_rank
#ads.ads_vova_mct_profile
#ads.ads_vova_hot_search_word
#dim.dim_vova_goods
#dim.dim_vova_merchant
#dwd.dwd_vova_fact_pay

sql="
INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_group PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
mpg.group_number,
mpg.goods_id,
nvl(10000 * gmv_1m / expre_cnt_1m, 0) as gmv_cr
from
(
select
mpg.group_number,
sum(agp.expre_cnt_1m) as expre_cnt_1m,
sum(agp.gmv_1m) as gmv_1m
from
ads.ads_vova_min_price_goods_d mpg
inner join ads.ads_vova_goods_portrait agp on agp.gs_id = mpg.goods_id
where mpg.strategy='c'
and mpg.pt = '${cur_date}'
and agp.pt = '${cur_date}'
group by mpg.group_number
having expre_cnt_1m > 5000
and 10000 * gmv_1m / expre_cnt_1m > 80
) t1
inner join ads.ads_vova_min_price_goods_d mpg on t1.group_number = mpg.group_number
inner join dim.dim_vova_goods dg on dg.goods_id = mpg.goods_id
where mpg.strategy='c'
and mpg.pt = '${cur_date}'

union all

select /*+ REPARTITION(1) */
concat(dg.goods_id,'-', dg.cat_id, '-', 'single') as group_number,
dg.goods_id,
nvl(10000 * gmv_1m / expre_cnt_1m, 0) as gmv_cr
from
dim.dim_vova_goods dg
left join ads.ads_vova_min_price_goods_d mpg on dg.goods_id = mpg.goods_id and mpg.strategy='c' and mpg.pt = '${cur_date}'
inner join ads.ads_vova_goods_portrait agp on agp.gs_id = dg.goods_id
where agp.pt = '${cur_date}'
and mpg.goods_id is null
and agp.expre_cnt_1m > 5000
and 10000 * agp.gmv_1m / agp.expre_cnt_1m > 80

;


INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_group PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
mpg.group_number,
mpg.goods_id,
dg.cat_id,
dg.brand_id
from
(
select
mpg.group_number,
sum(agp.expre_cnt_1m) as expre_cnt_1m
from
ads.ads_vova_min_price_goods_d mpg
inner join ads.ads_vova_goods_portrait agp on agp.gs_id = mpg.goods_id
where mpg.strategy='c'
and mpg.pt = '${cur_date}'
and agp.pt = '${cur_date}'
group by mpg.group_number
having expre_cnt_1m < 300
) t1
inner join ads.ads_vova_min_price_goods_d mpg on t1.group_number = mpg.group_number
inner join dim.dim_vova_goods dg on dg.goods_id = mpg.goods_id
where mpg.strategy='c'
and mpg.pt = '${cur_date}'
and dg.is_on_sale = 1

union all

select
/*+ REPARTITION(5) */
concat(dg.goods_id,'-', dg.cat_id, '-', 'single') as group_number,
dg.goods_id,
dg.cat_id,
dg.brand_id
from
dim.dim_vova_goods dg
left join ads.ads_vova_min_price_goods_d mpg on dg.goods_id = mpg.goods_id and mpg.strategy='c' and mpg.pt = '${cur_date}'
inner join ads.ads_vova_goods_portrait agp on agp.gs_id = dg.goods_id
where agp.pt = '${cur_date}'
and mpg.goods_id is null
and agp.expre_cnt_1m < 300
and dg.is_on_sale = 1

;

INSERT OVERWRITE TABLE mlb.mlb_vova_hot_goods_group_vec
select /*+ REPARTITION(50) */
agg.goods_id,
nvl(iv.img_vec, '') as img_vec
from
mlb.mlb_vova_hot_goods_group agg
left join ods_vova_vbai.ods_vova_images_vector iv on agg.goods_id = iv.goods_id
where agg.pt = '${cur_date}'
;

INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_group_vec
select /*+ REPARTITION(50) */
agg.goods_id,
nvl(iv.img_vec, '') as img_vec
from
mlb.mlb_vova_new_goods_group agg
left join ods_vova_vbai.ods_vova_images_vector iv on agg.goods_id = iv.goods_id
where agg.pt = '${cur_date}'
;

INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_base PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
dg.goods_id,
dg.brand_id,
dg.goods_name,
dg.shop_price,
dg.goods_weight,
dg.first_cat_id,
dg.second_cat_id,
dg.cat_id,
dg.keywords,
dm.mct_id,
dm.reg_time,
dm.mct_cat_desc,
dm.sale_region_desc,
dm.logistics_type_desc,
dm.is_banned,
dm.mct_status,
dm.first_customer_buy_time,
nvl(amr.rank, -1) as mct_rank,
nvl(amp.bs_avg_cr_1m, -1) as bs_avg_cr_1m,
nvl(amp.bs_cohort_rate_1mth, -1) as bs_cohort_rate_1mth,
nvl(amp.bs_inter_rate_3_6w, -1) as bs_inter_rate_3_6w,
nvl(amp.bs_rf_rate_1_3m, -1) as bs_rf_rate_1_3m,
round(nvl(sale_goods_1m.sale_goods_1m, 0) / sale_cnt, 4) as mct_pop_prob,
if(dg.second_cat_id is null, -1, sec_most.avg_price - (dg.shop_price + dg.shipping_fee)) as second_diff_mean_price,
if(dg.second_cat_id is null, -1, sec_median.median_price - (dg.shop_price + dg.shipping_fee)) as second_diff_mid_price,
if(dg.second_cat_id is null, -1, if(mct_most_sec.second_cat_id = dg.second_cat_id,1,0)) as is_mct_pop_second_cat,
if(dg.first_cat_id is null, -1, if(mct_most_fir.first_cat_id = dg.first_cat_id,1,0)) as is_mct_pop_fisrt_cat,
if(amp.atv_2m is null, -1, amp.atv_2m - (dg.shop_price + dg.shipping_fee)) as diff_atv_2m,
if(amp.on_sale_goods_cnt_1m is null, -1, round(amp.payed_uv_1m / amp.on_sale_goods_cnt_1m, 4)) as avg_goods_payed,
nvl(agp.sales_vol_1m, 0) as ord_cnt,
dg.last_on_time as timestamp,
if(agp.expre_cnt_1m > 5000, if(agp.sales_vol_1m > 0, 1, 0), -1) as goods_tag,
nvl(agp.expre_cnt_1m, -1) as expre_cnt_1m
from
dim.dim_vova_goods dg
left join dim.dim_vova_merchant dm on dg.mct_id = dm.mct_id
left join ads.ads_vova_mct_rank amr on amr.mct_id = dg.mct_id and amr.first_cat_id = dg.first_cat_id and amr.pt = '${cur_date}'
left join ads.ads_vova_mct_profile amp on amp.mct_id = dg.mct_id and amp.first_cat_id = dg.first_cat_id and amp.pt = '${cur_date}'
left join ads.ads_vova_goods_portrait agp on agp.gs_id = dg.goods_id and agp.pt = '${cur_date}'
left join (
select
dg.mct_id,
count(*) as sale_cnt
from
dim.dim_vova_goods dg
group by dg.mct_id
) sale_goods on dg.mct_id = sale_goods.mct_id
left join
(
select
count(*) as sale_goods_1m,
agp.mct_id
from
ads.ads_vova_goods_portrait agp
where agp.pt = '${cur_date}'
and agp.expre_cnt_1m > 5000
and agp.sales_vol_1m > 0
group by agp.mct_id
) sale_goods_1m on dg.mct_id = sale_goods_1m.mct_id
left join
(
select
second_cat_id,
avg(shop_price + shipping_fee) as avg_price
from
(
select
dg.second_cat_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
RANK() OVER(PARTITION BY dg.second_cat_id ORDER BY goods_number desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
group by second_cat_id
) sec_most on sec_most.second_cat_id = dg.second_cat_id
left join
(
select
second_cat_id,
goods_id,
shop_price + shipping_fee as median_price,
cnt,
rn1,
rn2
from
(
select
second_cat_id,
goods_id,
shop_price,
shipping_fee,
cnt,
rn1,
ROW_NUMBER() OVER(PARTITION BY second_cat_id ORDER BY rn1) AS rn2
from
(
select
dg.second_cat_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
count(*) OVER(PARTITION BY dg.second_cat_id) AS cnt,
ROW_NUMBER() OVER(PARTITION BY dg.second_cat_id ORDER BY t1.goods_number desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
where dg.second_cat_id is not null
) t2
where rn1 / cnt > 0.5
) t3
where rn2 = 1
) sec_median on sec_median.second_cat_id = dg.second_cat_id

left join
(
select
second_cat_id,
mct_id
from
(
select
dg.second_cat_id,
dg.mct_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
ROW_NUMBER() OVER(PARTITION BY dg.mct_id ORDER BY goods_number desc,gmv desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
) mct_most_sec on mct_most_sec.mct_id = dg.mct_id

left join
(
select
first_cat_id,
mct_id
from
(
select
dg.first_cat_id,
dg.mct_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
ROW_NUMBER() OVER(PARTITION BY dg.mct_id ORDER BY goods_number desc,gmv desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
) mct_most_fir on mct_most_fir.mct_id = dg.mct_id
where
dg.first_on_time > date_sub('${cur_date}', 30)
and dg.first_on_time <= '${cur_date}'
;



INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_predict PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
dg.goods_id,
dg.brand_id,
dg.goods_name,
dg.shop_price,
dg.goods_weight,
dg.first_cat_id,
dg.second_cat_id,
dg.cat_id,
dg.keywords,
dm.mct_id,
dm.reg_time,
dm.mct_cat_desc,
dm.sale_region_desc,
dm.logistics_type_desc,
dm.is_banned,
dm.mct_status,
dm.first_customer_buy_time,
nvl(amr.rank, -1) as mct_rank,
nvl(amp.bs_avg_cr_1m, -1) as bs_avg_cr_1m,
nvl(amp.bs_cohort_rate_1mth, -1) as bs_cohort_rate_1mth,
nvl(amp.bs_inter_rate_3_6w, -1) as bs_inter_rate_3_6w,
nvl(amp.bs_rf_rate_1_3m, -1) as bs_rf_rate_1_3m,
round(nvl(sale_goods_1m.sale_goods_1m, 0) / sale_cnt, 4) as mct_pop_prob,
if(dg.second_cat_id is null, -1, sec_most.avg_price - (dg.shop_price + dg.shipping_fee)) as second_diff_mean_price,
if(dg.second_cat_id is null, -1, sec_median.median_price - (dg.shop_price + dg.shipping_fee)) as second_diff_mid_price,
if(dg.second_cat_id is null, -1, if(mct_most_sec.second_cat_id = dg.second_cat_id,1,0)) as is_mct_pop_second_cat,
if(dg.first_cat_id is null, -1, if(mct_most_fir.first_cat_id = dg.first_cat_id,1,0)) as is_mct_pop_fisrt_cat,
if(amp.atv_2m is null, -1, amp.atv_2m - (dg.shop_price + dg.shipping_fee)) as diff_atv_2m,
if(amp.on_sale_goods_cnt_1m is null, -1, round(amp.payed_uv_1m / amp.on_sale_goods_cnt_1m, 4)) as avg_goods_payed,
nvl(agp.sales_vol_1m, 0) as ord_cnt,
dg.last_on_time as timestamp,
nvl(agp.expre_cnt_1m, -1) as expre_cnt_1m
from
dim.dim_vova_goods dg
inner join (select distinct goods_id from mlb.mlb_vova_new_goods_group where pt = '${cur_date}') new_goods on new_goods.goods_id = dg.goods_id
left join dim.dim_vova_merchant dm on dg.mct_id = dm.mct_id
left join ads.ads_vova_mct_rank amr on amr.mct_id = dg.mct_id and amr.first_cat_id = dg.first_cat_id and amr.pt = '${cur_date}'
left join ads.ads_vova_mct_profile amp on amp.mct_id = dg.mct_id and amp.first_cat_id = dg.first_cat_id and amp.pt = '${cur_date}'
left join ads.ads_vova_goods_portrait agp on agp.gs_id = dg.goods_id and agp.pt = '${cur_date}'
left join (
select
dg.mct_id,
count(*) as sale_cnt
from
dim.dim_vova_goods dg
group by dg.mct_id
) sale_goods on dg.mct_id = sale_goods.mct_id
left join
(
select
count(*) as sale_goods_1m,
agp.mct_id
from
ads.ads_vova_goods_portrait agp
where agp.pt = '${cur_date}'
and agp.expre_cnt_1m > 5000
and agp.sales_vol_1m > 0
group by agp.mct_id
) sale_goods_1m on dg.mct_id = sale_goods_1m.mct_id
left join
(
select
second_cat_id,
avg(shop_price + shipping_fee) as avg_price
from
(
select
dg.second_cat_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
RANK() OVER(PARTITION BY dg.second_cat_id ORDER BY goods_number desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
group by second_cat_id
) sec_most on sec_most.second_cat_id = dg.second_cat_id
left join
(
select
second_cat_id,
goods_id,
shop_price + shipping_fee as median_price,
cnt,
rn1,
rn2
from
(
select
second_cat_id,
goods_id,
shop_price,
shipping_fee,
cnt,
rn1,
ROW_NUMBER() OVER(PARTITION BY second_cat_id ORDER BY rn1) AS rn2
from
(
select
dg.second_cat_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
count(*) OVER(PARTITION BY dg.second_cat_id) AS cnt,
ROW_NUMBER() OVER(PARTITION BY dg.second_cat_id ORDER BY t1.goods_number desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
where dg.second_cat_id is not null
) t2
where rn1 / cnt > 0.5
) t3
where rn2 = 1
) sec_median on sec_median.second_cat_id = dg.second_cat_id

left join
(
select
second_cat_id,
mct_id
from
(
select
dg.second_cat_id,
dg.mct_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
ROW_NUMBER() OVER(PARTITION BY dg.mct_id ORDER BY goods_number desc,gmv desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
) mct_most_sec on mct_most_sec.mct_id = dg.mct_id

left join
(
select
first_cat_id,
mct_id
from
(
select
dg.first_cat_id,
dg.mct_id,
dg.goods_id,
dg.shop_price,
dg.shipping_fee,
ROW_NUMBER() OVER(PARTITION BY dg.mct_id ORDER BY goods_number desc,gmv desc) AS rn1
from
(
select
fp.goods_id,
sum(fp.goods_number) as goods_number,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time) > date_sub('${cur_date}', 30)
and date(fp.pay_time) <= '${cur_date}'
group by fp.goods_id
) sec_pay_all
inner join dim.dim_vova_goods dg on dg.goods_id = sec_pay_all.goods_id
where dg.second_cat_id is not null
) sec_pay_all2
where sec_pay_all2.rn1 = 1
) mct_most_fir on mct_most_fir.mct_id = dg.mct_id
where dg.is_on_sale = 1
;

INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_group_key_words_base PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(5) */
ag.goods_id,
dg.cat_id,
dg.brand_id,
dg.goods_name
FROM
mlb.mlb_vova_new_goods_group ag
inner join dim.dim_vova_goods dg on ag.goods_id = dg.goods_id
 where ag.pt = '${cur_date}'
 and dg.is_on_sale = 1
;

INSERT OVERWRITE TABLE mlb.mlb_vova_new_goods_group_key_words PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(5) */
hot_word,
sum(search_counts) as search_counts
FROM
ads.ads_vova_hot_search_word h1
inner join (
select max(pt) as max_pt
from
ads.ads_vova_hot_search_word
) t1 on t1.max_pt = h1.pt
group by hot_word
;



"

spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=mlb_vova_new_goods" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=180" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=data_new_goods_rec --from=data --to=mlb --jtype=1D --retry=0

if [ $? -ne 0 ];then
  exit 1
fi


