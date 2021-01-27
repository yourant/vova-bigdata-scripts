#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
insert overwrite table dwb.dwb_vova_mct_rank PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
to_date('$cur_date') as event_date,
cast(t1.rank as string) rank,
cast(t1.first_cat_id as string) first_cat_id,
nvl(t1.mct_cnt,0) as mct_cnt,
nvl(t2.dau,0) as dau,
nvl(t3.payed_order_num,0) as payed_order_num,
nvl(t3.gmv,0) as gmv,
nvl(t3.payed_uv,0) as payed_uv,
round(nvl(t4.ct_dau,0)) as ct_dau
from
(
select
first_cat_id,
rank,
count(mct_id) as mct_cnt
from ads.ads_vova_mct_rank
where pt='$cur_date'
group by rank,first_cat_id
) t1
left join
(
select
amr.rank,
g.first_cat_id,
count(distinct flgc.device_id) as dau
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
left join ads.ads_vova_mct_rank amr on g.first_cat_id = amr.first_cat_id and g.mct_id = amr.mct_id
where flgc.pt ='$cur_date' and amr.pt='$cur_date' and flgc.platform='mob' and g.first_cat_id is not null
group by
amr.rank,
g.first_cat_id
) t2 on t1.rank = t2.rank and t1.first_cat_id = t2.first_cat_id
left join
(
select
amr.rank,
fp.first_cat_id,
count(distinct fp.buyer_id) as payed_uv,
count(distinct fp.order_id) as payed_order_num,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv
from dwd.dwd_vova_fact_pay fp
left join ads.ads_vova_mct_rank amr on fp.first_cat_id = amr.first_cat_id and fp.mct_id = amr.mct_id
where to_date(fp.pay_time)='$cur_date' and amr.pt='$cur_date' and fp.first_cat_id is not null
group by
amr.rank,
fp.first_cat_id
) t3 on t1.rank = t3.rank and t1.first_cat_id = t3.first_cat_id
left join
(
select
t0.rank,
t0.first_cat_id,
sum(t0.ct_dau) as ct_dau
from
(
select
amr.rank,
g.first_cat_id,
flgc.geo_country,
count(distinct flgc.device_id) * nvl(max(cc.rate*100),1) as ct_dau
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
left join tmp.rpt_mct_rank_detail_country_cr cc on flgc.geo_country = cc.country
left join ads.ads_vova_mct_rank amr on g.first_cat_id = amr.first_cat_id and g.mct_id = amr.mct_id
where flgc.pt ='$cur_date' and amr.pt='$cur_date' and flgc.platform='mob' and g.first_cat_id is not null
group by amr.rank,g.first_cat_id,flgc.geo_country
) t0
group by
t0.rank,
t0.first_cat_id
) t4 on t1.rank = t4.rank and t1.first_cat_id = t4.first_cat_id;

insert overwrite table dwb.dwb_vova_mct_rank_detail PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
to_date('$cur_date') as event_date,
t1.rank,
t1.first_cat_id,
t1.mct_id,
t1.gmv_rank,
t1.bs_inter_rate_3_6w,
t1.bs_lrf_rate_9_12w,
t1.bs_nlrf_rate_5_8w,
t1.bs_rep_rate_1mth,
nvl(t2.payed_order_num,0) as payed_order_num,
nvl(t2.gmv,0) as gmv,
nvl(t3.dau,0) as dau,
round(nvl(t4.ct_dau,0)) as ct_dau
from
(
select
rank,
mct_id,
first_cat_id,
gmv_rank,
bs_inter_rate_3_6w,
bs_lrf_rate_9_12w,
bs_nlrf_rate_5_8w,
bs_rep_rate_1mth
from ads.ads_vova_mct_rank
where pt='$cur_date'
) t1
left join
(
select
amr.rank,
amr.mct_id,
fp.first_cat_id,
count(distinct fp.order_id) as payed_order_num,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv
from dwd.dwd_vova_fact_pay fp
left join ads.ads_vova_mct_rank amr on fp.first_cat_id = amr.first_cat_id and fp.mct_id = amr.mct_id
where to_date(fp.pay_time)='$cur_date' and amr.pt='$cur_date' and fp.first_cat_id is not null
group by
amr.rank,
fp.first_cat_id,
amr.mct_id
) t2 on t1.rank =t2.rank and t1.first_cat_id = t2.first_cat_id and t1.mct_id =t2.mct_id
left join
(
select
g.mct_id,
g.first_cat_id,
count(distinct flgc.device_id) as dau
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
where pt='$cur_date' and platform='mob'
and g.first_cat_id is not null and g.mct_id is not null
group by g.mct_id,g.first_cat_id
) t3 on t1.first_cat_id = t3.first_cat_id and t1.mct_id =t3.mct_id
left join
(
select
t0.mct_id,
t0.first_cat_id,
sum(t0.ct_dau) as ct_dau
from
(
select
g.mct_id,w2
g.first_cat_id,
flgc.geo_country,
count(distinct flgc.device_id) * nvl(max(cc.rate*100),1) as ct_dau
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
left join tmp.rpt_mct_rank_detail_country_cr cc on flgc.geo_country = cc.country
where pt='$cur_date' and platform='mob'
and g.first_cat_id is not null and g.mct_id is not null
group by g.mct_id,g.first_cat_id,flgc.geo_country
) t0 group by t0.mct_id,t0.first_cat_id
) t4 on t1.first_cat_id = t4.first_cat_id and t1.mct_id =t4.mct_id
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql -e  --conf "spark.app.name=dwb_vova_mct_rank_zhangyin"  "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi





