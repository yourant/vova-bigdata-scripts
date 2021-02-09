#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
s3path=`date -d "${pre_date} 00:00:00" +%Y/%m/%d`
pre_month=`date -d "1 month ago ${pre_date}" +%Y-%m-%d`
echo "pre_month=${pre_month}"

pre_month_start==`date -d "1 month ago ${pre_date}" +%Y-%m-01`
echo "pre_month_start=${pre_month_start}"

pre_2_month=`date -d "2 month ago ${pre_date}" +%Y-%m-%d`
echo "pre_2_month=${pre_2_month}"

###逻辑sql
#依赖的表，dim_category，dim_goods，fact_pay，fact_log_goods_click，fact_refund,fact_logistics
sql="
--每日在售商品统计
insert overwrite table ads.ads_vova_on_sale_goods_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
g.goods_id,
g.merchant_id,
c.first_cat_id
from ods_vova_vts.ods_vova_goods_arc g
inner join dim.dim_vova_category c on c.cat_id = g.cat_id
where g.pt ='$pre_date' and g.is_on_sale=1 and g.is_display=1 and g.is_delete=0;

insert overwrite table ads.ads_vova_mct_profile PARTITION (pt = '${pre_date}')
--step1 商家信息
select
/*+ REPARTITION(1) */
t0.mct_id,
t0.first_cat_id,
t0.is_new,
t0.reg_to_now_days,
t0.on_sale_flag,
t0.average_price,
t1.cur_gmv,
t2.cur_uv,
t2.cur_pv,
t1.cur_payed_uv,
t2.cur_pv/t3.pv_goods_impression as cur_ctr,
(nvl(t2.cur_pv,0)+0.02*10)/(nvl(t3.pv_goods_impression,0)+10) as bs_cur_ctr,
t1.cur_payed_uv/t2.cur_uv as cur_cr,
(nvl(t1.cur_payed_uv,0)+0.02*100)/(nvl(t2.cur_uv,0)+100) as bs_cur_cr,
t4.gmv_1m,
t5.atv_2m,
t8.inter_rate_3_6w,
nvl(t8.bs_inter_rate_3_6w,0.9) as bs_inter_rate_3_6w,
t10.lrf_rate_9_12w,
nvl(t10.bs_lrf_rate_9_12w,0.1) as bs_lrf_rate_9_12w,
t9.nlrf_rate_5_8w,
nvl(t9.bs_nlrf_rate_5_8w,0.1) as bs_nlrf_rate_5_8w,
t6.rep_rate_1mth,
nvl(t6.bs_rep_rate_1mth,0.4) as bs_rep_rate_1mth,
t7.cohort_rate_1mth,
nvl(t7.bs_cohort_rate_1mth,0.8) as bs_cohort_rate_1mth,
t11.rf_rate_1_3m,
nvl(t11.bs_rf_rate_1_3m,0.2) as bs_rf_rate_1_3m,
t12.proper_rate_5_8w,
nvl(t12.bs_proper_rate_5_8w,0.55) as proper_rate_5_8w,
t13.uv_1m,
t4.payed_uv_1m,
(nvl(t4.payed_uv_1m,0)+0.02*100)/(nvl(t13.uv_1m,0)+100) as bs_avg_cr_1m,
nvl(t4.sell_goods_cnt_1m,0) as sell_goods_cnt_1m,
nvl(t14.on_sale_goods_cnt_1m,0) as on_sale_goods_cnt_1m,
nvl(t4.sell_goods_cnt_1m / t14.on_sale_goods_cnt_1m,0)  as turnover_rate_1m,
(nvl(t4.sell_goods_cnt_1m,0)+0.1*100)/(nvl(t14.on_sale_goods_cnt_1m,0)+100)  as bs_turnover_rate_1m
from
(
select
t2.mct_id,
t2.first_cat_id,
t2.is_new,
t2.reg_to_now_days,
t2.on_sale_flag,
t2.average_price
from
(
select
m.mct_id,
t1.first_cat_id,
case when datediff('${pre_date}',m.reg_time)<=90 then 1 else 0 end is_new,
datediff('${pre_date}',m.reg_time) as reg_to_now_days,
case when m.is_delete='0' and m.is_banned='0' then 1 else 0 end on_sale_flag,
t1.average_price
from dim.dim_vova_merchant m
left join
(
select
mct_id,
first_cat_id,
avg(shop_price + shipping_fee) as average_price
from dim.dim_vova_goods
where is_on_sale =1
and shop_price + shipping_fee < 10000
group by mct_id,first_cat_id
) t1 on t1.mct_id = m.mct_id
) t2 where t2.mct_id is not null and t2.first_cat_id is not null
) t0
left join
(
--step2当天销售
select
mct_id,
first_cat_id,
sum(shop_price * goods_number + shipping_fee) as cur_gmv,
count(distinct device_id) as cur_payed_uv
from
dwd.dwd_vova_fact_pay
where to_date(pay_time)='${pre_date}'
group by mct_id,first_cat_id
) t1 on t0.mct_id=t1.mct_id and t0.first_cat_id = t1.first_cat_id
left join
(
--step3当天点击
select
g.mct_id,
g.first_cat_id,
count(distinct flgc.device_id) as cur_uv,
count(*) as cur_pv
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
where pt='${pre_date}' and platform='mob'
and g.first_cat_id is not null and g.mct_id is not null
group by g.mct_id,g.first_cat_id
) t2 on t0.mct_id=t2.mct_id and t0.first_cat_id = t2.first_cat_id
left join
(
--step4当天曝光
select
g.mct_id,
g.first_cat_id,
count(*) as pv_goods_impression
from dwd.dwd_vova_log_goods_impression flgi
left join dim.dim_vova_goods g on flgi.virtual_goods_id = g.virtual_goods_id
where pt='${pre_date}' and platform='mob'
and g.first_cat_id is not null and g.mct_id is not null
group by g.mct_id,g.first_cat_id
) t3 on t0.mct_id=t3.mct_id and t0.first_cat_id = t3.first_cat_id
left join
(
--step5最近30天gmv
select
mct_id,
first_cat_id,
sum(shipping_fee + shop_price * goods_number) as gmv_1m,
count(distinct device_id) as payed_uv_1m,
count(distinct goods_id) as sell_goods_cnt_1m
from dwd.dwd_vova_fact_pay
where pay_time > date_sub('${pre_date}', 30)
group by mct_id,first_cat_id
) t4 on t0.mct_id=t4.mct_id and t0.first_cat_id = t4.first_cat_id
left join
(
--step6最近60天客单价
select
mct_id,
first_cat_id,
(sum(shipping_fee + shop_price * goods_number) / count(distinct buyer_id)) as atv_2m
from dwd.dwd_vova_fact_pay
where pay_time > date_sub('${pre_date}', 60)
group by mct_id,first_cat_id
) t5 on t0.mct_id=t5.mct_id and t0.first_cat_id = t5.first_cat_id
left join
(
--step7月复购
select
t1.mct_id,
t1.first_cat_id,
count(distinct t2.buyer_id_1)/count(distinct t1.buyer_id) as rep_rate_1mth,
(count(distinct t2.buyer_id_1) +0.4*5 )/(count(distinct t1.buyer_id)+5) as bs_rep_rate_1mth
from
(
select
distinct goods_id,
mct_id,
first_cat_id,
buyer_id
from dwd.dwd_vova_fact_pay
where year(pay_time)=year('${pre_2_month}') and month(pay_time)= month('${pre_2_month}')
) t1
left join
(
select
distinct buyer_id as buyer_id_1
from dwd.dwd_vova_fact_pay
where year(pay_time)=year('${pre_month}') and month(pay_time)= month('${pre_month}')
) t2 on t1.buyer_id=t2.buyer_id_1
group by t1.mct_id,t1.first_cat_id
) t6 on t0.mct_id=t6.mct_id and t0.first_cat_id = t6.first_cat_id
left join
(
--step8月留存
select
t1.mct_id,
t1.first_cat_id,
count(distinct t2.device_id_1) / count(distinct t1.device_id) as cohort_rate_1mth,
(count(distinct t2.device_id_1)+ 0.8*5 )/ (count(distinct t1.device_id)+5 )as bs_cohort_rate_1mth
from
(
select
distinct mct_id,
first_cat_id,
device_id
from dwd.dwd_vova_fact_pay
where year(pay_time)=year('${pre_2_month}') and month(pay_time)= month('${pre_2_month}')
) t1
left join
(
select
distinct device_id as device_id_1
from dwd.dwd_vova_fact_start_up
where pt>='$pre_month_start' and year(start_up_date)=year('${pre_month}') and month(start_up_date)= month('${pre_month}')
) t2 on t1.device_id=t2.device_id_1
group by t1.mct_id,t1.first_cat_id
) t7 on t0.mct_id=t7.mct_id and t0.first_cat_id = t7.first_cat_id
left join
(
--step9七天上网率 7天前再往前一个月的确认订单，上网时间减确认时间小于7天的子订单数/7天前再往前一个月的确认子订单数
select
t1.mct_id,
t1.first_cat_id,
sum(t1.so_order_cnt_3_6w)/count(t1.order_goods_id) as inter_rate_3_6w,
(sum(t1.so_order_cnt_3_6w)+0.9*5)/(count(t1.order_goods_id)+5) as bs_inter_rate_3_6w
from
(
select
og.mct_id,
c.first_cat_id,
og.order_goods_id,
case when datediff(fl.valid_tracking_date,fl.confirm_time)<7  and og.sku_pay_status>1 then 1 else 0 end so_order_cnt_3_6w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id=og.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pre_date}', date(og.confirm_time)) between 6 and 36
) t1
group by t1.mct_id,t1.first_cat_id
) t8 on t0.mct_id=t8.mct_id and t0.first_cat_id = t8.first_cat_id
left join
(
--step10 5到8周非物流退款率 63天前再往前一个月的确认订单，非物流退款时间减确认时间小于63天的子订单数/63天前再往前一个月的确认子订单数
select
t1.mct_id,
t1.first_cat_id,
sum(t1.nlrf_order_cnt_5_8w)/count(t1.order_goods_id) as nlrf_rate_5_8w,
(sum(t1.nlrf_order_cnt_5_8w)+0.1*5)/(count(t1.order_goods_id)+5) as bs_nlrf_rate_5_8w
from
(
select
og.mct_id,
c.first_cat_id,
og.order_goods_id,
case when datediff(fr.audit_time,og.confirm_time)<63 and  fr.refund_reason_type_id not in (8,9) and fr.refund_type_id=2 and fr.rr_audit_status='audit_passed' and og.sku_pay_status>1 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pre_date}', date(og.confirm_time)) between 62 and 92
) t1
group by t1.mct_id,t1.first_cat_id
) t9 on t0.mct_id=t9.mct_id and t0.first_cat_id = t9.first_cat_id
left join
(
--step11 9到12周物流退款率 84天前再往前一个月的确认订单，物流退款时间减确认时间小于84天的子订单数/84天前再往前一个月的确认子订单数
select
t1.mct_id,
t1.first_cat_id,
sum(t1.lrf_order_cnt_9_12w)/count(t1.order_goods_id) as lrf_rate_9_12w,
(sum(t1.lrf_order_cnt_9_12w)+0.1*5)/(count(t1.order_goods_id)+5) as bs_lrf_rate_9_12w
from
(
select
og.mct_id,
c.first_cat_id,
og.order_goods_id,
case when datediff(fr.audit_time,og.confirm_time)<84 and fr.refund_reason_type_id in (8,9) and fr.refund_type_id=2 and og.sku_pay_status>1
and fr.rr_audit_status='audit_passed' then 1 else 0 end lrf_order_cnt_9_12w
from  dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pre_date}', date(og.confirm_time)) between 83 and 113
) t1
group by t1.mct_id,t1.first_cat_id
) t10 on t0.mct_id=t10.mct_id and t0.first_cat_id = t10.first_cat_id
left join
(
--step12前90天至前30天订单的退款率
select
t1.mct_id,
t1.first_cat_id,
sum(t1.rf_order_cnt_1_3w)/count(t1.order_goods_id) as rf_rate_1_3m,
(sum(t1.rf_order_cnt_1_3w)+0.2*5)/(count(t1.order_goods_id)+5) as bs_rf_rate_1_3m
from
(
select
og.mct_id,
c.first_cat_id,
og.order_goods_id,
case when fr.sku_pay_status=4 then 1 else 0 end rf_order_cnt_1_3w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pre_date}', date(og.confirm_time)) between 30 and 90
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1
group by t1.mct_id,t1.first_cat_id
) t11 on t0.mct_id=t11.mct_id and t0.first_cat_id = t11.first_cat_id
left join
(
--step13 5到8周妥投率 63天前再往前一个月的确认订单，妥投时间减确认时间小于63天的子订单数/63天前再往前一个月的确认子订单数
select
t1.mct_id,
t1.first_cat_id,
sum(t1.proper_order_cnt_5_8w)/count(distinct t1.order_goods_id) as proper_rate_5_8w,
(sum(t1.proper_order_cnt_5_8w)+0.55*5)/(count(distinct t1.order_goods_id)+5)as bs_proper_rate_5_8w
from
(
select
og.mct_id,
c.first_cat_id,
og.order_goods_id,
case when datediff(fl.delivered_time,og.confirm_time)<63 and fl.process_tag = 'Delivered'  and og.sku_pay_status>1 then 1 else 0 end proper_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id = og.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pre_date}', date(og.confirm_time)) between 62 and 92
) t1
group by t1.mct_id,t1.first_cat_id
) t12 on t0.mct_id=t12.mct_id and t0.first_cat_id = t12.first_cat_id
left join
(
select
g.mct_id,
g.first_cat_id,
count(distinct flgc.device_id) as uv_1m
from dwd.dwd_vova_log_goods_click flgc
left join dim.dim_vova_goods g on flgc.virtual_goods_id = g.virtual_goods_id
where datediff('${pre_date}', pt) <=30 and platform='mob'
and g.first_cat_id is not null and g.mct_id is not null
group by g.mct_id,g.first_cat_id
) t13 on t0.mct_id= t13.mct_id and t0.first_cat_id = t13.first_cat_id
left join
(
select
mct_id,
first_cat_id,
count(distinct goods_id) as on_sale_goods_cnt_1m
from ads.ads_vova_on_sale_goods_d
where pt >='$pre_month' and pt <='$pre_date'
group by mct_id,first_cat_id
) t14 on t0.mct_id= t14.mct_id and t0.first_cat_id = t14.first_cat_id
"
spark-sql --executor-memory 6G --conf "spark.app.name=ads_vova_mct_profile_d_zhangyin"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
#hadoop fs -mkdir -p s3://vomkt-emr-rec/data/ads_mct_profile/$s3path
#if [ $? -ne 0 ]; then
#  exit 1
#fi
#hadoop distcp hdfs:///user/hive/warehouse/ads.db/ads_mct_profile/pt=${pre_date}/* s3://vomkt-emr-rec/data/ads_mct_profile/$s3path
##如果脚本失败，则报错
#if [ $? -ne 0 ]; then
#  exit 1
#fi