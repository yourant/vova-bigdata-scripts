#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_taiwan_first_order_buyer_req4892_chenkai"

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

echo "cur_date: $cur_date"
#
sql="
-- 当日用户中未签收过的用户 drop table tmp.tmp_today_order_buyer_${table_suffix}
create table if not EXISTS tmp.tmp_today_order_buyer_${table_suffix} as
 select
 tmp_flsv.pt pt, -- 日期
 tmp_flsv.os_type platform, -- 设备平台
 tmp_flsv.country region_code, -- 国家
 if(to_date(dd.activate_time) == '${cur_date}', 1, 0) is_new, -- 是否当天新激活
 tmp_flsv.device_id, -- 今天浏览的用户
 case when tmp_dog.device_id is null then tmp_flsv.device_id end no_sign_for, -- 是否为历史未签收过的用户
 case when tmp_dog2.device_id is not null then tmp_dog2.device_id end have_order, -- 是否历史下过单
 case when tmp_flsv2.device_id is not null then tmp_flsv2.device_id end firstbuyback, -- 浏览过会场
 case when tmp_dog3.device_id is not null then tmp_dog3.device_id end today_order, -- 当天是否下过单
 nvl(tmp_dog3.order_cnt,0) order_cnt, -- 当天首单数(取消的订单不算)
 nvl(tmp_dog3.order_goods_cnt,0) order_goods_cnt, -- 当天首单子订单数
 nvl(tmp_dog3.gmv,0) gmv, -- 当天首单用户gmv
 nvl(tmp_dog4.order_cnt, 0) all_order_cnt, -- 当天下单数(取消的订单也算在里面)
 nvl(tmp_dog4.cancel_order_cnt, 0) cancel_order_cnt -- 当天取消的订单数
 from
  (select distinct device_id, country, os_type, pt  -- 当天登录用户
  from dwd.dwd_vova_log_screen_view flsv
  where pt = '${cur_date}' and device_id is not null and platform = 'mob') tmp_flsv
 left join
  (select distinct(device_id) device_id     -- 子订单物流状态为已签收或子订单支付状态不为已退款的用户
  from dim.dim_vova_order_goods dog
  where sku_pay_status != 4 and sku_order_status !=2 and dog.pay_time < '${cur_date}') tmp_dog
 on tmp_flsv.device_id = tmp_dog.device_id
 left join
  dim.dim_vova_devices dd  -- 设备信息
 on tmp_flsv.device_id = dd.device_id
 left join
  (select distinct(device_id) device_id     -- 历史下过单的用户
  from dim.dim_vova_order_goods dog
  where dog.sku_order_status > 1 and dog.pay_time < '${cur_date}') tmp_dog2
 on tmp_flsv.device_id = tmp_dog2.device_id
 left join
  (select distinct(device_id)        -- 当天浏览会场的用户
  from dwd.dwd_vova_log_screen_view flsv
  where pt = '${cur_date}' and flsv.page_code =='theme_activity'
  and app_uri like '%firstbuyback%') tmp_flsv2
 on tmp_flsv.device_id = tmp_flsv2.device_id
 left join
  (
  select dog.device_id, -- 当天首单的用户
  count(distinct(dog2.order_id)) order_cnt, -- 当天首单的主订单数
  count(distinct(dog.order_goods_id)) order_goods_cnt, -- 当天首单子订单数
  sum(dog.shipping_fee+dog.shop_price*dog.goods_number) AS gmv -- 当天首单用户gmv
  from
  (select distinct device_id, order_id -- 当天正在进行的首单
  from
  (select *, row_number() over (partition by device_id order by pay_time) as rank
    from dim.dim_vova_order_goods
    where to_date(pay_time) = '${cur_date}' and sku_pay_status != 4 and sku_order_status != 2 and sku_pay_status >= 2) a where a.rank = 1) dog2
  left join
  dim.dim_vova_order_goods dog
  on dog.order_id = dog2.order_id
  group by dog.device_id
  ) tmp_dog3
 on tmp_flsv.device_id = tmp_dog3.device_id
 left join
  (select
  dog.device_id,
  count(distinct(dog.order_id)) order_cnt, -- 当天用户的下单数
  count(distinct(if(dog.sku_order_status = 2,dog.order_id,null))) cancel_order_cnt -- 当天用户的取消单数
  from dim.dim_vova_order_goods dog
  where to_date(dog.pay_time) = '${cur_date}' and dog.device_id is not null
  group by dog.device_id) tmp_dog4
 on tmp_flsv.device_id = tmp_dog4.device_id
;


-- 当天已支付的活动订单
create table if not EXISTS tmp.tmp_pay_order_detail_${table_suffix} as
select
pay_dog.order_goods_id
from
(select * -- 当天正在进行的首单
 from
 (select *, row_number() over (partition by device_id order by pay_time) as rank
  from dim.dim_vova_order_goods
  where to_date(pay_time) = '${cur_date}' and sku_pay_status != 4 and sku_order_status != 2) a where a.rank = 1) pay_dog
left join
(select *
from dim.dim_vova_order_goods
where sku_order_status != 2 and sku_pay_status != 4
) dog2
on pay_dog.device_id = dog2.device_id and pay_dog.pay_time > dog2.pay_time
where dog2.order_goods_id is null
;


-- tmp 当天已退款的活动订单
create table if not EXISTS tmp.tmp_refund_order_detail_${table_suffix} as
 select
 refund_dog.order_goods_id
 from
 (select order_goods_id
   from dwd.dwd_vova_fact_refund
   where to_date(create_time) = '${cur_date}'
 ) fr
 left join
 dim.dim_vova_order_goods refund_dog
 on fr.order_goods_id = refund_dog.order_goods_id
 left join
 -- 退款订单之前的订单： 不是已取消，不是已退款, 如果存在 则退款订单之前有在进行订单，该退款订单不是活动订单
 (select *
 from dim.dim_vova_order_goods d
 where sku_order_status != 2 and sku_pay_status != 4
 ) dog2
 on refund_dog.device_id = dog2.device_id and refund_dog.pay_time > dog2.pay_time
 where refund_dog.order_goods_id is not null and dog2.order_goods_id is null
;

-- 当天签收的活动订单
create table if not EXISTS tmp.tmp_delivered_order_detail_${table_suffix} as
 select
 delivered_dog.order_goods_id
 from
 (select order_goods_id  -- 当天签收的订单
   from dwd.dwd_vova_fact_logistics
   where to_date(delivered_time) = '${cur_date}'
 ) fl
 left join
 dim.dim_vova_order_goods delivered_dog     -- 当天签收的订单详情
 on fl.order_goods_id = delivered_dog.order_goods_id
 left join
 -- 签收订单之前的订单： 不是已取消，不是已退款, 如果存在 则签收订单之前有在进行订单，该签收订单不是活动订单
 (select *
 from dim.dim_vova_order_goods
 where sku_order_status != 2 and sku_pay_status != 4
 ) dog2
 on delivered_dog.device_id = dog2.device_id and delivered_dog.pay_time > dog2.pay_time
 where delivered_dog.order_goods_id is not null and dog2.order_goods_id is null
 ;

-- t1 首单返券活动日常报表
insert overwrite table dwb.dwb_vova_first_order_coupon PARTITION (pt = '${cur_date}')
 select
 nvl(tmp_order.is_new, 'all') is_new,
 tmp_order.region_code region_code,
 nvl(tmp_order.platform, 'all') platform,
 nvl(tmp_order.no_order_buyer_cnt, 0) no_order_buyer_cnt, --未下单用户数
 nvl(tmp_order.dau, 0) dau,-- 当日dau
 nvl(tmp_order.firstbuyback_dau, 0) firstbuyback_dau      ,-- 会场总DAU
 nvl(tmp_order.firstbuyback_order_cnt, 0) firstbuyback_order_cnt,-- 总活动订单数
 nvl(tmp_order.firstbuyback_order_gmv, 0) firstbuyback_order_gmv,-- 当日活动订单GMV
 nvl(tmp_order.all_order_cnt, 0) order_goods_cnt       ,-- 总下单数
 nvl(tmp_order.cancel_order_cnt, 0) cancel_order_cnt      ,-- 取消订单数
 nvl(tmp_refund.refund_order_goods_cnt, 0) refund_order_goods_cnt,-- 退款子订单数
 nvl(tmp_refund.refund_order_goods_gmv, 0) refund_order_goods_gmv-- 退款订单GMV
 from(
   select
   nvl(tmp_no.is_new, 'all') is_new,
   'TW' region_code,
   nvl(tmp_no.platform, 'all') platform,
   count(distinct(tmp_no.have_order)) no_order_buyer_cnt, -- 当日dau中的符合未下单过的用户
   count(distinct(tmp_no.device_id)) dau, -- 当日dau
   count(distinct(tmp_no.firstbuyback)) firstbuyback_dau, -- 会场总DAU
   sum(if(tmp_no.no_sign_for is not null and tmp_no.order_cnt > 0, 1, 0)) firstbuyback_order_cnt, --  总活动订单数(支付成功的订单）
   sum(if(tmp_no.no_sign_for is not null and tmp_no.gmv > 0, tmp_no.gmv, 0)) firstbuyback_order_gmv, --   当日活动订单GMV
   sum(if(tmp_no.no_sign_for is not null and tmp_no.all_order_cnt>0, tmp_no.all_order_cnt, 0)) all_order_cnt, -- 总下单数
   sum(if(tmp_no.no_sign_for is not null and tmp_no.cancel_order_cnt>0, tmp_no.cancel_order_cnt,0)) cancel_order_cnt -- 取消订单数
   from
   tmp.tmp_today_order_buyer_${table_suffix} tmp_no
   where tmp_no.region_code='TW'
   group by cube(
   tmp_no.is_new,
   tmp_no.platform
   )
 ) tmp_order
 left join
 (
   select
   nvl(dd.is_new, 'all') is_new ,
   'TW' region_code,
   nvl(dd.platform, 'all') platform ,
   count(distinct(order_goods_id)) refund_order_goods_cnt,
   sum(gmv) refund_order_goods_gmv,
   count(distinct(order_id)) refund_order_cnt
   from
   (select
   dog.device_id, dog.order_id, dog.order_goods_id,
   dog.shipping_fee+dog.shop_price*dog.goods_number gmv
   from
   tmp.tmp_refund_order_detail_${table_suffix} tmp
   left join
   dim.dim_vova_order_goods dog
   on tmp.order_goods_id = dog.order_goods_id
   ) tmp_order_gmv
   left join
   (select *,
     if(to_date(d.activate_time) = '${cur_date}',1,0) is_new
   from dim.dim_vova_devices d) dd
   on tmp_order_gmv.device_id = dd.device_id
   where dd.is_new is not null and dd.platform in ('ios','android') and dd.region_code = 'TW'
   group by cube(dd.is_new,dd.platform)
 ) tmp_refund
 on tmp_refund.region_code = tmp_order.region_code
 and tmp_refund.platform = tmp_order.platform
 and tmp_refund.is_new = tmp_order.is_new
 ;

-- 活动订单明细
insert overwrite table dwb.dwb_vova_first_order_detail PARTITION (pt = '${cur_date}')
 select
 nvl(nvl(tmp_pay.is_new,tmp_refund.is_new), tmp_delivered.is_new) is_new,
 'TW',
 nvl(nvl(tmp_pay.platform,tmp_refund.platform), tmp_delivered.platform) platform,
 nvl(tmp_pay.firstbuyback_order_cnt, 0) firstbuyback_order_cnt,
 nvl(tmp_delivered.sign_order_cnt, 0) sign_order_cnt,
 nvl(tmp_refund.refund_order_goods_cnt, 0) refund_order_goods_cnt,
 nvl(tmp_refund.refund_order_goods_gmv, 0) refund_order_goods_gmv,
 nvl(tmp_refund.refund_order_cnt      , 0) refund_order_cnt      ,
 nvl(tmp_pay.firstbuyback_order_gmv, 0) firstbuyback_order_gmv,
 nvl(tmp_pay.order_cnt_0_50        , 0) order_cnt_0_50        ,
 nvl(tmp_pay.order_cnt_50_250      , 0) order_cnt_50_250      ,
 nvl(tmp_pay.order_cnt_250_1000    , 0) order_cnt_250_1000    ,
 nvl(tmp_pay.order_cnt_1000        , 0) order_cnt_1000
 from
 (
   select
   nvl(dd.is_new, 'all') is_new,
   nvl(dd.platform, 'all') platform,
   count(distinct(tmp_order_gmv.order_id)) firstbuyback_order_cnt, -- 总活动订单数
   sum(tmp_order_gmv.gmv) firstbuyback_order_gmv,
   sum(if(tmp_order_gmv.gmv > 0 and tmp_order_gmv.gmv <= 50,1,0)) order_cnt_0_50,
   sum(if(tmp_order_gmv.gmv > 50 and tmp_order_gmv.gmv <= 250,1,0)) order_cnt_50_250,
   sum(if(tmp_order_gmv.gmv > 250 and tmp_order_gmv.gmv <= 1000,1,0)) order_cnt_250_1000,
   sum(if(tmp_order_gmv.gmv > 1000,1,0)) order_cnt_1000
   from
   (select
   dog.device_id, dog.order_id,
   sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv
   from
   tmp.tmp_pay_order_detail_${table_suffix} tmp
   left join
   dim.dim_vova_order_goods dog
   on tmp.order_goods_id = dog.order_goods_id
   group by dog.device_id,dog.order_id
   ) tmp_order_gmv
   left join
   (select *,
     if(to_date(d.activate_time) = '${cur_date}',1,0) is_new
   from dim.dim_vova_devices d) dd
   on tmp_order_gmv.device_id = dd.device_id
   where dd.is_new is not null and dd.platform in ('ios','android') and dd.region_code='TW'
   group by cube(
   dd.is_new,dd.platform)
 ) tmp_pay

 left join (
   select
   nvl(dd.is_new, 'all') is_new ,
   nvl(dd.platform, 'all') platform ,
   count(distinct(order_goods_id)) refund_order_goods_cnt,
   sum(gmv) refund_order_goods_gmv,
   count(distinct(order_id)) refund_order_cnt
   from
   (select
   dog.device_id, dog.order_id, dog.order_goods_id,
   dog.shipping_fee+dog.shop_price*dog.goods_number gmv
   from
   tmp.tmp_refund_order_detail_${table_suffix} tmp
   left join
   dim.dim_vova_order_goods dog
   on tmp.order_goods_id = dog.order_goods_id
   ) tmp_order_gmv
   left join
   (select *,
     if(to_date(d.activate_time) = '${cur_date}',1,0) is_new
   from dim.dim_vova_devices d) dd
   on tmp_order_gmv.device_id = dd.device_id
   where dd.is_new is not null and dd.platform in ('ios','android') and dd.region_code='TW'
   group by cube(dd.is_new,dd.platform)
   ) tmp_refund
 on tmp_refund.is_new = tmp_pay.is_new and tmp_refund.platform = tmp_pay.platform

 left join (
   select
   nvl(dd.is_new, 'all') is_new,
   nvl(dd.platform, 'all') platform,
   count(distinct(order_id)) sign_order_cnt
   from
   tmp.tmp_delivered_order_detail_${table_suffix} tmp
   left join
   dim.dim_vova_order_goods dog
   on tmp.order_goods_id = dog.order_goods_id
   left join
   (select *,
     if(to_date(d.activate_time) = '${cur_date}',1,0) is_new
   from dim.dim_vova_devices d) dd
   on dog.device_id = dd.device_id
   where dd.platform in ('ios','android') and is_new is not null and dd.region_code='TW'
   group by cube(dd.is_new,dd.platform)
   ) tmp_delivered
 on tmp_delivered.is_new = tmp_pay.is_new and tmp_delivered.platform = tmp_pay.platform
 ;

-- 用户核销情况
insert overwrite table dwb.dwb_vova_buyer_coupon_use PARTITION (pt = '${cur_date}')
 select
 nvl(tmp1.is_new, 'all') is_new,
 nvl(tmp1.region_code, 'all') region_code,
 nvl(tmp1.platform,'all') platform,
 count(*) order_cnt,
 sum(if(dc.cpn_code is not null, 1,0)) use_cpn_order_cnt,
 sum(if(dc.cpn_code is not null, gmv,0)) use_cpn_order_gmv
 from
 (select
  dog.device_id,
  dog.region_code,
  dog.platform,
  dog.order_id,
  dog.coupon_code,
  tmp_dd.is_new,
  sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv
  from
  tmp.tmp_delivered_order_detail_${table_suffix} fl
  left join
  dim.dim_vova_order_goods dog
  on fl.order_goods_id = dog.order_goods_id
  left join
  (select
  dd.device_id,
  if(to_date(dd.activate_time) = '${cur_date}', '1', '0') is_new
  from
  dim.dim_vova_devices dd where  dd.device_id is not null) tmp_dd
  on dog.device_id = tmp_dd.device_id
  where dog.order_goods_id is not null and dog.platform in ('android','ios') and dog.region_code='TW'
  group by dog.device_id,
  dog.region_code,
  dog.platform,
  dog.order_id,
  dog.coupon_code,
  tmp_dd.is_new
 ) tmp1
 left join
 (select *
 from dim.dim_vova_coupon d
 where d.cpn_cfg_id in  (
   1154839,1154838,1154840,1154841,1154981,1154842,1154981,
 -- 无门槛优惠券
   1154726,1154725,1154724,1154723,1154722
   )
 ) dc
 on dc.cpn_code = tmp1.coupon_code
 group by cube(tmp1.region_code,
 tmp1.platform,
 tmp1.is_new)
 HAVING region_code='TW'
 ;

-- 优惠券核销情况
insert overwrite table dwb.dwb_vova_coupon_use PARTITION (pt = '${cur_date}')
 select
 nvl(tmp_dd.is_new, 'all') is_new,
 nvl(tmp_dog.region_code,'all') region_code,
 nvl(tmp_dog.platform,'all') platform,
 sum(if(dc.cpn_cfg_id=1154839, 1,0)) coupon_20_5_cnt,
 sum(if(dc.cpn_cfg_id=1154839, gmv,0)) coupon_20_5_gmv,
 sum(if(dc.cpn_cfg_id=1154838, 1,0)) coupon_50_10_cnt,
 sum(if(dc.cpn_cfg_id=1154838, gmv,0)) coupon_50_10_gmv,
 sum(if(dc.cpn_cfg_id=1154840, 1,0)) coupon_40_10_cnt,
 sum(if(dc.cpn_cfg_id=1154840, gmv,0)) coupon_40_10_gmv,
 sum(if(dc.cpn_cfg_id=1154841, 1,0)) coupon_100_20_cnt,
 sum(if(dc.cpn_cfg_id=1154841, gmv,0)) coupon_100_20_gmv,
 sum(if(dc.cpn_cfg_id=1154981, 1,0)) coupon_250_50_cnt,
 sum(if(dc.cpn_cfg_id=1154981, gmv,0)) coupon_250_50_gmv,
 sum(if(dc.cpn_cfg_id=1154842, 1,0)) coupon_400_100_cnt,
 sum(if(dc.cpn_cfg_id=1154842, gmv,0)) coupon_400_100_gmv,
 sum(if(dc.cpn_cfg_id=1154981, 1,0)) coupon_1000_200_cnt,
 sum(if(dc.cpn_cfg_id=1154981, gmv,0)) coupon_1000_200_gmv,
 sum(if(dc.cpn_cfg_id=1154726, 1,0)) coupon_1_cnt,
 sum(if(dc.cpn_cfg_id=1154726, gmv,0)) coupon_1_gmv,
 sum(if(dc.cpn_cfg_id=1154725, 1,0)) coupon_2_cnt,
 sum(if(dc.cpn_cfg_id=1154725, gmv,0)) coupon_2_gmv,
 sum(if(dc.cpn_cfg_id=1154724, 1,0)) coupon_3_cnt,
 sum(if(dc.cpn_cfg_id=1154724, gmv,0)) coupon_3_gmv,
 sum(if(dc.cpn_cfg_id=1154723, 1,0)) coupon_4_cnt,
 sum(if(dc.cpn_cfg_id=1154723, gmv,0)) coupon_4_gmv,
 sum(if(dc.cpn_cfg_id=1154722, 1,0)) coupon_5_cnt,
 sum(if(dc.cpn_cfg_id=1154722, gmv,0)) coupon_5_gmv
 from
 dim.dim_vova_coupon dc
 left join
 (select
 dog.device_id,
 dog.region_code,
 dog.platform,
 dog.order_id,
 dog.coupon_code,
 sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv
 from
 dim.dim_vova_order_goods dog
 where to_date(dog.order_time) = '${cur_date}' and dog.order_source='app' and dog.region_code='TW'
 group by dog.device_id, dog.region_code, dog.platform, dog.order_id, dog.coupon_code
 ) tmp_dog
 on dc.cpn_code = tmp_dog.coupon_code
 left join
 (select
 dd.device_id,
 if(to_date(dd.activate_time) = '${cur_date}', 1, 0) is_new
 from
 dim.dim_vova_devices dd) tmp_dd
 on tmp_dog.device_id = tmp_dd.device_id
 where tmp_dd.is_new is not null and tmp_dog.region_code is not null and tmp_dog.platform is not null
 group by cube(tmp_dd.is_new, tmp_dog.region_code, tmp_dog.platform)
 HAVING region_code='TW'
 ;

drop table if EXISTS tmp.tmp_today_order_buyer_${table_suffix};
drop table if EXISTS tmp.tmp_pay_order_detail_${table_suffix};
drop table if EXISTS tmp.tmp_refund_order_detail_${table_suffix};
drop table if EXISTS tmp.tmp_delivered_order_detail_${table_suffix};
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 12G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "${job1_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
