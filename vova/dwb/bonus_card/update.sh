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

job_name="dwb_vova_bonus_card_req6571_chenkai_${cur_date}"

###逻辑sql
sql="
-- 表一：购卡链路转化率监控
insert overwrite table dwb.dwb_vova_bonus_card_conversion partition(pt='${cur_date}')
select
/*+ REPARTITION(1) */
  'vova' datasource,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(distinct(buyer_id)) dau, -- 非月卡DAU
  count(distinct(vouchercard_hp_unpay_buyer_id))   vouchercard_hp_unpay_uv, -- 未购卡会场页面UV（非pending）
  count(distinct(payment_bonus_card_buyer_id)) payment_bonus_card_uv, --   月卡支付页面UV
  count(distinct(paid_buyer_id)) paid_uv, -- 开卡成功UV
  count(distinct(pending_buyer_id)) pending_uv -- 开卡pendingUV
from
(
  select
  /*+ REPARTITION(100) */
    nvl(db.region_code, 'NA') region_code,
    nvl(tmp1.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel, -- 渠道
    CASE WHEN datediff(tmp1.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(tmp1.pt,dd.activate_time)>=1 and datediff(tmp1.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(tmp1.pt,dd.activate_time)>=7 and datediff(tmp1.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    dd.device_id device_id,
    if(tmp1.status is null or tmp1.status != 'paid' or from_unixtime(tmp1.start_time, 'yyyy-MM-dd')='${cur_date}', tmp1.buyer_id, null) buyer_id, --   非月卡DAU
    if(tmp1.page_code = 'vouchercard_hp_unpay' and (tmp1.status is null or tmp1.status != 'pending'), tmp1.buyer_id, null) vouchercard_hp_unpay_buyer_id, -- 未购卡会场页面UV(非pending)
    if(tmp1.status is not null and create_time='${cur_date}', tmp1.buyer_id, null)   payment_bonus_card_buyer_id, -- 月卡支付页面UV
    if(tmp1.status = 'paid' and create_time = '${cur_date}' , tmp1.buyer_id, null) paid_buyer_id, -- 开卡成功UV
    if(tmp1.status = 'pending' and create_time = '${cur_date}', tmp1.buyer_id, null) pending_buyer_id -- 开卡pendingUV
  from
  (
    select
    /*+ REPARTITION(200) */
      COALESCE(flsv.buyer_id, vbc.user_id) buyer_id,
      COALESCE(flsv.pt, from_unixtime(vbc.start_time, 'yyyy-MM-dd')) pt,
      flsv.page_code page_code,
      flsv.os_type os_type,
      vbc.status status,
      vbc.start_time start_time,
      vbc.create_time create_time
    from
    (
      select
        tmp_card.user_id user_id,
        if(tmp_log.new_status is not null, tmp_log.new_status, tmp_card.status) status,
        if(tmp_log.new_status ='paid', tmp_log.update_time, tmp_card.start_time) start_time,
        if(tmp_log.update_time is not null, from_unixtime(tmp_log.update_time, 'yyyy-MM-dd'), to_date(tmp_card.create_time, 'yyyy-MM-dd')) create_time
      from
      (
        select
          *
        from
          ods_vova_vts.ods_vova_bonus_card
        where status = 'pending'
          or (status='paid' and from_unixtime(end_time, 'yyyy-MM-dd')>='${cur_date}')
          or (status='unpaid' and to_date(update_time, 'yyyy-MM-dd') = '${cur_date}')
      ) tmp_card
      left join
      (
        select
          *
        from
        (
          select *,
            row_number() over(partition by user_id order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt = '${cur_date}'
        ) where row = 1
      ) tmp_log
      on tmp_card.user_id = tmp_log.user_id
    ) vbc
    full outer join
    (
      select * from
      dwd.dwd_vova_log_screen_view
      where pt = '${cur_date}'
        and platform = 'mob'
        and datasource = 'vova'
        and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
    ) flsv
    on flsv.buyer_id = vbc.user_id
  ) tmp1
  left join
    dim.dim_vova_buyers db
  on tmp1.buyer_id = db.buyer_id
  left join
    dim.dim_vova_devices dd
  on db.datasource = dd.datasource and db.current_device_id = dd.device_id
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on tmp1.buyer_id = abgs.buyer_id
)
group by cube(region_code, os_type, main_channel, is_new, gmv_stage)
;

-- 表二：优惠券发放与核销
create table if not exists tmp.tmp_bonus_card_coupon_use_${table_suffix} as
  select
    fc.order_id order_id,
    sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv
  from
    dwd.dwd_vova_fact_coupon fc
  left join
    dim.dim_vova_coupon dc
  on fc.datasource = dc.datasource and fc.cpn_code = dc.cpn_code
  left join
    dim.dim_vova_order_goods dog
  on fc.datasource = dog.datasource and fc.cpn_code = dog.coupon_code
  where fc.datasource = 'vova' and to_date(fc.used_time) = '${cur_date}'
    and dc.cpn_cfg_type_id in ('470', '469', '468', '467', '466', '465', '464', '463', '461', '460', '459', '458', '457', '456', '455', '454', '453', '452', '451', '450', '449', '448', '447', '446', '445', '444', '443', '442', '441', '439')
    and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
    and dog.pay_status >=1
  group by fc.order_id
;

insert overwrite table dwb.dwb_vova_bonus_card_coupon partition(pt='${cur_date}')
select
/*+ REPARTITION(1) */
  datasource,
  region_code,
  platform,
  gmv_stage,
  tmp1.cpn_cfg_type_id cpn_cfg_type_id,
  nvl(cpn_cfg_type_name, 'NA') cpn_cfg_type_name,
  get_cpn_cnt, -- 优惠券已领数量
  get_cpn_user_cnt, -- 优惠券已领人数
  get_cpn_sum, -- 优惠券发放金额
  use_cpn_cnt, -- 优惠券使用数量
  use_cpn_user_cnt, -- 优惠券使用人数
  use_cpn_sum, -- 优惠券使用金额
  nvl(cpn_order_gmv, 0) -- 优惠券带来GMV
from
(
  select
    'vova' datasource,
    nvl(region_code, 'all') region_code,
    nvl(platform, 'all') platform,
    nvl(gmv_stage, 'all') gmv_stage,
    nvl(cpn_cfg_type_id, 'all') cpn_cfg_type_id,
    count(distinct get_cpn_id) get_cpn_cnt, -- 优惠券已领数量
    count(distinct get_cpn_buyer_id) get_cpn_user_cnt, -- 优惠券已领人数
    sum(get_cpn_sum) get_cpn_sum, -- 优惠券发放金额

    count(distinct use_cpn_id) use_cpn_cnt, -- 优惠券使用数量
    count(distinct use_cpn_user_id) use_cpn_user_cnt, -- 优惠券使用人数
    sum(use_cpn_sum) use_cpn_sum, -- 优惠券使用金额
    sum(cpn_order_gmv) cpn_order_gmv -- 优惠券带来GMV
  from
  (
    select
      nvl(db.region_code, 'NA') region_code,
      nvl(db.platform, 'NA') platform,
      nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
      nvl(dc.cpn_cfg_type_id, '0') cpn_cfg_type_id,
      if(to_date(give_time) ='${cur_date}', fc.cpn_id, null) get_cpn_id, -- 优惠券已领数量
      if(to_date(give_time) ='${cur_date}', fc.buyer_id, null) get_cpn_buyer_id, -- 优惠券已领人数
      if(to_date(give_time) ='${cur_date}' and dc.cpn_cfg_val is not null, dc.cpn_cfg_val, 0) get_cpn_sum, --     优惠券发放金额
      if(to_date(used_time) ='${cur_date}', fc.cpn_id, null) use_cpn_id, -- 优惠券使用数量
      if(to_date(used_time) ='${cur_date}', fc.buyer_id, null) use_cpn_user_id, -- 优惠券使用人数
      if(to_date(used_time) ='${cur_date}' and dc.cpn_cfg_val is not null, dc.cpn_cfg_val, 0) use_cpn_sum, --     优惠券使用金额
      tmp1.gmv cpn_order_gmv -- 优惠券带来GMV
    from
      dwd.dwd_vova_fact_coupon fc
    left join
      dim.dim_vova_coupon dc
    on fc.datasource = dc.datasource and fc.cpn_id = dc.cpn_id
    left join
      dim.dim_vova_buyers db
    on fc.datasource = db.datasource and fc.buyer_id = db.buyer_id
    left join
        (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
    on fc.buyer_id = abgs.buyer_id
    left join
      tmp.tmp_bonus_card_coupon_use_${table_suffix} tmp1
    on fc.order_id = tmp1.order_id
    where fc.datasource = 'vova' and (to_date(give_time) ='${cur_date}' or to_date(used_time) ='${cur_date}')
      and dc.cpn_cfg_type_id in ('470', '469', '468', '467', '466', '465', '464', '463', '461', '460', '459', '458', '457', '456', '455', '454', '453', '452', '451', '450', '449', '448', '447', '446', '445', '444', '443', '442', '441', '439')
      and db.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
      and tmp1.order_id is not null
  )
  group by cube(region_code, platform, gmv_stage, cpn_cfg_type_id)
) tmp1
left join
(
  select
    distinct cpn_cfg_type_id, cpn_cfg_type_name
  from
    dim.dim_vova_coupon
) dc
on tmp1.cpn_cfg_type_id = dc.cpn_cfg_type_id
;

-- 表三：月卡用户交易数据
create table if not exists tmp.tmp_bonus_card_pay_${table_suffix} as
select
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  nvl(sum(bonus_card_price), 0) bonus_card_price, -- 开卡费用总和
  count(distinct(paid_buyer_id)) paid_buyer_cnt, -- 开卡成功UV
  count(distinct(pending_buyer_id)) pending_buyer_cnt --开卡pendingUV
from
(
  select
    nvl(db.region_code, 'NA') region_code,
    nvl(db.platform, 'NA') platform,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff('${cur_date}',dd.activate_time)<=0 THEN 'new'
      WHEN datediff('${cur_date}',dd.activate_time)>=1 and datediff('${cur_date}',dd.activate_time)<6 THEN '2-7'
      WHEN datediff('${cur_date}',dd.activate_time)>=7 and datediff('${cur_date}',dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    if(vbc.status = 'paid', vbc.price, null) bonus_card_price, -- 月卡价格
    if(vbc.status = 'paid', vbc.user_id, null) paid_buyer_id, -- 开卡成功用户id
    if(vbc.status = 'pending', vbc.user_id, null) pending_buyer_id -- 开卡pending用户id
  from
  (
    select
      tmp1.user_id user_id,
      tmp2.price price,
      tmp1.new_status status
      -- to_date(tmp1.update_time, 'yyyy-MM-dd') start_time
    from
    (
      select
        *
      from
      (
        select *,
          row_number() over(partition by user_id order by update_time desc) row
        from
          dwd.dwd_vova_fact_log_bonus_card
        where pt = '${cur_date}'
      ) where row = 1
    ) tmp1
    left join
    (
      select
      *
      from
        ods_vova_vts.ods_vova_bonus_card
      where
        status='paid' and from_unixtime(end_time, 'yyyy-MM-dd')>='${cur_date}'
    ) tmp2
    on tmp1.user_id = tmp2.user_id
  ) vbc
  left join
    dim.dim_vova_buyers db
  on vbc.user_id = db.buyer_id
  left join
    dim.dim_vova_devices dd
  on db.current_device_id = dd.device_id
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on vbc.user_id = abgs.buyer_id
  where
    -- (status = 'paid' and from_unixtime(start_time, 'yyyy-MM-dd') = '${cur_date}')
    -- or status = 'pending'
    -- and
    db.datasource = 'vova' and dd.datasource = 'vova'
    and db.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
)
group by cube(region_code, platform, main_channel, is_new, gmv_stage)
;

create table if not exists tmp.tmp_bonus_card_gmv_${table_suffix} as
select
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  sum(gmv) gmv, -- 大盘GMV
  sum(bonus_card_gmv) bonus_card_gmv, -- 月卡用户GMV
  count(distinct bonus_card_order_id) bonus_card_order_cnt, -- 月卡用户支付成功订单量
  count(distinct bonus_card_first_order_id) bonus_card_first_order_cnt,   -- 月卡用户首单订单量
  count(distinct bonus_card_buyer_id) bonus_card_device_cnt, -- 月卡用户支付成功UV
  nvl(sum(cpn_cfg_val), 0) cpn_cfg_val -- 月卡优惠券抵扣金额
from
(
  select
    nvl(dog.region_code, 'NA') region_code,
    nvl(dog.platform, 'NA') platform,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff('${cur_date}',dd.activate_time)<=0 THEN 'new'
      WHEN datediff('${cur_date}',dd.activate_time)>=1 and datediff('${cur_date}',dd.activate_time)<6 THEN '2-7'
      WHEN datediff('${cur_date}',dd.activate_time)>=7 and datediff('${cur_date}',dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    dog.gmv gmv, -- 大盘GMV
    if(vbc.status is not null, dog.gmv, 0) bonus_card_gmv, -- 月卡用户GMV
    if(vbc.status is not null, dog.order_id, null) bonus_card_order_id, -- 月卡用户支付成功订单量
    if(vbc.status is not null and dog.order_id = db.first_order_id, dog.order_id, null) bonus_card_first_order_id,   -- 月卡用户首单订单量
    if(vbc.status is not null, dog.buyer_id, null) bonus_card_buyer_id, -- 月卡用户支付成功UV
    if(vbc.status is not null and dc.cpn_cfg_type_id in ('470', '469', '468', '467', '466', '465', '464', '463', '461', '460', '459',     '458', '457', '456', '455', '454', '453', '452', '451', '450', '449', '448', '447', '446', '445', '444', '443', '442', '441',       '439'), dc.cpn_cfg_val, null) cpn_cfg_val -- 月卡优惠券抵扣金额
  from
  (
    select distinct
      datasource,
      buyer_id,
      order_id,
      coupon_code,
      region_code,
      platform,
      sum(shipping_fee+shop_price*goods_number) gmv
    from
      dim.dim_vova_order_goods dog
    where to_date(order_time) = '${cur_date}' and platform in ('android','ios')
    and datasource = 'vova' and pay_status >= 1
    and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
    group by datasource, buyer_id, order_id, coupon_code, region_code, platform
  ) dog
  left join
  (
    select
      tmp1.user_id user_id,
      if(tmp2.new_status is not null, tmp2.new_status, tmp1.status) status
    from
      ods_vova_vts.ods_vova_bonus_card tmp1
    left join
    (
      select
        *
      from
      (
        select *,
          row_number() over(partition by user_id order by update_time desc) row
        from
          dwd.dwd_vova_fact_log_bonus_card
        where pt <= '${cur_date}'
      ) where row = 1
    ) tmp2
    on tmp1.user_id = tmp2.user_id
    where status='paid' and from_unixtime(end_time, 'yyyy-MM-dd')>='${cur_date}'
      and if(tmp2.pt is not null, tmp2.pt, from_unixtime(tmp1.start_time, 'yyyy-MM-dd')) <= '${cur_date}'
  ) vbc
  on dog.buyer_id = vbc.user_id
  left join
    dim.dim_vova_buyers db
  on dog.datasource = db.datasource and dog.buyer_id = db.buyer_id
  left join
    dim.dim_vova_devices dd
  on dog.datasource = dd.datasource and db.current_device_id = dd.device_id
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on dog.buyer_id = abgs.buyer_id
  left join
    dim.dim_vova_coupon dc
  on dog.datasource = dc.datasource and dog.coupon_code = dc.cpn_code
)
group by cube(region_code, platform, main_channel, is_new, gmv_stage)
;

create table if not exists tmp.tmp_bonus_card_screen_view_${table_suffix} as
select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(distinct bonus_card_paid_buyer_id) bonus_card_paid_dau, -- 月卡DAU
  count(distinct bonus_card_pending_buyer_id) bonus_card_pending_dau, -- 月卡pendingDAU
  count(distinct vouchercard_hp_paid_buyer_id) vouchercard_hp_paid_uv, -- 已购卡会场页面UV
  count(distinct push_switch_open_buyer_id) push_switch_open_uv, -- 推送开关打开UV
  count(distinct vouchercard_hp_paid_day2_buyer_id) vouchercard_hp_paid_day2_uv, -- 已购卡会场页面UV
  count(distinct vouchercard_hp_paid_day7_buyer_id) vouchercard_hp_paid_day7_uv, -- 已购卡会场页面UV
  count(distinct vouchercard_hp_paid_day14_buyer_id) vouchercard_hp_paid_day14_uv, -- 已购卡会场页面UV
  count(distinct vouchercard_hp_paid_day28_buyer_id) vouchercard_hp_paid_day28_uv, -- 已购卡会场页面UV

  count(distinct paid_app_buyer_id) paid_app_uv, -- 已购卡appUV
  count(distinct paid_app_day2_buyer_id)  paid_app_day2_uv,  -- 已购卡app次日UV
  count(distinct paid_app_day7_buyer_id)  paid_app_day7_uv,  -- 已购卡app7日UV
  count(distinct paid_app_day14_buyer_id) paid_app_day14_uv, -- 已购卡app14日UV
  count(distinct paid_app_day28_buyer_id) paid_app_day28_uv  -- 已购卡app28日UV

from
(
  select
    nvl(flsv.pt, 'NA') pt,
    nvl(flsv.geo_country, 'NA') region_code,
    nvl(flsv.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff(flsv.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flsv.pt,dd.activate_time)>=1 and datediff(flsv.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flsv.pt,dd.activate_time)>=7 and datediff(flsv.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    if(vbc.status = 'paid', flsv.buyer_id, null) bonus_card_paid_buyer_id, -- 月卡DAU
    if(vbc.status = 'pending', flsv.buyer_id, null) bonus_card_pending_buyer_id, -- 月卡pendingDAU
    if(flsv.page_code = 'vouchercard_hp_paid', flsv.buyer_id, null) vouchercard_hp_paid_buyer_id, -- 已购卡会场页面UV
    if(flsv.page_code = 'vouchercard_hp_paid' and vbc.push_switch = '1', vbc.user_id, null) push_switch_open_buyer_id, -- 推送开关打开uv
    if(flsv.page_code = 'vouchercard_hp_paid' and flsv_today.page_code = 'vouchercard_hp_paid' and flsv.pt = date_sub(flsv_today.pt, 1) and flsv_today.device_id is not null, flsv.buyer_id, null) vouchercard_hp_paid_day2_buyer_id, -- 已购卡会场页面次日UV
    if(flsv.page_code = 'vouchercard_hp_paid' and flsv_today.page_code = 'vouchercard_hp_paid' and flsv.pt = date_sub(flsv_today.pt, 7) and flsv_today.device_id is not null, flsv.buyer_id, null) vouchercard_hp_paid_day7_buyer_id, -- 已购卡会场页面七日UV
    if(flsv.page_code = 'vouchercard_hp_paid' and flsv_today.page_code = 'vouchercard_hp_paid' and flsv.pt = date_sub(flsv_today.pt, 14) and flsv_today.device_id is not null, flsv.buyer_id, null) vouchercard_hp_paid_day14_buyer_id, -- 已购卡会场页面14日UV
    if(flsv.page_code = 'vouchercard_hp_paid' and flsv_today.page_code = 'vouchercard_hp_paid' and flsv.pt = date_sub(flsv_today.pt, 28) and flsv_today.device_id is not null, flsv.buyer_id, null) vouchercard_hp_paid_day28_buyer_id, -- 已购卡会场页面28日UV

    if(vbc.status = 'paid', flsv.buyer_id, null) paid_app_buyer_id, -- 已购卡app
    if(vbc.status = 'paid' and flsv.pt = date_sub(flsv_today.pt, 1) and flsv_today.device_id is not null and flsv.device_id is not null, flsv.buyer_id, null)  paid_app_day2_buyer_id,  -- 已购卡app次日UV
    if(vbc.status = 'paid' and flsv.pt = date_sub(flsv_today.pt, 7) and flsv_today.device_id is not null and flsv.device_id is not null, flsv.buyer_id, null)  paid_app_day7_buyer_id,  -- 已购卡app七日UV
    if(vbc.status = 'paid' and flsv.pt = date_sub(flsv_today.pt, 14) and flsv_today.device_id is not null and flsv.device_id is not null, flsv.buyer_id, null) paid_app_day14_buyer_id, -- 已购卡app14日UV
    if(vbc.status = 'paid' and flsv.pt = date_sub(flsv_today.pt, 28) and flsv_today.device_id is not null and flsv.device_id is not null, flsv.buyer_id, null) paid_app_day28_buyer_id  -- 已购卡app28日UV

  from
  (
    select
      distinct pt, buyer_id, geo_country, os_type, device_id, page_code
    from
    dwd.dwd_vova_log_screen_view
    where datasource = 'vova'
      and pt in ('${cur_date}', date_sub('${cur_date}', 1), date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 28))
      and platform = 'mob'
      and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com' 
  ) flsv
  left join
    dim.dim_vova_devices dd
  on flsv.device_id = dd.device_id
  left join
  (
    select
      pt,
      user_id,
      status,
      push_switch,
      start_time,
      end_time
    from
    (
      -- 当天的用户最新状态
      select
        '${cur_date}' pt,
        COALESCE(tmp1.user_id, tmp2.user_id) user_id,
        if(tmp1.new_status is not null, tmp1.new_status, tmp2.status) status,
        tmp2.push_switch push_switch,
        if(tmp1.update_time is not null, from_unixtime(tmp1.update_time, 'yyyy-MM-dd'), from_unixtime(tmp2.start_time, 'yyyy-MM-dd')) start_time,
        from_unixtime(tmp2.end_time, 'yyyy-MM-dd') end_time
      from
      (
        select  -- 从log中获取用户当天的状态
          user_id,
          new_status,
          update_time
        from
        (
          select *,
            row_number() over(partition by user_id, pt order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt <= '${cur_date}' and new_status != 'unpaid'
        ) where row = 1
      ) tmp1
      full outer join
      (
        select  -- 从mysql中获取用户当天的状态
          user_id,
          status,
          push_switch,
          start_time,
          end_time
        from
          ods_vova_vts.ods_vova_bonus_card tmp2
        where from_unixtime(tmp2.start_time, 'yyyy-MM-dd') <= '${cur_date}' and from_unixtime(tmp2.end_time, 'yyyy-MM-dd') >= '${cur_date}'
      ) tmp2
      on tmp1.user_id = tmp2.user_id
      where tmp1.update_time is not null or from_unixtime(tmp2.start_time, 'yyyy-MM-dd') < '${cur_date}'
      union all
      -- 前一天的用户最新状态
      select
        date_sub('${cur_date}', 1) pt,
        COALESCE(tmp1.user_id, tmp2.user_id) user_id,
        if(tmp1.new_status is not null, tmp1.new_status, tmp2.status) status,
        tmp2.push_switch push_switch,
        if(tmp1.update_time is not null, from_unixtime(tmp1.update_time, 'yyyy-MM-dd'), from_unixtime(tmp2.start_time, 'yyyy-MM-dd')) start_time,
        from_unixtime(tmp2.end_time, 'yyyy-MM-dd') end_time
      from
      (
        select  -- 从log中获取用户当天的状态
          user_id,
          new_status,
          update_time
        from
        (
          select *,
            row_number() over(partition by user_id, pt order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt <= date_sub('${cur_date}', 1) and new_status != 'unpaid'
        ) where row = 1
      ) tmp1
      full outer join
      (
        select  -- 从mysql中获取用户当天的状态
          user_id,
          status,
          push_switch,
          start_time,
          end_time
        from
          ods_vova_vts.ods_vova_bonus_card tmp2
        where from_unixtime(tmp2.start_time, 'yyyy-MM-dd') <= date_sub('${cur_date}', 1)
          and from_unixtime(tmp2.end_time, 'yyyy-MM-dd') >= date_sub('${cur_date}', 1)
      ) tmp2
      on tmp1.user_id = tmp2.user_id
      where tmp1.update_time is not null or from_unixtime(tmp2.start_time, 'yyyy-MM-dd') < date_sub('${cur_date}', 1)
      union all
      -- 前七天的用户最新状态
      select
        date_sub('${cur_date}', 7) pt,
        COALESCE(tmp1.user_id, tmp2.user_id) user_id,
        if(tmp1.new_status is not null, tmp1.new_status, tmp2.status) status,
        tmp2.push_switch push_switch,
        if(tmp1.update_time is not null, from_unixtime(tmp1.update_time, 'yyyy-MM-dd'), from_unixtime(tmp2.start_time, 'yyyy-MM-dd')) start_time,
        from_unixtime(tmp2.end_time, 'yyyy-MM-dd') end_time
      from
      (
        select  -- 从log中获取用户当天的状态
          user_id,
          new_status,
          update_time
        from
        (
          select *,
            row_number() over(partition by user_id, pt order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt <= date_sub('${cur_date}', 7) and new_status != 'unpaid'
        ) where row = 1
      ) tmp1
      full outer join
      (
        select  -- 从mysql中获取用户当天的状态
          user_id,
          status,
          push_switch,
          start_time,
          end_time
        from
          ods_vova_vts.ods_vova_bonus_card tmp2
        where from_unixtime(tmp2.start_time, 'yyyy-MM-dd') <= date_sub('${cur_date}', 7)
          and from_unixtime(tmp2.end_time, 'yyyy-MM-dd') >= date_sub('${cur_date}', 7)
      ) tmp2
      on tmp1.user_id = tmp2.user_id
      where tmp1.update_time is not null or from_unixtime(tmp2.start_time, 'yyyy-MM-dd') < date_sub('${cur_date}', 7)
      union all
      -- 前14天的用户最新状态
      select
        date_sub('${cur_date}', 14) pt,
        COALESCE(tmp1.user_id, tmp2.user_id) user_id,
        if(tmp1.new_status is not null, tmp1.new_status, tmp2.status) status,
        tmp2.push_switch push_switch,
        if(tmp1.update_time is not null, from_unixtime(tmp1.update_time, 'yyyy-MM-dd'), from_unixtime(tmp2.start_time, 'yyyy-MM-dd')) start_time,
        from_unixtime(tmp2.end_time, 'yyyy-MM-dd') end_time
      from
      (
        select  -- 从log中获取用户当天的状态
          user_id,
          new_status,
          update_time
        from
        (
          select *,
            row_number() over(partition by user_id, pt order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt <= date_sub('${cur_date}', 14) and new_status != 'unpaid'
        ) where row = 1
      ) tmp1
      full outer join
      (
        select  -- 从mysql中获取用户当天的状态
          user_id,
          status,
          push_switch,
          start_time,
          end_time
        from
          ods_vova_vts.ods_vova_bonus_card tmp2
        where from_unixtime(tmp2.start_time, 'yyyy-MM-dd') <= date_sub('${cur_date}', 14)
          and from_unixtime(tmp2.end_time, 'yyyy-MM-dd') >= date_sub('${cur_date}', 14)
      ) tmp2
      on tmp1.user_id = tmp2.user_id
      where tmp1.update_time is not null or from_unixtime(tmp2.start_time, 'yyyy-MM-dd') < date_sub('${cur_date}', 14)
      union all
      -- 前28天的用户最新状态
      select
        date_sub('${cur_date}', 28) pt,
        COALESCE(tmp1.user_id, tmp2.user_id) user_id,
        if(tmp1.new_status is not null, tmp1.new_status, tmp2.status) status,
        tmp2.push_switch push_switch,
        if(tmp1.update_time is not null, from_unixtime(tmp1.update_time, 'yyyy-MM-dd'), from_unixtime(tmp2.start_time, 'yyyy-MM-dd')) start_time,
        from_unixtime(tmp2.end_time, 'yyyy-MM-dd') end_time
      from
      (
        select  -- 从log中获取用户当天的状态
          user_id,
          new_status,
          update_time
        from
        (
          select *,
            row_number() over(partition by user_id, pt order by update_time desc) row
          from
            dwd.dwd_vova_fact_log_bonus_card
          where pt <= date_sub('${cur_date}', 28) and new_status != 'unpaid'
        ) where row = 1
      ) tmp1
      full outer join
      (
        select  -- 从mysql中获取用户当天的状态
          user_id,
          status,
          push_switch,
          start_time,
          end_time
        from
          ods_vova_vts.ods_vova_bonus_card tmp2
        where from_unixtime(tmp2.start_time, 'yyyy-MM-dd') <= date_sub('${cur_date}', 28)
          and from_unixtime(tmp2.end_time, 'yyyy-MM-dd') >= date_sub('${cur_date}', 28)
      ) tmp2
      on tmp1.user_id = tmp2.user_id
      where tmp1.update_time is not null or from_unixtime(tmp2.start_time, 'yyyy-MM-dd') < date_sub('${cur_date}', 28)
    )
  ) vbc
  on flsv.buyer_id = vbc.user_id and flsv.pt = vbc.pt
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on flsv.buyer_id = abgs.buyer_id
  left join
  (
    select
      distinct device_id device_id, pt pt, page_code
    from
      dwd.dwd_vova_log_screen_view
    where pt in ('${cur_date}', date_sub('${cur_date}', -1), date_sub('${cur_date}', 6), date_sub('${cur_date}', 13), date_sub('${cur_date}', 27)
      , date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 21))
      -- and page_code = 'vouchercard_hp_paid'
      and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com' 
      and datasource = 'vova'
  ) flsv_today
  on flsv.device_id = flsv_today.device_id
    and (flsv.pt = date_sub(flsv_today.pt, 28) or flsv.pt = date_sub(flsv_today.pt, 14)
       or flsv.pt = date_sub(flsv_today.pt, 7) or flsv.pt = date_sub(flsv_today.pt, 1))
  where vbc.status = 'pending' or (vbc.status='paid' and vbc.start_time<=flsv.pt
      and vbc.end_time>=flsv.pt)
) tmp
group by cube(pt, region_code, os_type, main_channel, is_new, gmv_stage)
having pt !='all'
;


insert overwrite table dwb.dwb_vova_bonus_card_pay partition(pt='${cur_date}')
select
/*+ REPARTITION(1) */
  'vova',
  region_code,
  os_type,
  main_channel,
  is_new,
  gmv_stage,
  sum(bonus_card_paid_dau) bonus_card_paid_dau, -- 月卡用户DAU
  sum(bonus_card_pending_dau) bonus_card_pending_dau, --月卡pending用户DAU
  sum(bonus_card_gmv) bonus_card_gmv, -- 月卡用户GMV
  sum(bonus_card_order_cnt) bonus_card_order_cnt, -- 月卡用户支付成功订单量
  sum(bonus_card_first_order_cnt) bonus_card_first_order_cnt,   -- 月卡用户首单订单量
  sum(bonus_card_device_cnt) bonus_card_device_cnt, -- 月卡用户支付成功UV
  sum(bonus_card_price) bonus_card_price, -- 开卡费用总和
  sum(gmv) gmv, -- 大盘GMV 
  sum(cpn_cfg_val) cpn_cfg_val -- 月卡优惠券抵扣金额
from 
(
  select 
    region_code,
    os_type,
    main_channel,
    is_new,
    gmv_stage,
    0 bonus_card_paid_dau,
    0 bonus_card_pending_dau,
    0 bonus_card_gmv,
    0 bonus_card_order_cnt,
    0 bonus_card_first_order_cnt,
    0 bonus_card_device_cnt,
    bonus_card_price,
    0 gmv,
    0 cpn_cfg_val
  from 
    tmp.tmp_bonus_card_pay_${table_suffix}  
union all
  select 
    region_code,
    os_type,
    main_channel,
    is_new,
    gmv_stage,
    0 bonus_card_paid_dau,
    0 bonus_card_pending_dau,
    bonus_card_gmv,
    bonus_card_order_cnt,
    bonus_card_first_order_cnt,
    bonus_card_device_cnt,
    0 bonus_card_price,
    gmv,
    cpn_cfg_val
  from 
    tmp.tmp_bonus_card_gmv_${table_suffix} 
union all
  select 
    region_code,
    os_type,
    main_channel,
    is_new,
    gmv_stage,
    bonus_card_paid_dau,
    bonus_card_pending_dau,
    0 bonus_card_gmv,
    0 bonus_card_order_cnt,
    0 bonus_card_first_order_cnt,
    0 bonus_card_device_cnt,
    0 bonus_card_price,
    0 gmv,
    0 cpn_cfg_val
  from 
    tmp.tmp_bonus_card_screen_view_${table_suffix} 
  where pt = '${cur_date}'
) 
group by region_code, os_type, main_channel, is_new, gmv_stage
;

-- 表四：主页留存数据监控
-- 已购卡商品列表曝光数pv
create table if not exists tmp.tmp_paid_goods_impression_pv_${table_suffix} as
select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(paid_goods_impression_device_id) paid_goods_impression_pv -- 已购卡商品列表曝光数pv
from
(
  select
    nvl(flgi.pt, 'NA') pt,
    nvl(flgi.geo_country, 'NA') region_code,
    nvl(flgi.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff(flgi.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flgi.pt,dd.activate_time)>=1 and datediff(flgi.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flgi.pt,dd.activate_time)>=7 and datediff(flgi.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    flgi.device_id paid_goods_impression_device_id
  from
    dwd.dwd_vova_log_goods_impression flgi
  left join
    dim.dim_vova_devices dd
  on flgi.device_id = dd.device_id and flgi.datasource = dd.datasource
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on flgi.buyer_id = abgs.buyer_id
  where flgi.datasource = 'vova'
    and flgi.pt in ('${cur_date}', date_sub('${cur_date}', 1), date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 28))
    and flgi.platform = 'mob' and page_code = 'vouchercard_hp_paid'
    and flgi.list_type = '/vouchercard_hp_paid_bestselling'
    and flgi.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
) tmp
group by cube(pt, region_code, os_type, main_channel, is_new, gmv_stage)
having pt !='all'
;

-- 已购卡商品列表点击数pv
create table if not exists tmp.tmp_paid_goods_click_pv_${table_suffix} as
select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(paid_goods_click_device_id) paid_goods_click_pv -- 已购卡商品列表点击数pv
from
(
  select
    nvl(flgc.pt, 'NA') pt,
    nvl(flgc.geo_country, 'NA') region_code,
    nvl(flgc.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff(flgc.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flgc.pt,dd.activate_time)>=1 and datediff(flgc.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flgc.pt,dd.activate_time)>=7 and datediff(flgc.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    flgc.device_id paid_goods_click_device_id
  from
    dwd.dwd_vova_log_goods_click flgc
  left join
    dim.dim_vova_devices dd
  on flgc.device_id = dd.device_id and flgc.datasource = dd.datasource
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on flgc.buyer_id = abgs.buyer_id
  where flgc.datasource = 'vova'
    and flgc.pt in ('${cur_date}', date_sub('${cur_date}', 1), date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 28))
    and flgc.platform = 'mob' and page_code = 'vouchercard_hp_paid'
    and flgc.list_type = '/vouchercard_hp_paid_bestselling'
    and flgc.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
) tmp
group by cube(pt, region_code, os_type, main_channel, is_new, gmv_stage)
having pt !='all'
;

-- 续费按钮点击UV
create table if not exists tmp.tmp_renew_button_click_uv_${table_suffix} as
select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  count(distinct renew_button_click_device_id) renew_button_click_uv -- 续费按钮点击UV
from
(
  select
    nvl(flcc.pt, 'NA') pt,
    nvl(flcc.geo_country, 'NA') region_code,
    nvl(flcc.os_type, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff(flcc.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flcc.pt,dd.activate_time)>=1 and datediff(flcc.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flcc.pt,dd.activate_time)>=7 and datediff(flcc.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    flcc.device_id renew_button_click_device_id
  from
    dwd.dwd_vova_log_common_click flcc
  left join
    dim.dim_vova_devices dd
  on flcc.device_id = dd.device_id and flcc.datasource = dd.datasource
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on flcc.buyer_id = abgs.buyer_id
  where flcc.datasource = 'vova'
    and flcc.pt in ('${cur_date}', date_sub('${cur_date}', 1), date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 28))
    and flcc.platform = 'mob' and page_code = 'vouchercard_hp_paid'
    and flcc.element_name like '%renewButton%'
    and flcc.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
)
group by cube(pt, region_code, os_type, main_channel, is_new, gmv_stage)
having pt !='all'
;


-- 会场商品归因GMV
create table if not exists tmp.tmp_bonus_card_cause_gmv_${table_suffix} as
select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(os_type, 'all') os_type,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(gmv_stage, 'all') gmv_stage,
  sum(gmv) gmv -- 会场商品归因GMV
from
(
  select
    nvl(foc2.pt, 'NA') pt,
    nvl(dog.region_code, 'NA') region_code,
    nvl(foc2.platform, 'NA') os_type,
    nvl(dd.main_channel, 'NA') main_channel,
    CASE WHEN datediff(dog.order_time,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(dog.order_time,dd.activate_time)>=1 and datediff(dog.order_time,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(dog.order_time,dd.activate_time)>=7 and datediff(dog.order_time,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new, -- 激活时间
    nvl(abgs.gmv_stage, '0') gmv_stage, -- 用户等级
    dog.shipping_fee+dog.shop_price*dog.goods_number gmv
  from
    dwd.dwd_vova_fact_order_cause_v2 foc2
  left join
    dim.dim_vova_devices dd
  on foc2.device_id = dd.device_id and foc2.datasource = dd.datasource
  left join
    (select * from ads.ads_vova_buyer_portrait_feature where pt='${cur_date}') abgs
  on foc2.buyer_id = abgs.buyer_id
  left join
    dim.dim_vova_order_goods dog
  on foc2.order_goods_id = dog.order_goods_id and foc2.datasource = dog.datasource
  where foc2.datasource = 'vova'
    and foc2.pt in ('${cur_date}', date_sub('${cur_date}', 1), date_sub('${cur_date}', 7), date_sub('${cur_date}', 14), date_sub('${cur_date}', 28))
    and foc2.platform in ('android','ios') and foc2.pre_page_code = 'vouchercard_hp_paid'
    and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
)
group by cube(pt, region_code, os_type, main_channel, is_new, gmv_stage)
having pt !='all'
;

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert OVERWRITE TABLE dwb.dwb_vova_vouchercard_hp_paid_retention PARTITION (pt)
select
/*+ REPARTITION(1) */
  'vova' datasource,
  tmp1.region_code,
  tmp1.os_type,
  tmp1.main_channel,
  tmp1.is_new,
  tmp1.gmv_stage,
  nvl(tmp1.bonus_card_paid_dau, 0) bonus_card_paid_dau, -- 月卡DAU
  nvl(tmp1.vouchercard_hp_paid_uv, 0) vouchercard_hp_paid_uv, -- 已购卡会场页面UV
  nvl(renew_button_click_uv, 0) renew_button_click_uv, -- 续费按钮点击UV
  nvl(tmp1.push_switch_open_uv, 0) push_switch_open_uv, -- 推送开关打开UV
  nvl(paid_goods_impression_pv, 0) paid_goods_impression_pv, -- 已购卡商品列表曝光数pv
  nvl(paid_goods_click_pv, 0) paid_goods_click_pv, -- 已购卡商品列表点击数pv
  nvl(gmv, 0) gmv, -- 会场商品归因GMV
  nvl(tmp1.vouchercard_hp_paid_day2_uv, 0) vouchercard_hp_paid_day2_uv, -- 当日浏览uv中在次日浏览的uv
  nvl(tmp1.vouchercard_hp_paid_day7_uv, 0) vouchercard_hp_paid_day7_uv, -- 当日浏览uv中在第七日浏览的uv
  nvl(tmp1.vouchercard_hp_paid_day14_uv, 0) vouchercard_hp_paid_day14_uv, -- 当日浏览uv中在14日浏览的uv
  nvl(tmp1.vouchercard_hp_paid_day28_uv, 0) vouchercard_hp_paid_day28_uv, -- 当日浏览uv中在第28日浏览的uv

  nvl(tmp1.paid_app_uv, 0) paid_app_uv, -- 已购卡appUV
  nvl(tmp1.paid_app_day2_uv, 0) paid_app_day2_uv, -- 当日浏览uv中在次日浏览的uv
  nvl(tmp1.paid_app_day7_uv, 0) paid_app_day7_uv, -- 当日浏览uv中在第七日浏览的uv
  nvl(tmp1.paid_app_day14_uv, 0) paid_app_day14_uv, -- 当日浏览uv中在14日浏览的uv
  nvl(tmp1.paid_app_day28_uv, 0) paid_app_day28_uv, -- 当日浏览uv中在第28日浏览的uv
  tmp1.pt
from
  tmp.tmp_bonus_card_screen_view_${table_suffix} tmp1
left join
  tmp.tmp_paid_goods_impression_pv_${table_suffix} tmp2
on tmp1.pt= tmp2.pt and tmp1.region_code= tmp2.region_code and tmp1.os_type= tmp2.os_type
  and tmp1.main_channel= tmp2.main_channel and tmp1.is_new= tmp2.is_new and tmp1.gmv_stage= tmp2.gmv_stage
left join
  tmp.tmp_paid_goods_click_pv_${table_suffix} tmp3
on tmp1.pt= tmp3.pt and tmp1.region_code= tmp3.region_code and tmp1.os_type= tmp3.os_type
  and tmp1.main_channel= tmp3.main_channel and tmp1.is_new= tmp3.is_new and tmp1.gmv_stage= tmp3.gmv_stage
left join
  tmp.tmp_renew_button_click_uv_${table_suffix} tmp4
on tmp1.pt= tmp4.pt and tmp1.region_code= tmp4.region_code and tmp1.os_type= tmp4.os_type
  and tmp1.main_channel= tmp4.main_channel and tmp1.is_new= tmp4.is_new and tmp1.gmv_stage= tmp4.gmv_stage
left join
  tmp.tmp_bonus_card_cause_gmv_${table_suffix} tmp5
on tmp1.pt= tmp5.pt and tmp1.region_code= tmp5.region_code and tmp1.os_type= tmp5.os_type
  and tmp1.main_channel= tmp5.main_channel and tmp1.is_new= tmp5.is_new and tmp1.gmv_stage= tmp5.gmv_stage
;

drop table if exists tmp.tmp_bonus_card_coupon_use_${table_suffix};
drop table if exists tmp.tmp_bonus_card_pay_${table_suffix};
drop table if exists tmp.tmp_bonus_card_gmv_${table_suffix};
drop table if exists tmp.tmp_bonus_card_screen_view_${table_suffix};
drop TABLE if exists tmp.tmp_paid_goods_impression_pv_${table_suffix};
drop table if exists tmp.tmp_paid_goods_click_pv_${table_suffix};
drop TABLE if exists tmp.tmp_renew_button_click_uv_${table_suffix};
drop table if exists tmp.tmp_bonus_card_cause_gmv_${table_suffix};
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
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
