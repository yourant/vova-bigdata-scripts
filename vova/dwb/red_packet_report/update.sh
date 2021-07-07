#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_red_packet_req8713_chenkai_${cur_date}"

sql="
create table if not EXISTS tmp.tmp_first_cat_rank_gmv_avg_req8713_${table_suffix} as
select /*+ REPARTITION(10) */
  first_cat_id,
  rank,
  is_brand,
  mct_cnt,
  gmv,
  avg_first_cat_rank_gmv
from
(
  select
    nvl(first_cat_id, 'all') first_cat_id,
    nvl(rank, 'all') rank,
    nvl(is_brand, 'all') is_brand,
    count(distinct mct_id) mct_cnt, -- 红包店铺数
    sum(gmv) gmv, -- 对应一级品类等级下gmv
    round(nvl(sum(gmv) / count(distinct mct_id), 0), 4) avg_first_cat_rank_gmv -- 该类目该等级下的所有店铺gmv均值
  from
  (
    select
      fp.goods_id,
      fp.first_cat_id,
      fp.mct_id,
      fp.order_id,
      fp.order_goods_id,
      amr.rank rank,
      fp.shipping_fee+fp.shop_price*fp.goods_number gmv,
      if(dg.brand_id > 0, 'Y', 'N') is_brand
    from
      dwd.dwd_vova_fact_pay fp
    left join
    (
      select *
      from
        ads.ads_vova_mct_rank
      where pt ='${cur_date}'
    ) amr
    on fp.first_cat_id = amr.first_cat_id and fp.mct_id = amr.mct_id
    left join
      dim.dim_vova_goods dg
    on fp.goods_id = dg.goods_id
    where fp.datasource = 'vova' and to_date(fp.pay_time) ='${cur_date}'
  )
  group by cube(first_cat_id, rank, is_brand)
) where first_cat_id != 'all' and rank != 'all'
;
create table if not EXISTS tmp.tmp_mct_gmv_req8713_${table_suffix} as
select /*+ REPARTITION(10) */
  nvl(mct_id,'all') mct_id,
  nvl(first_cat_id,'all') first_cat_id,
  nvl(is_brand,'all') is_brand,
  sum(gmv) gmv -- 店铺gmv
from
(
  select
    fp.mct_id,
    dg.first_cat_id,
    if(dg.brand_id > 0, 'Y', 'N') is_brand,
    fp.shipping_fee+fp.shop_price*fp.goods_number gmv
  from
  (
    select distinct
      merchant_id merchant_id
    from
      ods_vova_vts.ods_vova_gsn_coupon_sign_goods
  ) t1
  left join
    dwd.dwd_vova_fact_pay fp
  on t1.merchant_id = fp.mct_id
  left join
    dim.dim_vova_goods dg
  on fp.goods_id = dg.goods_id
  where to_date(fp.pay_time) = '${cur_date}'
    and fp.datasource = 'vova'
)
group by cube(mct_id,
  first_cat_id,
  is_brand
)
;
create table if not EXISTS tmp.tmp_coupon_gmv_req8713_${table_suffix} as
select /*+ REPARTITION(10) */
  nvl(mct_id, 'all') mct_id,
  nvl(first_cat_id, 'all') first_cat_id,
  nvl(is_brand, 'all') is_brand,
  sum(gmv) gmv, -- 订单gmv
  sum(coupon_value) coupon_value, -- 红包折扣
  sum(gmv) - sum(coupon_value) remain_gmv  -- 使用红包的子订单金额（gmv-红包）
from
(
  select
    fp.goods_id goods_id,
    fp.first_cat_id first_cat_id,
    fp.mct_id mct_id,
    fp.order_id order_id,
    fp.order_goods_id order_goods_id,
    fp.bonus bonus,
    if(dg.brand_id > 0, 'Y', 'N') is_brand,
    amr.rank rank,
    fp.shipping_fee+fp.shop_price*fp.goods_number gmv,
    oge.ext_name ext_name,
    nvl(oge.extension_info, 0) coupon_value -- 订单实际优惠金额，
  from
    dwd.dwd_vova_fact_pay fp
  left join
    dim.dim_vova_goods dg
  on fp.goods_id = dg.goods_id
  left join
  (
    select *
    from
      ads.ads_vova_mct_rank
    where pt ='${cur_date}'
  ) amr
  on fp.first_cat_id = amr.first_cat_id and fp.mct_id = amr.mct_id
  left join
  (
    select * from
      ods_vova_vts.ods_vova_order_goods_extension
    where ext_name = 'merchant_coupon_discount' and extension_info > 0 -- 实际优惠金额要大于0
  ) oge
  on fp.order_goods_id = oge.rec_id
  where fp.datasource = 'vova'
    and to_date(fp.pay_time) ='${cur_date}'
    and oge.rec_id is not null
) t1
group by cube(
  mct_id,
  first_cat_id,
  is_brand
)
;

create table if not EXISTS tmp.tmp_red_packet_goods_req8713_${table_suffix} as
select /*+ REPARTITION(1) */
  t2.first_cat_id,
  t2.first_cat_name,
  t2.second_cat_id,
  t2.second_cat_name,
  t2.is_brand,
  t2.virtual_goods_id,
  t2.mct_id,
  t2.mct_name,
  amr.rank rank,
  t2.goods_id,
  t2.gsn_status,
  t2.coupon_num,
  t2.remain_num,
  t2.reach_time
from
(
  select
    dg.first_cat_id,
    dg.first_cat_name,
    dg.second_cat_id,
    dg.second_cat_name,
    if(dg.brand_id>0, 'Y','N') is_brand,
    dg.virtual_goods_id,
    dg.mct_id,
    dg.mct_name,
    t2.goods_id,
    t2.gsn_status,
    t2.coupon_num,
    t2.remain_num,
    t2.reach_time,
    if(t1.create_time is null, t2.create_time, t1.create_time) create_time
  from
  (
    select
      gcsg.goods_id goods_id,
      gca.gsn_status gsn_status,
      gcsg.coupon_num coupon_num,
      gcsg.remain_num remain_num,
      gca.reach_time reach_time,
      gcsg.create_time create_time
    from
      ods_vova_vts.ods_vova_gsn_coupon_activity gca
    inner join
      ods_vova_vts.ods_vova_gsn_coupon_sign_goods gcsg
    on gca.goods_sn = gcsg.goods_sn
    where gca.gsn_status != 4
      and gca.is_delete = 0
    union all
    select
      t2.goods_id goods_id,
      5 gsn_status,
      t2.coupon_num coupon_num,
      t2.remain_num remain_num,
      t1.send_time reach_time,
      t1.create_time create_time
    from
      ods_vova_vts.ods_vova_gsn_coupon_history_log t1
    inner join
      ods_vova_vts.ods_vova_gsn_coupon_sign_history_log t2
    on t2.gchl_id = t1.log_id
    where to_date(t2.create_time) >= '${cur_date}' or to_date(t2.last_update_time) >= '${cur_date}'
  ) t2
  left join
    dim.dim_vova_goods dg
  on t2.goods_id = dg.goods_id
  left join
  (
    select
      goods_id,
      min(create_time) create_time
    from
      ods_vova_vts.ods_vova_gsn_coupon_sign_history_log
    group by goods_id
  ) t1
  on t2.goods_id = t1.goods_id
) t2
left join
  ads.ads_vova_mct_rank amr
on to_date(t2.create_time) = amr.pt and t2.mct_id = amr.mct_id and t2.first_cat_id = amr.first_cat_id
;

insert overwrite table dwb.dwb_vova_red_packet_mct partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  t1.mct_id mct_id                ,  -- d_商家ID
  dm.mct_name mct_name            ,  -- d_商家名称
  t1.first_cat_id first_cat_id    ,  -- d_一级品类ID
  regexp_replace(dc.first_cat_name,'\'','') first_cat_name,  -- d_一级品类名称
  t1.is_brand                     ,  -- d_是否brand(Y,N)
  amr.rank rank                   ,  -- i_店铺一级类目等级
  t4.avg_first_cat_rank_gmv first_cat_rank_gmv_avg,  -- i_所属类目当前等级的gmv均值
  nvl(t2.gmv, 0) red_packet_order_gmv         ,  -- i_使用红包的子订单gmv
  nvl(t2.coupon_value, 0) red_packet_discount ,  -- i_红包折扣
  nvl(t2.remain_gmv, 0) red_packet_gmv        ,  -- i_红包带来的gmv(red_packet_order_gmv - red_packet_discount)
  nvl(t3.gmv, 0) mct_gmv                      ,  -- i_店铺gmv
  t1.coupon_num coupon_num            ,  -- i_红包总数
  t1.used_num used_num                ,  -- i_消耗红包数量
  t1.order_gsn_num order_gsn_num      ,  -- i_有出单的红包gsn数
  t1.activity_gsn_num activity_gsn_num,  -- i_已成团的gsn数
  t1.no_end_gsn_num no_end_gsn_num    ,  -- i_未售罄红包gsn数
  t1.end_gsn_num end_gsn_num          ,  -- i_售罄红包gsn数
  round(nvl(t1.order_gsn_num / t1.activity_gsn_num, 0), 4) turnover_rate, -- i_动销率
  round(nvl(t1.used_num / t1.coupon_num, 0), 4) sell_out_rate    -- i_售罄率:已消耗红包数/已成团gsn对应的红包数
from
(
  select
    nvl(mct_id, 'all') mct_id,
    nvl(first_cat_id, 'all') first_cat_id,
    nvl(is_brand, 'all') is_brand,
    sum(coupon_num) coupon_num, -- 已成团的红包总数
    sum(used_num) used_num, -- 已成团的红包中消耗的红包数量
    count(distinct order_gsn_num) order_gsn_num, -- 有出单的红包gsn数
    count(distinct activity_gsn_num) activity_gsn_num, -- 已成团的gsn数
    count(distinct no_end_gsn_num) no_end_gsn_num, -- 未售罄红包gsn数
    if(count(distinct activity_gsn_num) - count(distinct no_end_gsn_num) < 0, 0, count(distinct activity_gsn_num) - count(distinct   no_end_gsn_num)) end_gsn_num -- 已售罄红包gsn数
  from
  (
  select
    dg.goods_id,
    dg.mct_id,
    dg.first_cat_id,
    if(dg.brand_id > 0, 'Y', 'N') is_brand,
    coupon_num coupon_num, -- 已成团的红包总数
    used_num used_num, -- 已成团的红包中消耗的红包数量
    if(order_gsn_goods_id > 0, goods_sn, null) order_gsn_num, -- 有出单的红包gsn数
    if(activity_gsn_goods_id > 0, goods_sn, null) activity_gsn_num, -- 已成团的gsn数
    if(applied_coupon_num != used_num, goods_sn, null) no_end_gsn_num -- 未售罄红包gsn数
  from
  (
    select
      goods_id,
      sum(if(gsn_status = 3 or (gsn_status = 4 and coupon_num != remain_num), coupon_num, 0)) coupon_num, -- 已成团的红包总数
      sum(if(gsn_status = 3 or (gsn_status = 4 and coupon_num != remain_num), coupon_num - remain_num, 0)) used_num, -- 已成团的红包中消耗的红包数量
      sum(if(coupon_num - remain_num > 0, 1, 0)) order_gsn_goods_id, -- 有出单的商品, >0 则该商品有出单
      sum(if(gsn_status = 3 or (gsn_status = 4 and coupon_num != remain_num), 1, 0)) activity_gsn_goods_id, -- 已成团的商品 >0 则该商品有成团
      sum(coupon_num) applied_coupon_num -- 报名红包数量
    from
    (
      select
        gcsg.goods_id,
        gcsg.coupon_num,
        gcsg.remain_num,
        gca.gsn_status gsn_status
      from
        ods_vova_vts.ods_vova_gsn_coupon_activity gca
      inner join ods_vova_vts.ods_vova_gsn_coupon_sign_goods gcsg
        on gca.goods_sn = gcsg.goods_sn
      where gca.gsn_status = 3
        and gca.is_delete = 0
        and gcsg.remain_num > 0
      union all
      select
        goods_id,
        coupon_num,
        remain_num,
        4 gsn_status
      from
        ods_vova_vts.ods_vova_gsn_coupon_sign_history_log
    ) t
    group by goods_id
  ) t1
  left join
    dim.dim_vova_goods dg
  on t1.goods_id = dg.goods_id
  )
  group by cube(mct_id,
    first_cat_id,
    is_brand
  )
) t1
left join
(
  select *
  from
    ads.ads_vova_mct_rank
  where pt ='${cur_date}'
) amr
on t1.mct_id = amr.mct_id and t1.first_cat_id = amr.first_cat_id
left join
  tmp.tmp_coupon_gmv_req8713_${table_suffix} t2
on t1.mct_id = t2.mct_id
  and t1.first_cat_id = t2.first_cat_id
  and t1.is_brand = t2.is_brand
left join
  tmp.tmp_mct_gmv_req8713_${table_suffix} t3
on t1.mct_id = t3.mct_id and t1.first_cat_id = t3.first_cat_id and t1.is_brand = t3.is_brand
left join
  tmp.tmp_first_cat_rank_gmv_avg_req8713_${table_suffix} t4
on t1.first_cat_id = t4.first_cat_id and amr.rank = t4.rank and t1.is_brand = t4.is_brand
left join
(
  select distinct
    mct_id mct_id,
    mct_name mct_name
  from dim.dim_vova_merchant
) dm
on t1.mct_id = dm.mct_id
left join
(
  select distinct
    first_cat_id,
    first_cat_name
  from
    dim.dim_vova_category
) dc
on t1.first_cat_id = dc.first_cat_id
where t1.mct_id != 'all'
and t1.first_cat_id != 'all'
;

-- tables 2
insert overwrite table dwb.dwb_vova_red_packet_goods partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  t1.first_cat_id        ,
  regexp_replace(t1.first_cat_name,'\'','') first_cat_name ,
  t1.second_cat_id       ,
  t1.second_cat_name     ,
  t1.is_brand            ,
  t1.virtual_goods_id    ,
  t1.goods_id            ,
  t1.reach_time activity_start_time,
  t1.gsn_status          ,
  t1.mct_id              ,
  t1.mct_name            ,
  t1.rank                ,
  t1.coupon_num          ,
  if(t1.coupon_num - t1.remain_num > 0, t1.coupon_num - t1.remain_num, 0) used_num,
  t2.impression_pv       ,
  t2.impression_uv       ,
  t3.gmv red_packet_order_gmv,
  t3.red_packet_gmv red_packet_gmv,
  t3.order_goods_cnt red_packet_order_cnt,
  round(nvl(t3.red_packet_gmv/t3.order_goods_cnt, 0), 4) red_packet_avg_gmv,
  t3.pay_uv pay_uv,
  round(nvl(t3.pay_uv / t2.impression_uv, 0), 4) cr
from
  tmp.tmp_red_packet_goods_req8713_${table_suffix} t1
left join
(
  select
    t1.goods_id goods_id,
    count(device_id) impression_pv, -- 曝光pv
    count(distinct device_id) impression_uv -- 曝光uv
  from
    tmp.tmp_red_packet_goods_req8713_${table_suffix} t1
  left join
  (
    select
      virtual_goods_id, device_id
    from
      dwd.dwd_vova_log_goods_impression
    where pt = '${cur_date}'
      and dp = 'vova'
      and (page_code = 'RNflashsale'
        or (page_code ='theme_activity' and list_type = '/hongbao')
        )
    union all
    select
      element_id virtual_goods_id, device_id
    from
      dwd.dwd_vova_log_impressions_arc
    where pt ='${cur_date}'
      and datasource = 'vova'
      and event_type = 'goods'
      and get_json_object(extra, '$.activity') = 'merchant_coupon'
  ) dlgi
  on t1.virtual_goods_id = dlgi.virtual_goods_id
  group by t1.goods_id
) t2
on t1.goods_id = t2.goods_id
left join
(
  select
    fp.goods_id,
    sum(fp.shipping_fee+fp.shop_price*fp.goods_number) gmv, -- 子订单gmv
    sum(oge.extension_info) discount, -- 红包折扣
    count(distinct fp.order_goods_id) order_goods_cnt, -- 子订单数
    sum(fp.shipping_fee+fp.shop_price*fp.goods_number)-sum(oge.extension_info) red_packet_gmv, -- 使用红包的子订单金额(gmv-红包)
    count(distinct device_id) pay_uv -- 红包活动中的支付人数
  from
    dwd.dwd_vova_fact_pay fp
  left join
  (
    select * from
      ods_vova_vts.ods_vova_order_goods_extension
    where ext_name = 'merchant_coupon_discount' and extension_info > 0 -- 实际优惠金额要大于0
  ) oge
  on fp.order_goods_id = oge.rec_id
  where fp.datasource = 'vova'
    and to_date(fp.pay_time) = '${cur_date}'
    and oge.rec_id is not null
  group by fp.goods_id
) t3
on t1.goods_id = t3.goods_id
where t3.gmv > 0
;

-- table 3
insert overwrite table dwb.dwb_vova_red_packet_cat partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  t1.first_cat_id,
  regexp_replace(if(t1.first_cat_id = 'all', 'all', t2.first_cat_name),'\'','') first_cat_name,
  t1.second_cat_id,
  regexp_replace(if(t1.second_cat_id = 'all', 'all', t3.second_cat_name),'\'','') second_cat_name,
  t1.is_brand,
  gsn_cnt.gsn_cnt,   -- gsn总数
  gsn_cnt.applying_gsn_cnt,   -- 报名中gsn总数
  replenish_applying_gsn_cnt,   -- 补充报名中gsn总数
  activity_gsn_cnt,   -- 活动中gsn总数
  group_gsn_cnt,   -- 成团gsn数量
  order_gsn_cnt,   -- 已出单gsn数量
  sell_out_gsn_cnt,   -- 售罄红包gsn数量
  round(nvl(order_gsn_cnt / group_gsn_cnt, 0), 4) turnover_rate, -- 动销率
  round(nvl(used_num / coupon_num, 0), 4) sell_out_rate, -- 售罄率
  coupon_num, -- 已成团红包数
  used_num, -- 消耗红包数
  t1.gsn_source -- gsn来源
from
(
  select
    nvl(first_cat_id, 'all') first_cat_id,
    nvl(second_cat_id, 'all') second_cat_id,
    nvl(is_brand, 'all') is_brand,
    nvl(gsn_source, 'all') gsn_source,
    count(distinct goods_sn) gsn_cnt, -- 当前商家报名的商品 gsn
    count(distinct applying_gsn_cnt) applying_gsn_cnt,
    count(distinct replenish_applying_gsn_cnt) replenish_applying_gsn_cnt,
    count(distinct activity_gsn_cnt) activity_gsn_cnt,
    count(distinct group_gsn_cnt) group_gsn_cnt,
    count(distinct order_gsn_cnt) order_gsn_cnt,
    sum(coupon_num) coupon_num, -- 已成团的红包总数
    sum(used_num) used_num -- 已成团的红包中消耗的红包数量
  from
  (
    select
      nvl(dg.first_cat_id, 'unknown') first_cat_id,
      nvl(dg.second_cat_id, 'unknown') second_cat_id,
      if(dg.brand_id > 0, 'Y', 'N') is_brand,
      nvl(t1.gsn_source, 0) gsn_source,
      dg.goods_sn goods_sn, -- 当前商家报名的商品 gsn
      dg.goods_id goods_id,
      if(t1.gsn_status = 1, goods_sn, null) applying_gsn_cnt, -- 报名中gsn
      if(t1.gsn_status = 2, goods_sn, null) replenish_applying_gsn_cnt, -- 补充报名中gsn
      if(t1.gsn_status = 3, goods_sn, null) activity_gsn_cnt, -- 活动中gsn总数
      if(t1.gsn_status = 3 or (t1.gsn_status =4 and t1.coupon_num > t1.remain_num), goods_sn, null) group_gsn_cnt, --     成团gsn
      if(t1.coupon_num > t1.remain_num, goods_sn, null) order_gsn_cnt, -- 已出单gsn
      if(gsn_status = 3 or (gsn_status = 4 and coupon_num != remain_num), coupon_num, 0) coupon_num, -- 已成团的红包总数
      if(gsn_status = 3 or (gsn_status = 4 and coupon_num != remain_num), coupon_num - remain_num, 0) used_num -- 已成团的红包中消耗的红包数量
    from
    (
      select
        gcsg.goods_id goods_id,
        gcsg.coupon_num coupon_num, -- 报名红包数量
        gcsg.remain_num remain_num, -- 剩余红包数量
        gca.gsn_status gsn_status, -- 活动状态
        nvl(gca.gsn_source, 0) gsn_source -- gsn来源
      from
        ods_vova_vts.ods_vova_gsn_coupon_activity gca
      inner join
        ods_vova_vts.ods_vova_gsn_coupon_sign_goods gcsg
      on gca.goods_sn = gcsg.goods_sn
      where gca.is_delete = 0 and gca.gsn_status != 4
      union all
      select
        gcshl.goods_id goods_id,
        gcshl.coupon_num coupon_num,
        gcshl.remain_num remain_num,
        4 gsn_status,
        nvl(gchl.gsn_source, 0) gsn_source
      from
        ods_vova_vts.ods_vova_gsn_coupon_sign_history_log gcshl
      left join
        ods_vova_vts.ods_vova_gsn_coupon_history_log gchl
      on gcshl.gchl_id = gchl.log_id
    ) t1
    left join
      dim.dim_vova_goods dg
    on t1.goods_id = dg.goods_id
  )
  group by cube(first_cat_id, second_cat_id, is_brand, gsn_source)
) t1
left join
(
  select
    nvl(first_cat_id, 'all') first_cat_id,
    nvl(second_cat_id, 'all') second_cat_id,
    nvl(is_brand, 'all') is_brand,
    nvl(gsn_source, 'all') gsn_source,
    count(distinct goods_sn) gsn_cnt, -- gsn 总数
    count(distinct applying_gsn) applying_gsn_cnt -- 报名中gsn数
  from
  (
    select
      nvl(dg.first_cat_id, 'unknown') first_cat_id,
      nvl(dg.second_cat_id, 'unknown') second_cat_id,
      if(dg.brand_id > 0, 'Y', 'N') is_brand,
      nvl(gca.gsn_source, 0) gsn_source,
      dg.goods_sn, -- 总 gsn
      if(gca.gsn_status=1, dg.goods_sn, null) applying_gsn -- 报名中gsn
    from
      ods_vova_vts.ods_vova_gsn_coupon_activity gca
    left join
      dim.dim_vova_goods dg
    on gca.goods_id = dg.goods_id
    where gca.is_delete = 0 and gca.gsn_status > 0
  )
  group by cube(first_cat_id, second_cat_id, is_brand, gsn_source)
) gsn_cnt
on t1.first_cat_id = gsn_cnt.first_cat_id and t1.second_cat_id = gsn_cnt.second_cat_id
  and t1.is_brand = gsn_cnt.is_brand and t1.gsn_source = gsn_cnt.gsn_source
left join
(
  select
    first_cat_id,
    second_cat_id,
    is_brand,
    gsn_source,
    count(distinct(if(remain_num = 0, goods_sn, null))) sell_out_gsn_cnt
  from
  (
    select
      nvl(first_cat_id, 'all') first_cat_id,
      nvl(second_cat_id, 'all') second_cat_id,
      nvl(is_brand, 'all') is_brand,
      nvl(goods_sn,'all') goods_sn,
      sum(remain_num) remain_num,
      nvl(gsn_source, 'all') gsn_source
    from
    (
      select
        nvl(first_cat_id, 'unknown') first_cat_id,
        nvl(second_cat_id, 'unknown') second_cat_id,
        if(dg.brand_id > 0, 'Y', 'N') is_brand,
        nvl(goods_sn, 'unknown') goods_sn,
        remain_num,
        nvl(gsn_source, 0) gsn_source
      from
      (
        select
          gcsg.goods_id goods_id,
          gcsg.coupon_num coupon_num, -- 报名红包数量
          gcsg.remain_num remain_num, -- 剩余红包数量
          gca.gsn_status gsn_status, -- 活动状态
          nvl(gca.gsn_source, 0) gsn_source
        from
          ods_vova_vts.ods_vova_gsn_coupon_activity gca
        inner join
          ods_vova_vts.ods_vova_gsn_coupon_sign_goods gcsg
        on gca.goods_sn = gcsg.goods_sn
        where gca.is_delete = 0 and gca.gsn_status != 4

        union all
        select
          gcshl.goods_id goods_id,
          gcshl.coupon_num coupon_num,
          gcshl.remain_num remain_num,
          4 gsn_status,
          nvl(gchl.gsn_source, 0) gsn_source
        from
          ods_vova_vts.ods_vova_gsn_coupon_sign_history_log gcshl
        left join
          ods_vova_vts.ods_vova_gsn_coupon_history_log gchl
        on gcshl.gchl_id = gchl.log_id
      ) t1
      left join
        dim.dim_vova_goods dg
      on t1.goods_id = dg.goods_id
    )
    group by cube(first_cat_id, second_cat_id,  is_brand, goods_sn, gsn_source)
  ) where first_cat_id != 'all' and goods_sn != 'all' -- and second_cat_id != 'all'
  group by first_cat_id, second_cat_id, is_brand, gsn_source
) t4
on t1.first_cat_id = t4.first_cat_id and t1.second_cat_id = t4.second_cat_id
  and t1.is_brand = t4.is_brand and t1.gsn_source = t4.gsn_source
left join
(
  select distinct
    first_cat_id, first_cat_name
  from
    dim.dim_vova_category
) t2
on t1.first_cat_id = t2.first_cat_id
left join
(
  select distinct
    second_cat_id, second_cat_name
  from
    dim.dim_vova_category
) t3
on t1.second_cat_id = t3.second_cat_id
where t1.first_cat_id != 'all' -- and t1.second_cat_id != 'all'
;

drop table if EXISTS tmp.tmp_first_cat_rank_gmv_avg_req8713_${table_suffix};
drop table if EXISTS tmp.tmp_mct_gmv_req8713_${table_suffix};
drop table if EXISTS tmp.tmp_coupon_gmv_req8713_${table_suffix};
drop table if EXISTS tmp.tmp_red_packet_goods_req8713_${table_suffix};
"

spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.default.parallelism=80"  \
--conf "spark.app.name=${job_name}"  \
--conf "spark.sql.autoBroadcastJoinThreshold=52428800" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.dynamicAllocation.initialExecutors=80"  \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
