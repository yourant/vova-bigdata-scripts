#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天:1
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
ALTER TABLE ads.ads_vova_goods_portrait DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
with tmp_goods_portrait as
(SELECT
  dg.goods_id,
  dg.cat_id,
  dg.first_cat_id,
  dg.second_cat_id,
  dg.brand_id,
  dg.shop_price,
  IF('${pre_date}' >= to_date ( vg.promote_start ) AND '${pre_date}' <= to_date ( vg.promote_end ),( dg.shop_price - vg.promote_price ) / dg.shop_price,0 ) AS gs_discount,
  dg.shipping_fee,
  dg.mct_id,
  nvl(tmp_comment.comment_cnt_6m, 0) as comment_cnt_6m,
  nvl(tmp_comment.comment_good_cnt_6m, 0) as comment_good_cnt_6m,
  nvl(tmp_comment.comment_bad_cnt_6m, 0) as comment_bad_cnt_6m,
  nvl(tmp_beh.gmv_1w, 0) as gmv_1w,
  nvl(tmp_beh.gmv_15d, 0) as gmv_15d,
  nvl(tmp_beh.gmv_1m, 0) as gmv_1m,
  nvl(tmp_beh.sales_vol_1w, 0) as sales_vol_1w,
  nvl(tmp_beh.sales_vol_15d, 0) as sales_vol_15d,
  nvl(tmp_beh.sales_vol_1m, 0) as sales_vol_1m,
  nvl(tmp_beh.expre_cnt_1w, 0) as expre_cnt_1w,
  nvl(tmp_beh.expre_cnt_15d, 0) as expre_cnt_15d,
  nvl(tmp_beh.expre_cnt_1m, 0) as expre_cnt_1m,
  nvl(tmp_beh.clk_cnt_1w, 0) as clk_cnt_1w,
  nvl(tmp_beh.clk_cnt_15d, 0) as clk_cnt_15d,
  nvl(tmp_beh.clk_cnt_1m, 0) as clk_cnt_1m,
  nvl(tmp_beh.collect_cnt_1w, 0) as collect_cnt_1w,
  nvl(tmp_beh.collect_cnt_15d, 0) as collect_cnt_15d,
  nvl(tmp_beh.collect_cnt_1m, 0) as collect_cnt_1m,
  nvl(tmp_beh.add_cat_cnt_1w, 0) as add_cat_cnt_1w,
  nvl(tmp_beh.add_cat_cnt_15d, 0) as add_cat_cnt_15d,
  nvl(tmp_beh.add_cat_cnt_1m, 0) as add_cat_cnt_1m,
  nvl ( tmp_beh.clk_cnt_1w / tmp_beh.expre_cnt_1w * 100, 0 ) AS clk_rate_1w,
  nvl ( tmp_beh.clk_cnt_15d / tmp_beh.expre_cnt_15d * 100, 0 ) AS clk_rate_15d,
  nvl ( tmp_beh.clk_cnt_1m / tmp_beh.expre_cnt_1m * 100, 0 ) AS clk_rate_1m,
  nvl ( tmp_ord.ord_uv_1w / tmp_expre.expre_uv_1w * 100, 0 ) AS pay_rate_1w,
  nvl ( tmp_ord.ord_uv_15d / tmp_expre.expre_uv_15d * 100, 0 ) AS pay_rate_15d,
  nvl ( tmp_ord.ord_uv_1m / tmp_expre.expre_uv_1m * 100, 0 ) AS pay_rate_1m,
  nvl ( tmp_add_cat.add_cat_uv_1w / tmp_expre.expre_uv_1w * 100, 0 ) AS add_cat_rate_1w,
  nvl ( tmp_add_cat.add_cat_uv_15d / tmp_expre.expre_uv_15d * 100, 0 ) AS add_cat_rate_15d,
  nvl ( tmp_add_cat.add_cat_uv_1m / tmp_expre.expre_uv_1m * 100, 0 ) AS add_cat_rate_1m,
  nvl ( tmp_clk.clk_uv_1w / tmp_expre.expre_uv_1w * 100, 0 ) AS rate_1w,
  nvl ( tmp_clk.clk_uv_15d / tmp_expre.expre_uv_15d * 100, 0 ) AS rate_15d,
  nvl ( tmp_clk.clk_uv_1m / tmp_expre.expre_uv_1m * 100, 0 ) AS rate_1m,
  nvl(tmp_beh.gmv_1w  / tmp_clk.clk_uv_1w  * 100, 0 ) gr_1w,
  nvl(tmp_beh.gmv_15d / tmp_clk.clk_uv_15d * 100, 0 ) gr_15d,
  nvl(tmp_beh.gmv_1m  / tmp_clk.clk_uv_1m  * 100, 0 ) gr_1m,
  nvl(tmp_beh.gmv_1w  / tmp_clk.clk_uv_1w  * tmp_beh.clk_cnt_1w  / tmp_beh.expre_cnt_1w  * 10000, 0) gcr_1w,
  nvl(tmp_beh.gmv_15d / tmp_clk.clk_uv_15d * tmp_beh.clk_cnt_15d / tmp_beh.expre_cnt_15d * 10000, 0) gcr_15d,
  nvl(tmp_beh.gmv_1m  / tmp_clk.clk_uv_1m  * tmp_beh.clk_cnt_1m  / tmp_beh.expre_cnt_1m  * 10000, 0) gcr_1m,
  nvl(tmp_clk.clk_uv_1w,  0) as clk_uv_1w,
  nvl(tmp_clk.clk_uv_15d, 0) as clk_uv_15d,
  nvl(tmp_clk.clk_uv_1m,  0) as clk_uv_1m,
  gkw.key_words,
  nvl(tmp_ord.ord_cnt_1w,0) as ord_cnt_1w,
  nvl(tmp_ord.ord_cnt_15d) as ord_cnt_15d,
  nvl(tmp_ord.ord_cnt_1m) as ord_cnt_1m
FROM
dim.dim_vova_goods dg
left  JOIN
  (SELECT
      gs_id,
      sum( IF ( day_gap < 7, expre_cnt, 0 ) ) AS expre_cnt_1w,
      sum( IF ( day_gap < 15, expre_cnt, 0 ) ) AS expre_cnt_15d,
      sum( IF ( day_gap < 30, expre_cnt, 0 ) ) AS expre_cnt_1m,
      sum( IF ( day_gap < 7, clk_cnt, 0 ) ) AS clk_cnt_1w,
      sum( IF ( day_gap < 15, clk_cnt, 0 ) ) AS clk_cnt_15d,
      sum( IF ( day_gap < 30, clk_cnt, 0 ) ) AS clk_cnt_1m,
      sum( IF ( day_gap < 7, collect_cnt, 0 ) ) AS collect_cnt_1w,
      sum( IF ( day_gap < 15, collect_cnt, 0 ) ) AS collect_cnt_15d,
      sum( IF ( day_gap < 30, collect_cnt, 0 ) ) AS collect_cnt_1m,
      sum( IF ( day_gap < 7, add_cat_cnt, 0 ) ) AS add_cat_cnt_1w,
      sum( IF ( day_gap < 15, add_cat_cnt, 0 ) ) AS add_cat_cnt_15d,
      sum( IF ( day_gap < 30, add_cat_cnt, 0 ) ) AS add_cat_cnt_1m,
      sum( IF ( day_gap < 7, sales_vol, 0 ) ) AS sales_vol_1w,
      sum( IF ( day_gap < 15, sales_vol, 0 ) ) AS sales_vol_15d,
      sum( IF ( day_gap < 30, sales_vol, 0 ) ) AS sales_vol_1m,
      sum( IF ( day_gap < 7, gmv, 0 ) ) AS gmv_1w,
      sum( IF ( day_gap < 15, gmv, 0 ) ) AS gmv_15d,
      sum( IF ( day_gap < 30, gmv, 0 ) ) AS gmv_1m
    FROM
      (
    SELECT
      gs_id,
      pt,
      datediff( '${pre_date}', pt ) AS day_gap,
      sum( expre_cnt ) AS expre_cnt,
      sum( clk_cnt ) AS clk_cnt,
      sum( clk_valid_cnt ) AS clk_valid_cnt,
      sum( collect_cnt ) AS collect_cnt,
      sum( add_cat_cnt ) AS add_cat_cnt,
      sum( sales_vol ) AS sales_vol,
      sum( gmv ) AS gmv
    FROM
      dws.dws_vova_buyer_goods_behave
    WHERE
      pt > date_sub( '${pre_date}', 30 )
      AND pt <= '${pre_date}'
    GROUP BY
      gs_id,
      pt
     )
   GROUP BY
     gs_id
  ) tmp_beh
   ON tmp_beh.gs_id = dg.goods_id
  LEFT JOIN ods_vova_vts.ods_vova_goods vg ON dg.goods_id = vg.goods_id
  LEFT JOIN -- 曝光uv
  (
    SELECT
      t1.goods_id,
      count( DISTINCT IF ( t1.day_gap < 7, device_id, NULL ) ) AS expre_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, device_id, NULL ) ) AS expre_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, device_id, NULL ) ) AS expre_uv_1m
    FROM
      (
    SELECT
      dg.goods_id,
      gi.device_id,
      datediff( '${pre_date}', pt ) AS day_gap
    FROM
      dwd.dwd_vova_log_goods_impression gi
      INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gi.virtual_goods_id
    WHERE
      gi.pt > date_sub( '${pre_date}', 30 )
      AND gi.pt <= '${pre_date}'
      AND platform = 'mob'
      ) t1
    GROUP BY
      t1.goods_id
  ) tmp_expre
  ON dg.goods_id = tmp_expre.goods_id
  LEFT JOIN -- 点击uv
  (
    SELECT
      t1.goods_id,
      count( DISTINCT IF ( t1.day_gap < 7, device_id, NULL ) ) AS clk_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, device_id, NULL ) ) AS clk_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, device_id, NULL ) ) AS clk_uv_1m
    FROM
      (
    SELECT
      dg.goods_id,
      gc.device_id,
      datediff( '${pre_date}', pt ) AS day_gap
    FROM
      dwd.dwd_vova_log_goods_click gc
      INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gc.virtual_goods_id
    WHERE
      gc.pt > date_sub( '${pre_date}', 30 )
      AND gc.pt <= '${pre_date}'
      AND platform = 'mob'
      ) t1
    GROUP BY
      t1.goods_id
  ) tmp_clk
  ON dg.goods_id = tmp_clk.goods_id
  LEFT JOIN -- 购买uv
  (
    SELECT
      t1.goods_id,
      count( DISTINCT IF ( t1.day_gap < 7, buyer_id, NULL ) ) AS ord_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, buyer_id, NULL ) ) AS ord_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, buyer_id, NULL ) ) AS ord_uv_1m,
      count( IF ( t1.day_gap < 7, buyer_id, NULL ) ) AS ord_cnt_1w,
      count( IF ( t1.day_gap < 15, buyer_id, NULL ) ) AS ord_cnt_15d,
      count( IF ( t1.day_gap < 30, buyer_id, NULL ) ) AS ord_cnt_1m
    FROM
      (
    SELECT
      fp.goods_id,
      fp.buyer_id,
      datediff( '${pre_date}', to_date ( pay_time ) ) AS day_gap
    FROM
      dwd.dwd_vova_fact_pay fp
    WHERE
      to_date ( order_time ) > date_sub( '${pre_date}', 30 )
      AND to_date ( order_time ) <= '${pre_date}'
      AND fp.platform IN ( 'ios', 'android' )
      ) t1
    GROUP BY
      t1.goods_id
  ) tmp_ord
  ON dg.goods_id = tmp_ord.goods_id
  LEFT JOIN (
    SELECT
      t1.goods_id,
      count( DISTINCT IF ( t1.day_gap < 7, device_id, NULL ) ) AS add_cat_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, device_id, NULL ) ) AS add_cat_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, device_id, NULL ) ) AS add_cat_uv_1m
    FROM
      (
    SELECT
      dg.goods_id,
      cc.device_id,
      datediff( '${pre_date}', pt ) AS day_gap
    FROM
      dwd.dwd_vova_log_common_click cc
      INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = cc.element_id
    WHERE
      cc.pt > date_sub( '${pre_date}', 30 )
      AND cc.pt <= '${pre_date}'
      AND cc.element_name = 'pdAddToCartSuccess'
      AND platform = 'mob'
      ) t1
    GROUP BY
      t1.goods_id
  ) tmp_add_cat
  ON dg.goods_id = tmp_add_cat.goods_id
  LEFT JOIN (
    SELECT
      goods_id,
      count( * ) AS comment_cnt_6m,
      sum( IF ( rating <= 2, 1, 0 ) ) AS comment_bad_cnt_6m,
      sum( IF ( rating = 5, 1, 0 ) ) AS comment_good_cnt_6m
    FROM
      dwd.dwd_vova_fact_comment
    WHERE
      to_date ( post_time ) > date_sub( '${pre_date}', 180 )
      AND to_date ( post_time ) <= '${pre_date}'
    GROUP BY
      goods_id
  ) tmp_comment
  ON dg.goods_id = tmp_comment.goods_id
  LEFT JOIN (select goods_id,first(key_words) key_words from tmp.tmp_vova_goods_key_words group by goods_id) gkw
  ON tmp_beh.gs_id = gkw.goods_id),

tmp_goods_painting_pt as
(select
a.goods_id,
if((nvl(pt_man,0)*0.5 + nvl(clk_man_rate,0.5)*0.5) / (nvl(pt_women,0)*0.5 + nvl(clk_women_rate,0.5)*0.5) >= 2,
1,if((nvl(pt_women,0)*0.5 + nvl(clk_women_rate,0.5)*0.5) / (nvl(pt_man,0)*0.5 + nvl(clk_man_rate,0.5)*0.5) >= 2,0,2)) gs_gender
from (
select
tmp.goods_id,tmp.virtual_goods_id,
if(tmp.pt_man > 0 and tmp.pt_women = 0,1,if(tmp.pt_man > 0 and tmp.pt_women > 0,0.5,0)) pt_man,
if(tmp.pt_women > 0 and tmp.pt_man = 0,1,if(tmp.pt_man > 0 and tmp.pt_women > 0,0.5,0)) pt_women
from (
select
dg.goods_id,
dg.virtual_goods_id,
sum(if(lower(dg.goods_name) like concat('%',lower(agd.word),'%') and agd.gender = 1,1,0))  pt_man,
sum(if(lower(dg.goods_name) like concat('%',lower(agd.word),'%') and agd.gender = 0,0,1))  pt_women
from dim.dim_vova_goods dg
full join ads.ads_vova_gender_dic agd on 1 = 1
group by dg.goods_id,dg.virtual_goods_id
) tmp
) a
left join (
select
virtual_goods_id,
count(*) clk_cnt,
sum(if(gender = 'male',1,0)) / count(*) clk_man_rate,
sum(if(gender = 'female',1,0)) / count(*) clk_women_rate
from dwd.dwd_vova_log_goods_click flgc
where flgc.pt >= date_sub( '${pre_date}', 30 )
and flgc.gender in ('female','male')
group by virtual_goods_id
having clk_cnt >= 100
) b
on a.virtual_goods_id = b.virtual_goods_id),

tmp_goods_painting_most_popular_clk as
(select
tmp.goods_id,
tmp.mp_clk_pv_1w,
tmp.mp_clk_pv_15d,
tmp.mp_clk_pv_1m,
row_number() over(partition by tmp.goods_id order by tmp.mp_clk_pv_1w DESC) mp_clk_pv_1w_rk,
row_number() over(partition by tmp.goods_id order by tmp.mp_clk_pv_15d DESC) mp_clk_pv_15d_rk,
row_number() over(partition by tmp.goods_id order by tmp.mp_clk_pv_1m DESC) mp_clk_pv_1m_rk
from (
SELECT
  t1.goods_id,
  count( IF ( t1.day_gap < 7, device_id, NULL ) ) AS mp_clk_pv_1w,
  count( IF ( t1.day_gap < 15, device_id, NULL ) ) AS mp_clk_pv_15d,
  count( IF ( t1.day_gap < 30, device_id, NULL ) ) AS mp_clk_pv_1m
FROM
  (
SELECT
  dg.goods_id,
  gc.device_id,
  datediff( '${pre_date}', pt ) AS day_gap
FROM
  dwd.dwd_vova_log_goods_click gc
  INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gc.virtual_goods_id
WHERE
  gc.pt > date_sub( '${pre_date}', 30 )
  AND gc.pt <= '${pre_date}'
  AND platform = 'mob'
  and gc.page_code in ('homepage','product_list') and  gc.list_type in ('/product_list_popular','/product_list')
  ) t1
GROUP BY
  t1.goods_id
  ) tmp
),


tmp_goods_painting_most_popular_cart as
(select
tmp.goods_id,
tmp.mp_cart_pv_1w,
tmp.mp_cart_pv_15d,
tmp.mp_cart_pv_1m,
row_number() over(partition by tmp.goods_id order by tmp.mp_cart_pv_1w DESC) mp_cart_pv_1w_rk,
row_number() over(partition by tmp.goods_id order by tmp.mp_cart_pv_15d DESC) mp_cart_pv_15d_rk,
row_number() over(partition by tmp.goods_id order by tmp.mp_cart_pv_1m DESC) mp_cart_pv_1m_rk
from (
SELECT
  t1.goods_id,
  count( IF ( t1.day_gap < 7, device_id, NULL ) ) AS mp_cart_pv_1w,
  count( IF ( t1.day_gap < 15, device_id, NULL ) ) AS mp_cart_pv_15d,
  count( IF ( t1.day_gap < 30, device_id, NULL ) ) AS mp_cart_pv_1m
FROM
  (
select
  dg.goods_id,
  fcc.device_id,
  datediff( '${pre_date}', pt ) AS day_gap
      from
     dwd.dwd_vova_fact_cart_cause_v2 fcc
     INNER JOIN dim.dim_vova_goods dg ON fcc.virtual_goods_id = dg.virtual_goods_id
     where fcc.pt > date_sub( '${pre_date}', 30 )
     and fcc.pre_page_code in ('homepage','product_list') and  fcc.pre_list_type in ('/product_list_popular','/product_list')
) t1
GROUP BY
  t1.goods_id
  ) tmp
),

-- 七天上网率
tmp_goods_painting_inter_rate_3_6w as
(select
  t1.goods_id,
  sum(t1.so_order_cnt_3_6w)/count(t1.order_goods_id) as inter_rate_3_6w,
  (sum(t1.so_order_cnt_3_6w)+0.9*5)/(count(t1.order_goods_id)+5) as bs_inter_rate_3_6w
from
(
select
  og.goods_id,
  og.order_goods_id,
  case when datediff(fl.valid_tracking_date,fl.confirm_time)< 7 and og.sku_pay_status>1 then 1 else 0
    end so_order_cnt_3_6w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id=og.order_goods_id
where datediff('${pre_date}', date(og.confirm_time)) between 6 and 36
) t1
group by t1.goods_id
),

-- 5到8周非物流退款率
tmp_goods_painting_nlrf_rate_5_8w as
(select
t1.goods_id,
sum(t1.nlrf_order_cnt_5_8w)/count(t1.order_goods_id) as nlrf_rate_5_8w,
(sum(t1.nlrf_order_cnt_5_8w)+0.1*5)/(count(t1.order_goods_id)+5) as bs_nlrf_rate_5_8w
from
(
  select
    og.goods_id,
    og.order_goods_id,
    case when datediff(fr.rr_audit_time,og.confirm_time)< 63
      and fr.rr_audit_status = 'audit_passed'
      and og.sku_pay_status>1 and fr.refund_reason_type_id not in (8,9) then 1 else 0
    end nlrf_order_cnt_5_8w
  from dim.dim_vova_order_goods og
  left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
  where datediff('${pre_date}', date(og.confirm_time)) between 62 and 92
) t1
group by t1.goods_id
),

-- 9到12周物流退款率
tmp_goods_painting_lrf_rate_9_12w as
(select
t1.goods_id,
sum(t1.lrf_order_cnt_9_12w)/count(t1.order_goods_id) as lrf_rate_9_12w,
(sum(t1.lrf_order_cnt_9_12w)+0.1*5)/(count(t1.order_goods_id)+5) as bs_lrf_rate_9_12w
from
(
  select
    og.goods_id,
    og.order_goods_id,
    case when datediff(fr.rr_audit_time,og.confirm_time)< 84
      and fr.rr_audit_status = 'audit_passed'
      and og.sku_pay_status>1 and fr.refund_reason_type_id in (8,9) then 1 else 0
    end lrf_order_cnt_9_12w
  from  dim.dim_vova_order_goods og
  left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
  where datediff('${pre_date}', date(og.confirm_time)) between 83 and 113
) t1
group by t1.goods_id
)

INSERT overwrite TABLE ads.ads_vova_goods_portrait partition(pt='$pre_date')
select
/*+ REPARTITION(50) */
/*+ BROADCAST(t1) */
/*+ BROADCAST(t2) */
/*+ BROADCAST(t3) */
  a.goods_id,
  cat_id,
  first_cat_id,
  second_cat_id,
  brand_id,
  shop_price,
  gs_discount,
  shipping_fee,
  mct_id,
  comment_cnt_6m,
  comment_good_cnt_6m,
  comment_bad_cnt_6m,
  gmv_1w,
  gmv_15d,
  gmv_1m,
  sales_vol_1w,
  sales_vol_15d,
  sales_vol_1m,
  expre_cnt_1w,
  expre_cnt_15d,
  expre_cnt_1m,
  clk_cnt_1w,
  clk_cnt_15d,
  clk_cnt_1m,
  collect_cnt_1w,
  collect_cnt_15d,
  collect_cnt_1m,
  add_cat_cnt_1w,
  add_cat_cnt_15d,
  add_cat_cnt_1m,
  clk_rate_1w,
  clk_rate_15d,
  clk_rate_1m,
  pay_rate_1w,
  pay_rate_15d,
  pay_rate_1m,
  add_cat_rate_1w,
  add_cat_rate_15d,
  add_cat_rate_1m,
  rate_1w,
  rate_15d,
  rate_1m,
  key_words,
  nvl(b.gs_gender, 2)         as gs_gender,
  nvl(c.mp_clk_pv_1w, 0)      as mp_clk_pv_1w,
  nvl(c.mp_clk_pv_15d, 0)     as mp_clk_pv_15d,
  nvl(c.mp_clk_pv_1m, 0)      as mp_clk_pv_1m,
  nvl(d.mp_cart_pv_1w, 0)     as mp_cart_pv_1w,
  nvl(d.mp_cart_pv_15d, 0)    as mp_cart_pv_15d,
  nvl(d.mp_cart_pv_1m, 0)     as mp_cart_pv_1m,
  nvl(c.mp_clk_pv_1w_rk, 0)   as mp_clk_pv_1w_rk,
  nvl(c.mp_clk_pv_15d_rk, 0)  as mp_clk_pv_15d_rk,
  nvl(c.mp_clk_pv_1m_rk, 0)   as mp_clk_pv_1m_rk,
  nvl(d.mp_cart_pv_1w_rk, 0)  as mp_cart_pv_1w_rk,
  nvl(d.mp_cart_pv_15d_rk, 0) as mp_cart_pv_15d_rk,
  nvl(d.mp_cart_pv_1m_rk, 0)  as mp_cart_pv_1m_rk,
  gr_1w,
  gr_15d,
  gr_1m,
  gcr_1w,
  gcr_15d,
  gcr_1m,
  clk_uv_1w,
  clk_uv_15d,
  clk_uv_1m,
  t5.inter_rate_3_6w inter_rate_3_6w, -- 7天上网率
  t7.lrf_rate_9_12w  lrf_rate_9_12w, -- 物流退款率
  t6.nlrf_rate_5_8w  nlrf_rate_5_8w, -- 非物流退款率
  t5.bs_inter_rate_3_6w bs_inter_rate_3_6w, -- 7天上网率
  t7.bs_lrf_rate_9_12w  bs_lrf_rate_9_12w, -- 物流退款率
  t6.bs_nlrf_rate_5_8w  bs_nlrf_rate_5_8w,  -- 非物流退款率
  ord_cnt_1w,
  ord_cnt_15d,
  ord_cnt_1m
from tmp_goods_portrait a
left join tmp_goods_painting_pt b
  on a.goods_id = b.goods_id
left join tmp_goods_painting_most_popular_clk c
  on a.goods_id = c.goods_id
left join tmp_goods_painting_most_popular_cart d
  on a.goods_id = d.goods_id
left join tmp_goods_painting_inter_rate_3_6w t5
  on a.goods_id = t5.goods_id
left join tmp_goods_painting_nlrf_rate_5_8w t6
  on a.goods_id = t6.goods_id
left join tmp_goods_painting_lrf_rate_9_12w t7
  on a.goods_id = t7.goods_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 6G \
--executor-memory 6G --executor-cores 1 \
--conf spark.executor.memoryOverhead=2048 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_goods_portrait" \
--conf "spark.default.parallelism = 577" \
--conf "spark.sql.shuffle.partitions=577" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=500000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
