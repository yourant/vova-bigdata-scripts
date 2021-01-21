#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
pre_3m=`date -d "89 day ago ${pre_date}" +%Y-%m-%d`
pre_1m=`date -d "29 day ago ${pre_date}" +%Y-%m-%d`
sql="
drop table if exists tmp.ads_buyer_portrait_act_score;
create table tmp.ads_buyer_portrait_act_score as
select
buyer_id,
sum(buyer_act_score) buyer_act_score
from
(
select
buyer_act,
buyer_id,
case when buyer_act='7' then buyer_act_cnt*4
     when buyer_act='7-14' then buyer_act_cnt*2
     when buyer_act='14-21' then buyer_act_cnt*1
     else 0 end buyer_act_score
from
(
select
buyer_act,
buyer_id,
count(*) buyer_act_cnt
from
(
select
buyer_id,
case when datediff('$pre_date',pt)<7 then '7'
     when datediff('$pre_date',pt)>=7 and datediff('$pre_date',pt)<14 then '7-14'
     when datediff('$pre_date',pt)>=14 and datediff('$pre_date',pt)<21 then '14-21'
     else  '21-90' end buyer_act
from
(
select
pt,
buyer_id
from dwd.fact_start_up where pt>='$pre_3m' and buyer_id>0
group by pt,buyer_id
) t
) t group by buyer_act,buyer_id
) t
) t group by buyer_id;

drop table if exists tmp.ads_buyer_portrait_trade;
create table tmp.ads_buyer_portrait_trade as
select
b.buyer_id,
nvl(t1.is_receive,0) is_receive,
nvl(t2.is_pay,0) is_pay,
nvl(t3.is_order,0) is_order,
nvl(t4.is_cart,0) is_cart,
nvl(t5.is_clk,0) is_clk
from dwd.dim_buyers b
left join (select distinct buyer_id, 1 is_receive from dwd.dim_order_goods where sku_shipping_status=2) t1 on b.buyer_id = t1.buyer_id
left join (select distinct buyer_id, 1 is_pay from dwd.fact_pay) t2 on b.buyer_id = t2.buyer_id
left join (select distinct buyer_id, 1 is_order from dwd.dim_order_goods where to_date(order_time)>='$pre_3m') t3 on b.buyer_id = t3.buyer_id
left join (select distinct buyer_id, 1 is_cart from dwd.fact_log_common_click where pt>='$pre_3m' and buyer_id>0 and element_name='pdAddToCartSuccess') t4 on b.buyer_id = t4.buyer_id
left join (select distinct buyer_id, 1 is_clk from dwd.fact_log_goods_click where pt>='$pre_1m' and buyer_id>0) t5 on b.buyer_id = t5.buyer_id
;

ALTER TABLE ads.ads_buyer_portrait_feature DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
INSERT overwrite TABLE ads.ads_buyer_portrait_feature partition(pt='$pre_date')
SELECT
  db.buyer_id,
  db.gender AS reg_gender,
  db.user_age_group AS reg_age_group,
  db.reg_time,
  db.region_code AS reg_ctry,
  db.language_code AS lag_id,
  dd.child_channel AS reg_channel,
  nvl ( dd.platform, 'other' ) AS os_type,
  tmp_clk.gs_ids,
  tmp_add_cat.gs_ids,
  tmp_collect.gs_ids,
  tmp_ord.gs_ids,
  case when datediff(to_date('$pre_date'),db.reg_time)=0 then '1'
       when datediff(to_date('$pre_date'),db.reg_time)>=1 and datediff(to_date('$pre_date'),db.reg_time)<7 then '2-7'
       when datediff(to_date('$pre_date'),db.reg_time)>=7 and datediff(to_date('$pre_date'),db.reg_time)<30 then '7-30'
       else '30+' end reg_tag,
  case when bas.buyer_act_score>=12 then 'high_act_user'
       when bas.buyer_act_score>4 and bas.buyer_act_score<12 then 'mid_act_user'
       when bas.buyer_act_score>=1 and bas.buyer_act_score<=4 then 'low_act_user'
       when bas.buyer_act_score=0 then 'silent_user'
       else 'lost_user' end buyer_act,
  case when bt.is_receive =1 then 'is_receive'
       when bt.is_pay =1 then 'is_pay'
       when bt.is_order =1 then 'is_order'
       when bt.is_cart =1 then 'is_cart'
       when bt.is_clk =1 then 'is_clk'
       else 'no_clk' end  trade_act,
  db.datasource,
  CASE
    WHEN datediff( '$pre_date', last_start_up_date ) < 7 THEN 1
    WHEN datediff( '$pre_date', last_start_up_date ) < 15 THEN 2
    WHEN datediff( '$pre_date', last_start_up_date ) < 30 THEN 3
    ELSE 0
    END last_logint_type,
  nvl(tmp_pay_type.last_buyer_type,0) as last_buyer_type,
  nvl(tmp_pay_type.buy_times_type,0) as buy_times_type,
  db.first_order_time,
  tmp_ord.order_cnt,
  tmp_ord.last_order_time,
  nvl(tmp_ord.gmv/tmp_ord.order_cnt,0) as avg_price,
  nvl(tmp_email.email_act,0) as email_act,
  gs.gmv_stage,
  nvl(bbl.is_brand,0) as is_brand
FROM
  dwd.dim_buyers db
  LEFT JOIN dwd.dim_devices dd ON dd.device_id = db.current_device_id
  AND dd.datasource = db.datasource
  LEFT JOIN
  -- 近30天点击商品集合
  (
     SELECT
        t2.buyer_id,
        regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(t2.rk as string),5,'0'),cast(t2.gs_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS gs_ids
      FROM
        (
      SELECT
        t1.buyer_id,
        t1.goods_id AS gs_id,
        row_number ( ) over ( PARTITION BY t1.buyer_id ORDER BY t1.last_clk_time DESC ) rk
      FROM
        (
      SELECT
        gc.buyer_id,
        dg.goods_id,
        max( collector_tstamp ) AS last_clk_time
      FROM
        dwd.fact_log_goods_click gc
        INNER JOIN dwd.dim_goods dg ON gc.virtual_goods_id = dg.virtual_goods_id
      WHERE
        gc.pt <= '${pre_date}' and gc.pt > date_sub( '${pre_date}', 30 )
        AND gc.platform = 'mob'
        AND gc.collector_tstamp IS NOT NULL
      GROUP BY
        gc.buyer_id,
        dg.goods_id
        ) t1
        ) t2
      WHERE
        rk <= 100
      GROUP BY
        t2.buyer_id
  ) tmp_clk ON db.buyer_id = tmp_clk.buyer_id
  LEFT JOIN
  -- 近60天加购商品集合
  (
     SELECT
        t2.buyer_id,
        regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(t2.rk as string),5,'0'),cast(t2.gs_id as string))
                 )
               )
             ),
        '\\\d+:','') AS gs_ids
      FROM
        (
      SELECT
        t1.buyer_id,
        t1.goods_id AS gs_id,
        row_number() over(partition by t1.buyer_id order by last_time DESC) rk
      FROM
        (
      SELECT
        cc.buyer_id,
        dg.goods_id,
        max( collector_tstamp ) AS last_time
      FROM
        dwd.fact_log_common_click cc
        INNER JOIN dwd.dim_goods dg ON cc.element_id = dg.virtual_goods_id
      WHERE
        cc.pt <= '${pre_date}' and cc.pt > date_sub( '${pre_date}', 60 )
        AND cc.platform = 'mob'
        AND cc.element_name = 'pdAddToCartSuccess'
        AND collector_tstamp IS NOT NULL
      GROUP BY
        cc.buyer_id,
        dg.goods_id
        ) t1
        ) t2
      GROUP BY
        t2.buyer_id
  ) tmp_add_cat ON db.buyer_id = tmp_add_cat.buyer_id
  LEFT JOIN
  -- 近60天收藏商品集合
  (
     SELECT
        t2.buyer_id,
        regexp_replace(
               concat_ws(',',
                 sort_array(
                   collect_list(
                     concat_ws(':',lpad(cast(t2.rk as string),5,'0'),cast(t2.gs_id as string))
                   )
                 )
               ),
        '\\\d+:','') AS gs_ids
      FROM
        (
      SELECT
        t1.buyer_id,
        t1.goods_id AS gs_id,
        row_number() over(partition by t1.buyer_id order by last_time DESC) rk
      FROM
        (
      SELECT
        cc.buyer_id,
        dg.goods_id,
        max( collector_tstamp ) AS last_time
      FROM
        dwd.fact_log_common_click cc
        INNER JOIN dwd.dim_goods dg ON cc.element_id = dg.virtual_goods_id
      WHERE
        cc.pt <= '${pre_date}' and cc.pt > date_sub( '${pre_date}', 60 )
        AND cc.platform = 'mob'
        AND cc.element_name = 'pdAddToWishlistClick'
        AND collector_tstamp IS NOT NULL
      GROUP BY
        cc.buyer_id,
        dg.goods_id
        ) t1
        ) t2
      GROUP BY
        t2.buyer_id
  ) tmp_collect ON db.buyer_id = tmp_collect.buyer_id
  LEFT JOIN
  -- 购买订单
  (
    SELECT
            t2.buyer_id,
            sum(t2.order_cnt) as order_cnt,
            max(t2.last_pay_time) as last_order_time,
            sum(t2.gmv) as gmv,
            regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(t2.rk as string),5,'0'),cast(t2.gs_id as string))
                         )
                       )
                     ),
            '\\\d+:','') AS gs_ids
          FROM
            (
          SELECT
            t1.buyer_id,
            t1.gs_id AS gs_id,
            t1.order_cnt,
            t1.gmv,
            t1.last_pay_time,
            row_number() over(partition by t1.buyer_id order by last_pay_time DESC) rk
          FROM
            (
          SELECT
            fp.buyer_id,
            fp.goods_id AS gs_id,
            count(1) as order_cnt,
            collect_set(order_id) as coll_id,
            sum(shop_price*goods_number + shipping_fee) as gmv,
            max( fp.order_time ) AS last_pay_time
          FROM
            dwd.fact_pay fp
          --WHERE fp.platform IN ( 'ios', 'android' )
          GROUP BY
            fp.buyer_id,
            fp.goods_id
            ) t1
            ) t2
          GROUP BY
            t2.buyer_id
  ) tmp_ord ON db.buyer_id = tmp_ord.buyer_id
 LEFT JOIN tmp.ads_buyer_portrait_act_score bas ON bas.buyer_id = db.buyer_id
 LEFT JOIN tmp.ads_buyer_portrait_trade bt on bt.buyer_id = db.buyer_id
 LEFT JOIN(
 SELECT
buyer_id,
CASE
    WHEN datediff( '$pre_date', last_pay_time ) < 7 THEN 1
    WHEN datediff( '$pre_date', last_pay_time ) < 15 THEN 2
    WHEN datediff( '$pre_date', last_pay_time ) < 30 THEN 3
    ELSE 0
    END last_buyer_type,
CASE
    WHEN rate < 7 THEN 1
    WHEN rate < 15 THEN 2
    WHEN rate < 30 THEN 3
    ELSE 0
    END buy_times_type
FROM
    (
    SELECT
        buyer_id,
        max( pay_time ) AS last_pay_time,
        least(
            90 / count( DISTINCT order_id ),
            60 / count( DISTINCT IF ( day_gap < 60, order_id, NULL ) ),
            30 / count( DISTINCT IF ( day_gap < 30, order_id, NULL ) )
        ) rate
    FROM
        (
        SELECT
            buyer_id,
            order_id,
            pay_time,
            datediff( '$pre_date', pay_time ) AS day_gap
        FROM
            dwd.fact_pay
        WHERE
            to_date ( pay_time ) > date_sub( '$pre_date', 90 )
            AND to_date ( pay_time ) <= '$pre_date'
        )
    GROUP BY
    buyer_id
)
 )tmp_pay_type
 on db.buyer_id = tmp_pay_type.buyer_id
 LEFT JOIN
 (
select
  email,
  if(sum(if(rk<=7 and open_time is not null,1,0)) >0,1,if(sum(if(rk>7 and rk<=14 and open_time is not null, 1,0)) >0,2,if(sum(if(rk>14 and rk<=30 and open_time is not null, 1,0))>0 ,3,4))) email_act
  from
  (select
  email,
  open_time,
  row_number() over(partition by email order by send_time desc) rk
  from
  (select
      email,
      send_time,
      open_time
  from
      ods.vova_newsletter_send_email_all
    where http_code=200
    and send_time is not null
  union all
  select
    email,
    create_time as send_time,
    if(open=1,update_time,null) as open_time
    from
    ods.vova_email_send_log_a
    where status = 0
  union all
  select
    email,
    create_time as send_time,
    if(open=1,update_time,null) as open_time
    from
    ods.vova_email_send_log_b
    where status = 0
  union all
  select
    email,
    create_time as send_time,
    if(open=1,update_time,null) as open_time
    from
    ods.vova_email_send_log_c
    where status = 0
  )
  )
  where rk<=30
  group by
  email
 )tmp_email
on db.email = tmp_email.email


left join ads.ads_buyer_gmv_stage_3m gs
on db.buyer_id = gs.buyer_id
left join ads.ads_buyer_brand_level bbl
on db.buyer_id = bbl.buyer_id

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_buyer_portrait_feature" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
