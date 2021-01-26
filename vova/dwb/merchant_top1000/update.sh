#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
last_week=`date -d $cur_date"-6 day" +%Y-%m-%d`
last_second_week=`date -d $cur_date"-13 day" +%Y-%m-%d`
cur_month=${cur_date:0:7}
days=${cur_date:8:10}
echo "cur_date:'${cur_date}',cur_month:'${cur_month}'"
###逻辑sql
sql="
-- 商品gmv
with tmp_gmv_mct_cat_goods as(
      SELECT
        mct_id,
        first_cat_name,
        goods_id,
        SUM( shop_price * goods_number + shipping_fee ) AS gmv_mct_cat_goods
      FROM
        dwd.dwd_vova_fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( confirm_time ), 'yyyy-MM' ) = '${cur_month}' AND date(confirm_time)<='${cur_date}'
      GROUP BY
        mct_id,
        first_cat_name,
        goods_id
),
-- top1000 gmv商家(去掉)
tmp_gmv_mct_top_1000_f as(
      SELECT
        *
      FROM
        (
      SELECT
        *
      FROM
        (
      SELECT
        mct_id,
        gmv
      FROM
        ( SELECT mct_id, sum( gmv_mct_cat_goods ) gmv FROM tmp_gmv_mct_cat_goods GROUP BY mct_id )
      ORDER BY
        gmv DESC
        LIMIT 1000
        )
        )
      WHERE
        mct_id NOT IN ( '1', '8', '11630', '26414' )
),
-- top1000 gmv商家
tmp_gmv_mct_top_1000 as(
      SELECT
        *
      FROM
        (
      SELECT
        mct_id,
        gmv
      FROM
        ( SELECT mct_id, sum( gmv_mct_cat_goods ) gmv FROM tmp_gmv_mct_cat_goods GROUP BY mct_id )
      ORDER BY
        gmv DESC
        LIMIT 1000
        )
),
-- 热卖商品
tmp_goods_hot as(
      SELECT
        gcgr.first_cat_name,
        gcgr.goods_id
      FROM
        (
      SELECT
        first_cat_name,
        goods_id,
        ROW_NUMBER ( ) OVER ( PARTITION BY first_cat_name ORDER BY gmv_cat_goods DESC ) rn
      FROM
        ( SELECT first_cat_name, goods_id, sum( gmv_mct_cat_goods ) AS gmv_cat_goods FROM tmp_gmv_mct_cat_goods GROUP BY first_cat_name, goods_id ) gcg
        ) gcgr
        INNER JOIN (
      SELECT
        first_cat_name,
        cast(
        ROUND( CASE WHEN COUNT( 1 ) * 0.1 < 1 THEN 1 ELSE COUNT( 1 ) * 0.1 END, 0 ) AS INT
        ) sel_cnt
      FROM
        tmp_gmv_mct_cat_goods
      GROUP BY
        first_cat_name
        ) cgg ON gcgr.first_cat_name = cgg.first_cat_name
      WHERE
        gcgr.rn <= cgg.sel_cnt
),
-- 正在销售商品
tmp_goods_on_sale as(
      SELECT
        mct_id,
        count( 1 ) AS goods_on_sale_cnt,
        sum(if(goods_sn like concat('SN', '%'),1,0)) AS goods_on_sale_self_cnt,
        sum(if(goods_sn like concat('GSN', '%'),1,0)) AS goods_on_sale_clone_cnt
      FROM
        dim.dim_vova_goods dg
      WHERE
        is_on_sale = 1
      GROUP BY
        mct_id
),
-- 上新商品
tmp_goods_online_new as(
      SELECT
        mct_id,
        count( goods_id ) AS goods_new_cnt,
        sum( IF ( goods_sn LIKE concat( 'SN', '%' ), 1, 0 ) ) AS goods_new_self_cnt,
        sum( IF ( goods_sn LIKE concat( 'GSN', '%' ), 1, 0 ) ) AS goods_new_clone_cnt
      FROM
        (
      SELECT
        dim_goods.mct_id,
        dim_goods.goods_sn,
        gor.goods_id
      FROM
        ( SELECT DISTINCT(goods_id) FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}' ) gor
        INNER JOIN dim.dim_vova_goods dim_goods ON gor.goods_id = dim_goods.goods_id
        )
      GROUP BY
        mct_id
),
-- 商家主营类目
tmp_cat_main as(
SELECT
    mct_id,
    first_cat_id
FROM
    (
SELECT
    mct_id,
    first_cat_id,
    ROW_NUMBER ( ) OVER ( PARTITION BY mct_id ORDER BY on_sale_cnt DESC ) AS rk
FROM
    (
SELECT
    mct_id,
    first_cat_name as first_cat_id,
    sum( if(is_on_sale=1,1,0) ) AS on_sale_cnt
FROM
    dim.dim_vova_goods dg
GROUP BY
    mct_id,
    first_cat_name
    )
    )
WHERE
    rk =1
),
-- 店铺爆品数
tmp_cat_goods_hot_cnt as(
      SELECT
        mct_id,
        first_cat_id,
        count( goods_id ) AS goods_hot_cnt
      FROM
        (
      SELECT
        dim_goods.mct_id,
        tmp_cat_main.first_cat_id,
        dim_goods.goods_id
      FROM
        tmp_cat_main
        INNER JOIN dim.dim_vova_goods dim_goods ON dim_goods.mct_id = tmp_cat_main.mct_id
        AND dim_goods.first_cat_name = tmp_cat_main.first_cat_id
      WHERE
        dim_goods.goods_id IN ( SELECT goods_id FROM tmp_goods_hot )
        ) mct_main_cat_goods
      GROUP BY
        mct_id,
        first_cat_id
),
-- 新上商品的第一类目及新上商品总数
tmp_goods_new_cnt as(
      SELECT
        mct_id,
        first_cat_id,
        first_cat_name,
        goods_new_cnt
      FROM
        (
      SELECT
        mct_id,
        first_cat_id,
        first_cat_name,
        goods_new_cnt,
        ROW_NUMBER ( ) OVER ( PARTITION BY mct_id ORDER BY goods_new_cnt DESC ) rk
      FROM
        (
      SELECT
        mct_id,
        first_cat_id,
        first_cat_name,
        count( 1 ) AS goods_new_cnt
      FROM
        (
      SELECT
        dim_goods.mct_id,
        gor.goods_id,
        dim_goods.first_cat_id,
        dim_goods.first_cat_name
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}'  GROUP BY goods_id ) gor
        INNER JOIN dim.dim_vova_goods dim_goods ON gor.goods_id = dim_goods.goods_id
        ) gnm
      GROUP BY
        mct_id,
        first_cat_id,
        first_cat_name
        ) gnmc
        ) gnmr
      WHERE
        gnmr.rk = 1
),
-- 最后一次操作上线商品时间
tmp_oper_online as(
      SELECT
        mct_id,
        datediff( now( ), max( create_time ) ) AS operate_gap_day_cnt
      FROM
        (
      SELECT
        dg.mct_id,
        gosr.goods_id,
        gosr.create_time
      FROM
        ods_vova_vts.ods_vova_goods_on_sale_record gosr
        INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = gosr.goods_id
      WHERE
        gosr.action = 'on'
        )
      GROUP BY
        mct_id
),
-- brand商品占比（brand_id>0）
tmp_brand_rate as(
      SELECT
        mct_id,
        sum( is_brand ) / count( is_brand ) AS brand_rate
      FROM
        (
      SELECT
        mct_id,
        goods_id,
      IF
        ( brand_id > 0, 1, 0 ) AS is_brand
      FROM
        dim.dim_vova_goods dg
      WHERE
        is_on_sale = 1
        ) gb
      GROUP BY
        mct_id
),
-- 当月上架和在售商品数去重数
tmp_goods_can_sale_cnt as(
      SELECT
        mct_id,
        count( DISTINCT ( goods_id ) ) AS goods_can_sale_cnt
      FROM
        (
        (
      SELECT
        dg.mct_id,
        gcm.goods_id
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}' GROUP BY goods_id ) gcm
        LEFT JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
        ) UNION ALL
        ( SELECT mct_id, goods_id FROM dim.dim_vova_goods dg WHERE is_on_sale = 1 )
        ) gal
      GROUP BY
        mct_id
),
-- 当月支付成功的商品数
tmp_goods_sold_cnt as(
      SELECT
        mct_id,
        count( DISTINCT goods_id ) AS goods_sold_cnt
      FROM
        dwd.dwd_vova_fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}' AND date(pay_time)<='${cur_date}'
      GROUP BY
        mct_id
),
-- 当月上架商品
tmp_online_goods_new as(
      SELECT
        dg.mct_id,
        dg.goods_id
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}' GROUP BY goods_id ) gcm
        INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
),
-- 当月新发布且支付成功商品数
tmp_goods_new_sold_cnt as(
      SELECT
        mct_id,
        count( goods_id ) AS goods_sold_new_cnt
      FROM
        tmp_online_goods_new
      WHERE
        goods_id IN ( SELECT goods_id FROM dwd.dwd_vova_fact_pay WHERE from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}' AND date(pay_time)<='${cur_date}' GROUP BY goods_id )
      GROUP BY
        mct_id
),
-- 当月支付成功订单数
tmp_pay_order_cnt as(
      SELECT
        mct_id,
        count( order_goods_id ) AS order_goods_cnt
      FROM
        dwd.dwd_vova_fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}'  AND date(pay_time)<='${cur_date}'
      GROUP BY
        mct_id
),
-- 商家gmv
tmp_mct_gmv as(
      SELECT
        mct_id,
        sum( gmv_mct_cat_goods ) AS gmv
      FROM
        tmp_gmv_mct_cat_goods
      GROUP BY
        mct_id
),
-- 主营类目销量
tmp_main_cat_gmv as(
      SELECT
        tmp_cat_main.mct_id,
        tmp_cat_main.first_cat_id AS first_cat_name,
        sum( fp.goods_number ) AS gmv_main_cat
      FROM
        tmp_cat_main
        LEFT JOIN (select * from dwd.dwd_vova_fact_pay where from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}' AND date(pay_time)<='${cur_date}')   fp
        ON fp.mct_id = tmp_cat_main.mct_id
        AND fp.first_cat_name = tmp_cat_main.first_cat_id

      GROUP BY
        tmp_cat_main.mct_id,
        tmp_cat_main.first_cat_id
),
-- 近30日5天发货率(近35日到近5日)
tmp_5_day_del_rate as(
      SELECT
        mct_id,
        sum( IF ( day_gap_cnt <= 5, 1, 0 ) ) / count( 1 ) AS ship_5_day_cnt
      FROM
        (
      SELECT
        fp.mct_id,
        fp.order_goods_id,
        datediff( fl.shipping_time, fp.confirm_time ) AS day_gap_cnt
      FROM
        dwd.dwd_vova_fact_logistics fl
        LEFT JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 35 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 4 )
        )
      GROUP BY
        mct_id
),
-- 平均发货时长
tmp_shop_del_time_avg(
      SELECT
        mct_id,
        sum(
      IF
        ( unix_timestamp( shipping_time ) - unix_timestamp( pay_time ) > 0, unix_timestamp( shipping_time ) - unix_timestamp( pay_time ), 0 )
        ) / count( 1 ) / 60 / 60 AS del_time_avg
      FROM
        (
      SELECT
        fp.mct_id,
        fl.order_goods_id,
        fp.order_goods_id,
        fl.shipping_time,
        fp.pay_time
      FROM
        dwd.dwd_vova_fact_logistics fl
        INNER JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 35 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 4 )
        )
      GROUP BY
        mct_id
),
-- 平均上线时长
tmp_shop_online_time_avg(
      SELECT
        mct_id,
        sum(
      IF
        ( unix_timestamp( valid_tracking_date ) - unix_timestamp( shipping_time ) > 0, unix_timestamp( valid_tracking_date ) - unix_timestamp( shipping_time ), 0 )
        ) / count( 1 ) / 60 / 60 AS online_time_avg
      FROM
        (
      SELECT
        fp.mct_id,
        fl.order_goods_id,
        fl.valid_tracking_date,
        fl.shipping_time
      FROM
        dwd.dwd_vova_fact_logistics fl
        INNER JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 37 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 6 )
        )
      GROUP BY
        mct_id
),
-- flash sale单量占比(当月)
tmp_order_flash_sale_rate(
      SELECT
        mct_id,
        sum( is_flash_sale ) / count( is_flash_sale ) AS flash_order_rate
      FROM
        (
      SELECT
        mct_id,
        order_goods_id,
      IF
        ( oge.ext_name = 'is_flash_sale', 1, 0 ) is_flash_sale
      FROM
        dim.dim_vova_order_goods dog
        INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON dog.order_goods_id = oge.rec_id
      WHERE
        from_unixtime( to_unix_timestamp ( order_time ), 'yyyy-MM' ) = '${cur_month}'  AND date(order_time)<='${cur_date}'
        )
      GROUP BY
        mct_id
),

-- last_week与last_last_second_week 商品gmv
tmp_gmv_week as(
      SELECT
      last_week_t.mct_id,
      last_week_t.gmv/last_sec_week_t.gmv as gmv_wow
      FROM
        (
      SELECT
        mct_id,
        SUM( shop_price * goods_number + shipping_fee ) AS gmv
      FROM
        dwd.dwd_vova_fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( confirm_time ), 'yyyy-MM-dd' ) >= '${last_week}'
        AND from_unixtime( to_unix_timestamp ( confirm_time ), 'yyyy-MM-dd' ) <= '${cur_date}'
      GROUP BY
        mct_id
        ) last_week_t
        INNER JOIN (
      SELECT
        mct_id,
        SUM( shop_price * goods_number + shipping_fee ) AS gmv
      FROM
        dwd.dwd_vova_fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( confirm_time ), 'yyyy-MM-dd' ) >= '${last_second_week}'
        AND from_unixtime( to_unix_timestamp ( confirm_time ), 'yyyy-MM-dd' ) < '${last_week}'
      GROUP BY
        mct_id
        ) last_sec_week_t ON last_week_t.mct_id = last_sec_week_t.mct_id
),

-- 近37到7日上网率
tmp_shop_online_rate(
      SELECT
        mct_id,
        count( IF ( datediff( fl.valid_tracking_date, fl.confirm_time ) <= 7, fl.order_goods_id, NULL ) ) / count( * )* 100 AS ship_online_rate
      FROM
        (
      SELECT
        mct_id,
        order_goods_id
      FROM
        dwd.dwd_vova_fact_pay fp LEFT anti
        JOIN ods_vova_vts.ods_vova_refund_reason vrr ON fp.order_goods_id = vrr.order_goods_id
        AND vrr.refund_type_id IN ( 1, 2, 5 )
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 37 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 6 )
        ) pay_without_refund_data
        LEFT JOIN dwd.dwd_vova_fact_logistics fl ON fl.order_goods_id = pay_without_refund_data.order_goods_id
      GROUP BY
        mct_id
),


-- top1000商家在售商品数
tmp_goods_on_sale_cnt_top1000(
      SELECT
        count( * ) goods_on_sale_cnt,
        sum(if(goods_sn like concat('SN', '%'),1,0)) AS goods_on_sale_self_cnt,
        sum(if(goods_sn like concat('GSN', '%'),1,0)) AS goods_on_sale_clone_cnt
      FROM
        dim.dim_vova_goods
      WHERE
        is_on_sale = 1
        AND mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
),
-- top1000商家上新商品数
tmp_goods_online_new_top1000 as(
        SELECT
        count( goods_id ) AS goods_new_cnt,
        sum( IF ( goods_sn LIKE concat( 'SN', '%' ), 1, 0 ) ) AS goods_new_self_cnt,
        sum( IF ( goods_sn LIKE concat( 'GSN', '%' ), 1, 0 ) ) AS goods_new_clone_cnt
      FROM
        (
      SELECT
        dim_goods.mct_id,
        dim_goods.goods_sn,
        gor.goods_id
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}' GROUP BY goods_id ) gor
        INNER JOIN dim.dim_vova_goods dim_goods ON gor.goods_id = dim_goods.goods_id
        WHERE mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
),
--  top1000商家上次操作距今的总天数
tmp_oper_online_top1000 as(
      SELECT
        sum( operate_gap_day_cnt ) AS operate_gap_day_cnt
      FROM
        (
      SELECT
        mct_id,
        datediff( now( ), max( create_time ) ) AS operate_gap_day_cnt
      FROM
        (
      SELECT
        dg.mct_id,
        gosr.goods_id,
        gosr.create_time
      FROM
        ods_vova_vts.ods_vova_goods_on_sale_record gosr
        INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = gosr.goods_id
      WHERE
        gosr.action = 'on'
        AND dg.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
      GROUP BY
        mct_id
        )
),
-- top1000的卖家,在架的备注了品牌的商品数占总在架商品数的占比
tmp_brand_rate_top1000 as(
      SELECT
        sum( is_brand ) / count( is_brand ) AS brand_rate
      FROM
        (
      SELECT
        mct_id,
        goods_id,
      IF
        ( brand_id > 0, 1, 0 ) AS is_brand
      FROM
        dim.dim_vova_goods dg
      WHERE
        is_on_sale = 1
        AND mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        ) gb
),

-- top1000的卖家,当月上架和在售商品数去重数
tmp_goods_can_sale_cnt_top1000 as(
      SELECT
        count( DISTINCT ( goods_id ) ) AS goods_can_sale_cnt
      FROM
        (
        (
      SELECT
        dg.mct_id,
        gcm.goods_id
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}'  GROUP BY goods_id ) gcm
        INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
      WHERE
        mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        ) UNION ALL
        ( SELECT mct_id, goods_id FROM dim.dim_vova_goods dg WHERE is_on_sale = 1 AND mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f ) )
        ) gal
),

-- top1000的卖家，当月支付成功的商品数
tmp_goods_sold_cnt_top1000 as(
      SELECT
        count( DISTINCT goods_id ) AS goods_sold_cnt
      FROM
        dwd.dwd_vova_fact_pay fact_pay
      WHERE
        from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}'
        AND fact_pay.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
),
-- top1000的卖家，当月上架商品数
tmp_online_goods_new_cnt_top1000 as(
      SELECT
        count( 1 ) AS goods_online_new_cnt
      FROM
        ( SELECT goods_id FROM ods_vova_vts.ods_vova_goods_on_sale_record WHERE action = 'on' AND from_unixtime( to_unix_timestamp ( create_time ), 'yyyy-MM' ) = '${cur_month}' AND date(create_time)<='${cur_date}'  GROUP BY goods_id ) gcm
        INNER JOIN dim.dim_vova_goods dg ON gcm.goods_id = dg.goods_id
      WHERE
        mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
),

-- top1000的卖家，当月新发布且支付成功商品数
tmp_goods_new_sold_cnt_top1000 as(
      SELECT
        count( goods_id ) AS goods_sold_new_cnt
      FROM
        tmp_online_goods_new
      WHERE
        goods_id IN ( SELECT goods_id FROM dwd.dwd_vova_fact_pay WHERE from_unixtime( to_unix_timestamp ( pay_time ), 'yyyy-MM' ) = '${cur_month}' AND date(pay_time)<='${cur_date}'  GROUP BY goods_id )
        AND mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
),
-- top1000的卖家，近30日5天发货率(近35日到近5日)
tmp_5_day_del_rate_top1000 as(
      SELECT
        sum( IF ( day_gap_cnt <= 5, 1, 0 ) ) / count( 1 ) AS ship_5_day_rate
      FROM
        (
      SELECT
        fp.mct_id,
        fp.order_goods_id,
        datediff( fl.shipping_time, fp.confirm_time ) AS day_gap_cnt
      FROM
        dwd.dwd_vova_fact_logistics fl
        LEFT JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 35 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 4 )
        AND fp.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
),
-- top1000的卖家，平均发货时长
tmp_shop_del_time_avg_top1000(
      SELECT
        sum(
      IF
        ( unix_timestamp( shipping_time ) - unix_timestamp( pay_time ) > 0, unix_timestamp( shipping_time ) - unix_timestamp( pay_time ), 0 )
        ) / count( 1 ) / 60 / 60 AS del_time_avg
      FROM
        (
      SELECT
        fp.mct_id,
        fl.order_goods_id,
        fp.order_goods_id,
        fl.shipping_time,
        fp.pay_time
      FROM
        dwd.dwd_vova_fact_logistics fl
        INNER JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 35 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 4 )
        AND fp.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
),
-- top1000的卖家，平均上线时长
tmp_shop_online_time_avg_top1000(
      SELECT
        sum(
      IF
        ( unix_timestamp( valid_tracking_date ) - unix_timestamp( shipping_time ) > 0, unix_timestamp( valid_tracking_date ) - unix_timestamp( shipping_time ), 0 )
        ) / count( 1 ) / 60 / 60 AS online_time_avg
      FROM
        (
      SELECT
        fp.mct_id,
        fl.order_goods_id,
        fl.valid_tracking_date,
        fl.shipping_time
      FROM
        dwd.dwd_vova_fact_logistics fl
        INNER JOIN dwd.dwd_vova_fact_pay fp ON fl.order_goods_id = fp.order_goods_id
      WHERE
        fp.confirm_time BETWEEN date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 37 )
        AND date_sub( to_date ( '${cur_date}', 'yyyy-MM-dd' ), 6 )
        AND fp.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
),
-- top1000的卖家，flash sale单量占比(当月)
tmp_order_flash_sale_rate_top1000(
      SELECT
        sum( is_flash_sale ) / count( is_flash_sale ) AS flash_order_rate
      FROM
        (
      SELECT
        mct_id,
        order_goods_id,
      IF
        ( oge.ext_name = 'is_flash_sale', 1, 0 ) is_flash_sale
      FROM
        dim.dim_vova_order_goods dog
        INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON dog.order_goods_id = oge.rec_id
      WHERE
        from_unixtime( to_unix_timestamp ( order_time ), 'yyyy-MM' ) = '${cur_month}' AND date(order_time)<='${cur_date}'
        AND dog.mct_id IN ( SELECT mct_id FROM tmp_gmv_mct_top_1000_f )
        )
)
insert overwrite table dwb.dwb_vova_mct_top1000 partition(event_date='${cur_date}')
SELECT
/*+ REPARTITION(1) */
datasource,
mct_name,
goods_on_sale_cnt,
goods_hot_cnt,
main_cat_new,
main_cat_new_goods_cnt,
goods_on_sale_self_cnt,
goods_new_self_cnt,
goods_on_sale_clone_cnt,
goods_new_clone_cnt,
goods_new_cnt,
operate_gap_day_cnt,
goods_brand_rate,
goods_sold_rate,
goods_new_sold_rate,
order_cnt,
gmv,
main_cat,
main_cat_order_cnt,
five_day_del_rate_1m,
del_time_avg,
online_time_avg,
flash_order_rate,
gmv_day_avg,
gmv_wow,
ship_online_rate
from
((select
'vova' as datasource,
mct_name,
nvl(tmp_goods_on_sale.goods_on_sale_cnt,0) as goods_on_sale_cnt,
nvl(tmp_cat_goods_hot_cnt.goods_hot_cnt,0) as goods_hot_cnt,
nvl(tmp_goods_new_cnt.first_cat_name,0) as main_cat_new,
nvl(tmp_goods_new_cnt.goods_new_cnt,0) as main_cat_new_goods_cnt,
nvl(tmp_goods_on_sale.goods_on_sale_self_cnt,0) as goods_on_sale_self_cnt,
nvl(tmp_goods_online_new.goods_new_self_cnt,0) as goods_new_self_cnt,
nvl(tmp_goods_on_sale.goods_on_sale_clone_cnt,0) as goods_on_sale_clone_cnt,
nvl(tmp_goods_online_new.goods_new_clone_cnt,0) as goods_new_clone_cnt,
nvl(tmp_goods_online_new.goods_new_cnt,0) as goods_new_cnt,
nvl(tmp_oper_online.operate_gap_day_cnt,0) as operate_gap_day_cnt,
nvl(tmp_brand_rate.brand_rate,0)*100 as goods_brand_rate,
nvl(tmp_goods_sold_cnt.goods_sold_cnt,0)/nvl(tmp_goods_can_sale_cnt.goods_can_sale_cnt,0)*100 as goods_sold_rate,
if(nvl(tmp_online_goods_new_cnt.goods_online_new_cnt,0)==0,0,nvl(tmp_goods_new_sold_cnt.goods_sold_new_cnt,0)/nvl(tmp_online_goods_new_cnt.goods_online_new_cnt,0))*100 as goods_new_sold_rate,
tmp_pay_order_cnt.order_goods_cnt as order_cnt,
tmp_mct_gmv.gmv,
tmp_main_cat_gmv.first_cat_name as main_cat,
nvl(tmp_main_cat_gmv.gmv_main_cat,0) as main_cat_order_cnt,
nvl(tmp_5_day_del_rate.ship_5_day_cnt ,0)*100 as five_day_del_rate_1m,
nvl(tmp_shop_del_time_avg.del_time_avg,0) as del_time_avg,
nvl(tmp_shop_online_time_avg.online_time_avg,0) as online_time_avg,
nvl(tmp_order_flash_sale_rate.flash_order_rate,0)*100 as flash_order_rate,
tmp_mct_gmv.gmv/cast('${days}' as int) as gmv_day_avg,
tmp_gmv_week.gmv_wow as gmv_wow,
tmp_shop_online_rate.ship_online_rate as ship_online_rate
from dim.dim_vova_merchant dm
left join tmp_goods_on_sale
on dm.mct_id = tmp_goods_on_sale.mct_id
left join tmp_cat_goods_hot_cnt
on dm.mct_id = tmp_cat_goods_hot_cnt.mct_id
left join tmp_goods_new_cnt
on dm.mct_id = tmp_goods_new_cnt.mct_id
left join tmp_goods_online_new
on dm.mct_id = tmp_goods_online_new.mct_id
left join  tmp_oper_online
on dm.mct_id=tmp_oper_online.mct_id
left join tmp_brand_rate
on dm.mct_id=tmp_brand_rate.mct_id
left join tmp_goods_can_sale_cnt
on dm.mct_id=tmp_goods_can_sale_cnt.mct_id
left join tmp_goods_sold_cnt
on dm.mct_id=tmp_goods_sold_cnt.mct_id
left join (select mct_id, count(1) as goods_online_new_cnt from tmp_online_goods_new group by mct_id)tmp_online_goods_new_cnt
on dm.mct_id=tmp_online_goods_new_cnt.mct_id
left join tmp_goods_new_sold_cnt
on dm.mct_id=tmp_goods_new_sold_cnt.mct_id
left join tmp_pay_order_cnt
on dm.mct_id=tmp_pay_order_cnt.mct_id
left join tmp_mct_gmv
on dm.mct_id=tmp_mct_gmv.mct_id
left join tmp_main_cat_gmv
on dm.mct_id=tmp_main_cat_gmv.mct_id
left join tmp_5_day_del_rate
on dm.mct_id=tmp_5_day_del_rate.mct_id
left join tmp_shop_del_time_avg
on dm.mct_id=tmp_shop_del_time_avg.mct_id
left join tmp_shop_online_time_avg
on dm.mct_id=tmp_shop_online_time_avg.mct_id
left join tmp_order_flash_sale_rate
on dm.mct_id=tmp_order_flash_sale_rate.mct_id
left join tmp_gmv_week
on dm.mct_id=tmp_gmv_week.mct_id
left join tmp_shop_online_rate
on dm.mct_id=tmp_shop_online_rate.mct_id
where dm.mct_id in (select mct_id from tmp_gmv_mct_top_1000)
order by  tmp_mct_gmv.gmv)

union all

(select
'vova' as datasource,
'Top1000平台数据' as mct_name,
-- 平均在售商品数
nvl(tmp_goods_on_sale_cnt_top1000.goods_on_sale_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_on_sale_cnt,
0 as goods_hot_cnt,
null as main_cat_new,
0 as main_cat_new_goods_cnt,
nvl(tmp_goods_on_sale_cnt_top1000.goods_on_sale_self_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_on_sale_self_cnt,
nvl(tmp_goods_online_new_top1000.goods_new_self_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_new_self_cnt,
nvl(tmp_goods_on_sale_cnt_top1000.goods_on_sale_clone_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_on_sale_clone_cnt,
nvl(tmp_goods_online_new_top1000.goods_new_clone_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_new_clone_cnt,
-- 平均新上商品数
nvl(tmp_goods_online_new_top1000.goods_new_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as goods_new_cnt,
-- 上次操作距今的平均天数
nvl(tmp_oper_online_top1000.operate_gap_day_cnt,0)/(select count(*) from tmp_gmv_mct_top_1000_f) as operate_gap_day_cnt,
-- top1000的卖家，在架的备注了品牌的商品数占总在架商品数 的占比
nvl(tmp_brand_rate_top1000.brand_rate,0)*100 as goods_brand_rate,
-- top1000的卖家，商品动销率
nvl(tmp_goods_sold_cnt_top1000.goods_sold_cnt,0)/nvl(tmp_goods_can_sale_cnt_top1000.goods_can_sale_cnt,0)*100 as goods_sold_rate,
-- top1000的卖家，新品动销率
if(nvl(tmp_online_goods_new_cnt_top1000.goods_online_new_cnt,0)==0,0,nvl(tmp_goods_new_sold_cnt_top1000.goods_sold_new_cnt,0)/nvl(tmp_online_goods_new_cnt_top1000.goods_online_new_cnt,0))*100 as goods_new_sold_rate,
0 as order_cnt,
0 as gmv,
null as main_cat,
0 as main_cat_order_cnt,
-- top1000的卖家，当月累计天数内的订单的5天发货率相加/1000
nvl(tmp_5_day_del_rate_top1000.ship_5_day_rate ,0)*100 as five_day_del_rate_1m,
-- top1000的卖家，（当月累计天数内内的所有订单发货时间-订单被支付时间)/(select count(*) from tmp_gmv_mct_top_1000_f)
nvl(tmp_shop_del_time_avg_top1000.del_time_avg,0) as del_time_avg,
-- top1000的卖家，(当月累计天数内的所有订单的上线时间-发货时间）/(select count(*) from tmp_gmv_mct_top_1000_f)
nvl(tmp_shop_online_time_avg_top1000.online_time_avg,0) as online_time_avg,
-- top1000的卖家，当月累计天数内flash sale 的活动单量/当月累计天数内的总单量
nvl(tmp_order_flash_sale_rate_top1000.flash_order_rate,0)*100 as flash_order_rate,
null as gmv_day_avg,
null as gmv_wow,
null as ship_online_rate
from tmp_goods_on_sale_cnt_top1000
CROSS JOIN tmp_goods_online_new_top1000
CROSS JOIN tmp_oper_online_top1000
CROSS JOIN tmp_brand_rate_top1000
CROSS JOIN tmp_goods_sold_cnt_top1000
CROSS JOIN tmp_goods_can_sale_cnt_top1000
CROSS JOIN tmp_online_goods_new_cnt_top1000
CROSS JOIN tmp_goods_new_sold_cnt_top1000
CROSS JOIN tmp_5_day_del_rate_top1000
CROSS JOIN tmp_shop_del_time_avg_top1000
CROSS JOIN tmp_shop_online_time_avg_top1000
CROSS JOIN tmp_order_flash_sale_rate_top1000))
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_mct_top1000" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi