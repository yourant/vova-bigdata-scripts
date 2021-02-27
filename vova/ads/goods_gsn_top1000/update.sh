#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with tmp_order_cause_data as
(
SELECT
    nvl ( og.region_code, 'NALL' ) ctry,
CASE
    WHEN oc.pre_page_code = 'homepage' AND oc.pre_list_type = '/popular' THEN'rec_best_selling'
    WHEN oc.pre_page_code IN ( 'homepage', 'product_list' ) AND oc.pre_list_type IN ( '/product_list_newarrival', 'product_list_newarrival' ) THEN 'rec_new_arrival'
    WHEN oc.pre_page_code IN ( 'homepage', 'product_list' ) AND oc.pre_list_type IN ( '/product_list_popular', 'product_list_popular' ) THEN 'rec_most_popular'
    WHEN oc.pre_page_code IN ( 'homepage', 'product_list' ) AND oc.pre_list_type IN ( '/product_list_sold', 'product_list_sold' ) THEN 'rec_sold'
    WHEN oc.pre_page_code IN ( 'h5flashsale', 'flashsale', 'h5flashsale_catlist', 'homepage' ) AND oc.pre_list_type IN ( '/hp_flashsale', '/hp_flashsale/', '/h5flashsale_category_list', '/h5flashsale_main_list', '/flashsale_2', '/flashsale' ) THEN 'rec_flash_sale'
    WHEN oc.pre_page_code = 'product_detail' AND oc.pre_list_type = '/detail_also_like' THEN 'rec_product_detail'
    WHEN oc.pre_page_code IN ( 'my_favorites' ) AND oc.pre_list_type IN ( '/favorites', 'favorites' ) THEN 'rec_wishlist'
    WHEN oc.pre_page_code = 'search_result' AND oc.pre_list_type IN ( '/search_result', '/search_result_recommend', '/search_result_sold', '/search_result_price_desc', ' /search_result_price_asc' ) THEN 'rec_search_result'
    WHEN oc.pre_page_code IN ( 'recently_View', 'recently_view' ) AND oc.pre_list_type = '/recently_View' THEN 'rec_recently_View'
    WHEN oc.pre_page_code = 'coins_rewards' AND oc.pre_list_type = '/coins_rewards' THEN' rec_coins_rewards'
    WHEN oc.pre_page_code = 'cart' AND oc.pre_list_type = '/cart_also_like' THEN 'rec_cart'
    WHEN oc.pre_page_code IN ( 'merchant_store' ) AND oc.pre_list_type IN ( '/merchant_store', 'merchant_store' ) THEN 'rec_merchant_store'
    WHEN oc.pre_page_code = 'me' AND oc.pre_list_type = '/me_also_like' THEN 'rec_me'
    WHEN oc.pre_page_code = 'payment_success' AND oc.pre_list_type = '/pay_success' THEN 'rec_payment_success'
    WHEN oc.pre_page_code = 'theme_activity' THEN 'rec_theme_activity'
    ELSE 'others' END rec_page_code,
    og.goods_id AS gs_id,
    dg.goods_sn AS gs_sn,
    og.goods_number * og.shop_price + og.shipping_fee AS gmv
FROM
    dwd.dwd_vova_fact_pay og
    LEFT JOIN dim.dim_vova_goods dg on og.goods_id = dg.goods_id
    LEFT JOIN dwd.dwd_vova_fact_order_cause_v2 oc ON og.order_goods_id = oc.order_goods_id and oc.pt='${cur_date}'
WHERE
    to_date( og.pay_time ) = '${cur_date}'
    AND dg.goods_sn LIKE 'GSN%'
    AND ( og.from_domain LIKE '%api.vova%' OR og.from_domain LIKE '%api.airyclub%' )
),
tmp_top1000gsn_data(
SELECT
    gs_sn
FROM
    ( SELECT gs_sn, sum( gmv ) AS gmv FROM tmp_order_cause_data GROUP BY gs_sn )
ORDER BY
    gmv DESC
    LIMIT 1000
)
insert overwrite table ads.ads_vova_gsn_top1000 partition(pt='${cur_date}')
select
expre_cube_data.ctry as ctry,
expre_cube_data.rec_page_code as rec_page_code,
dg.goods_sn as gs_sn,
expre_cube_data.gs_id as gs_id,
dg.shop_price as price,
dg.shipping_fee as shipping_fee,
nvl(expre_cube_data.expres,0) as pv,
nvl(ord_cause_cube_data.gmv,0) as gmv
from
(
SELECT
    nvl ( expre_data.ctry, 'all' ) AS ctry,
    nvl ( expre_data.rec_page_code, 'all' ) AS rec_page_code,
    nvl ( expre_data.gs_id, 'all' ) AS gs_id,
    count( 1 ) AS expres
FROM
    (
SELECT
    nvl ( gi.country, 'NALL' ) AS ctry,
CASE
    WHEN gi.page_code = 'homepage' AND gi.list_type = '/popular' THEN 'rec_best_selling'
    WHEN gi.page_code IN ( 'homepage', 'product_list' ) AND gi.list_type IN ( '/product_list_newarrival', 'product_list_newarrival' ) THEN 'rec_new_arrival'
    WHEN gi.page_code IN ( 'homepage', 'product_list' ) AND gi.list_type IN ( '/product_list_popular', 'product_list_popular' ) THEN 'rec_most_popular'
    WHEN gi.page_code IN ( 'homepage', 'product_list' ) AND gi.list_type IN ( '/product_list_sold', 'product_list_sold' ) THEN 'rec_sold'
    WHEN gi.page_code IN ( 'h5flashsale', 'flashsale', 'h5flashsale_catlist', 'homepage' ) AND gi.list_type IN ( '/hp_flashsale', '/hp_flashsale/', '/h5flashsale_category_list', '/h5flashsale_main_list', '/flashsale_2', '/flashsale' ) THEN 'rec_flash_sale'
    WHEN gi.page_code = 'product_detail' AND gi.list_type = '/detail_also_like' THEN 'rec_product_detail'
    WHEN gi.page_code IN ( 'my_favorites' ) AND gi.list_type IN ( '/favorites', 'favorites' ) THEN 'rec_wishlist'
    WHEN gi.page_code = 'search_result' AND gi.list_type IN ( '/search_result', '/search_result_recommend', '/search_result_sold', '/search_result_price_desc', ' /search_result_price_asc' ) THEN 'rec_search_result'
    WHEN gi.page_code IN ( 'recently_View', 'recently_view' ) AND gi.list_type = '/recently_View' THEN 'rec_recently_View'
    WHEN gi.page_code = 'coins_rewards' AND gi.list_type = '/coins_rewards' THEN 'rec_coins_rewards'
    WHEN gi.page_code = 'cart' AND gi.list_type = '/cart_also_like' THEN 'rec_cart'
    WHEN gi.page_code IN ( 'merchant_store' ) AND gi.list_type IN ( '/merchant_store', 'merchant_store' ) THEN 'rec_merchant_store'
    WHEN gi.page_code = 'me' AND gi.list_type = '/me_also_like' THEN 'rec_me'
    WHEN gi.page_code = 'payment_success' AND gi.list_type = '/pay_success' THEN 'rec_payment_success'
    WHEN gi.page_code = 'theme_activity' THEN 'rec_theme_activity'
    ELSE 'others' END rec_page_code,
    nvl ( dg.goods_id, 'NA' ) AS gs_id,
    dg.goods_sn as gs_sn
FROM
    dwd.dwd_vova_log_goods_impression gi
    INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gi.virtual_goods_id
WHERE
    gi.pt = '${cur_date}'
    AND os_type IN ( 'ios', 'android' )
    ) expre_data
    INNER JOIN tmp_top1000gsn_data ttd ON expre_data.gs_sn = ttd.gs_sn
GROUP BY
    expre_data.ctry,
    expre_data.rec_page_code,
    expre_data.gs_id WITH cube
)expre_cube_data

left join
(
SELECT
    nvl ( tocd.ctry, 'all' ) AS ctry,
    nvl ( tocd.rec_page_code, 'all' ) AS rec_page_code,
    nvl ( tocd.gs_id, 'all' ) AS gs_id,
    sum( gmv ) AS gmv
FROM
    tmp_order_cause_data tocd
    INNER JOIN tmp_top1000gsn_data ttd ON tocd.gs_sn = ttd.gs_sn
GROUP BY
    tocd.ctry,
    tocd.rec_page_code,
    tocd.gs_id WITH cube
)ord_cause_cube_data

ON ord_cause_cube_data.ctry = expre_cube_data.ctry
AND ord_cause_cube_data.rec_page_code = expre_cube_data.rec_page_code
AND ord_cause_cube_data.gs_id = expre_cube_data.gs_id
LEFT JOIN dim.dim_vova_goods dg
ON dg.goods_id = expre_cube_data.gs_id
where expre_cube_data.gs_id !='all'
"

spark-sql \
--executor-memory 5G --executor-cores 2 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=40" \
--conf "spark.app.name=ads_vova_gsn_top1000" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 500" \
--conf "spark.sql.shuffle.partitions=500" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
