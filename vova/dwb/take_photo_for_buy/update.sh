#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
-- req7424
WITH tmp_imp (
SELECT
    nvl ( app_version, 'all' ) app_version,
    nvl ( os_type, 'all' ) AS platform,
    count( DISTINCT ( IF ( page_code IN ( 'homepage', 'search_begin', 'category' ) AND element_name = 'image_search_button', device_id, NULL ) ) ) AS in_button_exp_uv,
    count( DISTINCT ( IF ( page_code = 'image_search_result' AND list_type = '/feature_image' AND element_name = 'feature_image' AND element_position = 1, device_id, NULL ) ) ) AS reg_zip_photo_exp_cnt
FROM
    dwd.dwd_vova_log_impressions li
WHERE
    pt = '${pre_date}'
    AND os_type IN ( 'ios', 'android' )
    AND platform = 'mob'
    AND (app_version >= '2.99.0' or app_version >= '2.100.0' and app_version <= '2.199.0')
GROUP BY
    app_version,
    os_type WITH cube
    ),
    tmp_sv (
SELECT
    nvl ( app_version, 'all' ) app_version,
    nvl ( os_type, 'all' ) AS platform,
    count( DISTINCT IF ( page_code = 'image_search', device_id, NULL ) ) sv_page_uv,
    count( IF ( page_code = 'image_search_result', device_id, NULL ) ) sv_page_result_pv
FROM
    dwd.dwd_vova_log_screen_view sv
WHERE
    sv.pt = '${pre_date}'
    AND os_type IN ( 'ios', 'android' )
    AND platform = 'mob'
    AND (app_version >= '2.99.0' or app_version >= '2.100.0' and app_version <= '2.199.0')
GROUP BY
    app_version,
    os_type WITH cube
    ),
    tmp_clk (
SELECT
    nvl ( app_version, 'all' ) app_version,
    nvl ( os_type, 'all' ) AS platform,
    count( DISTINCT ( IF ( page_code = 'image_search' AND element_name = 'camera_button', device_id, NULL ) ) ) AS button_clk_uv,
    count( DISTINCT ( IF ( page_code = 'image_search' AND element_name = 'album_button', device_id, NULL ) ) ) AS img_clk_uv,
    count( DISTINCT ( IF ( page_code = 'image_search' AND element_name = 'thumbnail', device_id, NULL ) ) ) AS zip_img_clk_uv,
    count( DISTINCT ( IF ( page_code IN ( 'homepage', 'search_begin', 'category' ) AND element_name = 'image_search_button', device_id, NULL ) ) ) AS in_button_clk_uv
FROM
    dwd.dwd_vova_log_click_arc cc
WHERE
    cc.pt = '${pre_date}'
    AND os_type IN ( 'ios', 'android' )
    AND platform = 'mob'
    AND (app_version >= '2.99.0' or app_version >= '2.100.0' and app_version <= '2.199.0')
GROUP BY
    app_version,
    os_type WITH cube
    ),
    tmp_goods_imp (
SELECT
    nvl ( app_version, 'all' ) app_version,
    nvl ( os_type, 'all' ) AS platform,
    count( DISTINCT ( IF ( page_code = 'image_search_result' AND list_type = '/image_search_recommend', device_id, NULL ) ) ) AS goods_exp_uv,
    count( IF ( page_code = 'image_search_result' AND list_type = '/image_search_recommend' AND absolute_position = 1, device_id, NULL ) ) AS goods_exp_result_dis_cnt,
    count( IF ( page_code = 'image_search_result' AND list_type = '/image_search_recommend', device_id, NULL ) ) AS goods_exp_result_cnt
FROM
    dwd.dwd_vova_log_goods_impression gi
WHERE
    gi.pt = '${pre_date}'
    AND os_type IN ( 'ios', 'android' )
    AND platform = 'mob'
    AND (app_version >= '2.99.0' or app_version >= '2.100.0' and app_version <= '2.199.0')
GROUP BY
    app_version,
    os_type WITH cube
    ),
    tmp_goods_clk (
SELECT
    nvl ( app_version, 'all' ) app_version,
    nvl ( os_type, 'all' ) AS platform,
    count( IF ( page_code = 'image_search_result' AND list_type = '/image_search_recommend', device_id, NULL ) ) AS goods_clk_cnt,
    count( DISTINCT ( IF ( page_code = 'image_search_result' AND list_type = '/image_search_recommend', device_id, NULL ) ) ) AS goods_clk_uv
FROM
    dwd.dwd_vova_log_goods_click gc
WHERE
    gc.pt = '${pre_date}'
    AND os_type IN ( 'ios', 'android' )
    AND platform = 'mob'
    AND (app_version >= '2.99.0' or app_version >= '2.100.0' and app_version <= '2.199.0')
GROUP BY
    app_version,
    os_type WITH cube
    ),
    tmp_add_cat (
SELECT
    nvl ( t1.platform, 'all' ) AS platform,
    nvl ( current_app_version, 'all' ) AS app_version,
    count( DISTINCT t1.buyer_id ) add_cat_uv
FROM
    dwd.dwd_vova_fact_cart_cause_v2 t1
    LEFT JOIN dim.dim_vova_buyers db ON t1.buyer_id = db.buyer_id
WHERE
    pt = '${pre_date}'
    AND (db.current_app_version >= '2.99.0' or db.current_app_version >= '2.100.0' and db.current_app_version <= '2.199.0')
    AND pre_page_code = 'image_search_result'
    AND pre_list_type = '/image_search_recommend'
GROUP BY
    t1.platform,
    db.current_app_version WITH cube
    ),
    tmp_order (
SELECT
    nvl ( t1.platform, 'all' ) AS platform,
    nvl ( current_app_version, 'all' ) AS app_version,
    count( DISTINCT t1.buyer_id ) order_uv,
    nvl ( sum( fp.shop_price * fp.goods_number + fp.shipping_fee ), 0 ) AS gmv
FROM
    dwd.dwd_vova_fact_order_cause_v2 t1
    INNER JOIN dim.dim_vova_buyers db ON t1.buyer_id = db.buyer_id
    LEFT JOIN dwd.dwd_vova_fact_pay fp ON t1.order_goods_id = fp.order_goods_id
WHERE
    pt = '${pre_date}'
    AND (db.current_app_version >= '2.99.0' or db.current_app_version >= '2.100.0' and db.current_app_version <= '2.199.0')
    AND pre_page_code = 'image_search_result'
    AND pre_list_type = '/image_search_recommend'
GROUP BY
    t1.platform,
    db.current_app_version WITH cube
    )

INSERT overwrite TABLE dwb.dwb_vova_take_photo_for_buy PARTITION ( pt = '${pre_date}' )
SELECT
    platform,
    app_version,
    sum( in_button_exp_uv ) AS in_button_exp_uv,
    sum( reg_zip_photo_exp_cnt ) AS reg_zip_photo_exp_cnt,
    sum( sv_page_uv ) AS sv_page_uv,
    sum( sv_page_result_pv ) AS sv_page_result_pv,
    sum( button_clk_uv ) AS button_clk_uv,
    sum( in_button_clk_uv ) AS in_button_clk_uv,
    sum( img_clk_uv ) AS img_clk_uv,
    sum( zip_img_clk_uv ) AS zip_img_clk_uv,
    sum( goods_exp_uv ) AS goods_exp_uv,
    sum( goods_exp_result_dis_cnt ) AS goods_exp_result_dis_cnt,
    sum( goods_exp_result_cnt ) AS goods_exp_result_cnt,
    sum( goods_clk_cnt ) AS goods_clk_cnt,
    sum( goods_clk_uv ) AS goods_clk_uv,
    sum( add_cat_uv ) AS add_cat_uv,
    sum( order_uv ) AS order_uv,
    sum( gmv ) AS gmv
FROM
    (
SELECT
    platform,
    app_version,
    in_button_exp_uv,
    reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_imp

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    sv_page_uv,
    sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_sv

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    button_clk_uv,
    in_button_clk_uv,
    img_clk_uv,
    zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_clk

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    goods_exp_uv,
    goods_exp_result_dis_cnt,
    goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_goods_imp

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    goods_clk_cnt,
    goods_clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_goods_clk

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    add_cat_uv,
    0 AS order_uv,
    0 AS gmv
FROM
    tmp_add_cat

UNION ALL
SELECT
    platform,
    app_version,
    0 AS in_button_exp_uv,
    0 AS reg_zip_photo_exp_cnt,
    0 AS sv_page_uv,
    0 AS sv_page_result_pv,
    0 AS button_clk_uv,
    0 AS in_button_clk_uv,
    0 AS img_clk_uv,
    0 AS zip_img_clk_uv,
    0 AS goods_exp_uv,
    0 AS goods_exp_result_dis_cnt,
    0 AS goods_exp_result_cnt,
    0 AS goods_clk_cnt,
    0 AS goods_clk_uv,
    0 AS add_cat_uv,
    order_uv,
    gmv
FROM
    tmp_order
    )
GROUP BY
    platform,
    app_version
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_take_photo_for_buy" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi