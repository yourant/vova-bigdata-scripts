SET hive.exec.compress.output=true;

with goods_impression as (
--impression
SELECT
    goods_id,
    project,
    language,
    platform,
    sum(goods_uv) as impressions
FROM tmp.tmp_fd_goods_uv_interval
WHERE record_type = 'impression' and list_type in ('list-category', "list-pre-order") 
GROUP BY
    goods_id,
    project,
    language,
    platform
),

goods_click as (
-- click
    SELECT
        goods_id,
        project,
        language,
        platform,
        sum(goods_uv) as clicks
    FROM tmp.tmp_fd_goods_uv_interval
    WHERE record_type = 'click' and list_type in ('list-category', "list-pre-order") 
    GROUP BY
        goods_id,
        project,
        language,
        platform
),

goods_user as (
-- 详情页访问用户
    SELECT
        goods_id,
        project,
        language,
        platform,
        sum(goods_uv) as users
    FROM tmp.tmp_fd_goods_uv_interval
    WHERE record_type = 'detail_view' 
    GROUP BY
        goods_id,
        project,
        language,
        platform
),

goods_sales_order as (
-- 商品已支付订单数
    SELECT
        goods_id,
        project,
        language,
        platform,
        sum(paid_order_num) as sales_order,
        sum(order_num) as sales
    FROM dwd.dwd_fd_goods_order_interval
    GROUP BY
        goods_id,
        project,
        language,
        platform
),

goods_detail_add_cart as (
--详情页加车
SELECT
    goods_id,
    project,
    language,
    platform,
    sum(goods_uv) as detail_add_cart
FROM tmp.tmp_fd_goods_uv_interval
WHERE record_type = 'add' and page_code = 'product' 
GROUP BY
    goods_id,
    project,
    language,
    platform
),

goods_list_add_cart as (
--列表页加车
SELECT
    goods_id,
    project,
    language,
    platform,
    sum(goods_uv) as list_add_cart
FROM tmp.tmp_fd_goods_uv_interval
WHERE record_type = 'add' and page_code = 'list' 
GROUP BY
    goods_id,
    project,
    language,
    platform
),

goods_checkout as (
--下单
SELECT
    goods_id,
    project,
    language,
    platform,
    sum(goods_uv) as checkout
FROM tmp.tmp_fd_goods_uv_interval
WHERE record_type = 'checkout' 
GROUP BY
    goods_id,
    project,
    language,
    platform
),

fd_tmp_one as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gc.clicks,0) as clicks
from goods_impression gi
full join goods_click gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
),

fd_tmp_two as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gc.users,0) as users
from fd_tmp_one gi
full join goods_user gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
),

fd_tmp_three as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gc.sales_order,0) as sales_order,
    nvl(gc.sales,0) as sales
from fd_tmp_two gi
full join goods_sales_order gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
),

fd_tmp_four as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.sales_order,0) as sales_order,
    nvl(gi.sales,0) as sales,
    nvl(gc.detail_add_cart,0) as detail_add_cart
from fd_tmp_three gi
full join goods_detail_add_cart gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
),

fd_tmp_five as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.sales_order,0) as sales_order,
    nvl(gi.sales,0) as sales,
    nvl(gi.detail_add_cart,0) as detail_add_cart,
    nvl(gc.list_add_cart,0) as list_add_cart
from fd_tmp_four gi
full join goods_list_add_cart gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
),

tmp_fd_goods_display_order_artemis_language as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.language,gc.language) as language,
    nvl(gi.project,gc.project) as project_name,
    nvl(gi.platform,gc.platform) as platform,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.sales_order,0) as sales_order,
    nvl(gi.sales,0) as sales,
    nvl(gi.detail_add_cart,0) as detail_add_cart,
    nvl(gi.list_add_cart,0) as list_add_cart,
    nvl(gc.checkout,0) as checkout,
    0 as virtual_sales_order,
    0 as goods_order,
    cast('${pt_begin} 16:00:00' as timestamp) as start_time,
    cast('${pt_end} 16:00:00' as timestamp) as end_time,
    1 as is_active,
    case when nvl(gi.project,gc.project) in ('tendaisy','sisdress','poprhine','beautlly','trendaisy','herachoice','blessrose','shinynight','jollyweek','merecloth','baltershop','eoschoice','chichut','cherbow','cherlady','cosydress','joycedays','vividpretty')
    and nvl(gi.platform,gc.platform) in ('web','h5') then '1'
    when nvl(gi.project,gc.project) in ('floryday','airydress') and nvl(gi.platform,gc.platform) in ('web','h5', 'mob') then '1'
    else '0' end as mark_code
from fd_tmp_five gi
full join goods_checkout gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.language = gc.language and gi.platform=gc.platform
)


insert overwrite table ads.ads_fd_goods_display_order_artemis_language_interval
select
/*+ REPARTITION(20) */
*
from
(
select
    /*+ REPARTITION(20) */
    doal.goods_id,
    dfl.language_id,
    doal.project_name,
    doal.platform,
    doal.impressions,
    doal.clicks,
    doal.users,
    doal.sales_order,
    doal.detail_add_cart,
    doal.list_add_cart,
    doal.checkout,
    doal.virtual_sales_order,
    doal.goods_order,
    cast('${pt_begin} 16:00:00' as timestamp) as start_time,
    cast('${pt_end} 16:00:00' as timestamp) as end_time,
    cast('${pt}' as int) as interval,
    doal.is_active,
    doal.sales
from
(select * from tmp_fd_goods_display_order_artemis_language where mark_code = '1' and goods_id != 'undefined' and goods_id != 'favorites') doal
left join
dim.dim_fd_language dfl
on
    doal.language = dfl.language_code
)
where language_id is not null
;



