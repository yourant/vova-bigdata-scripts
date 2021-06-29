SET hive.exec.compress.output=true;

with fd_tmp_one as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform_type,gc.platform_type) as platform_type,
    nvl(gi.uv, 0) as impressions,
    nvl(gc.uv,0) as clicks
from (select * from tmp.tmp_goods_some_kpi_collect where name = 'impression') gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.platform_type=gc.platform_type and gi.cat_id = gc.cat_id
),

fd_tmp_two as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform_type,gc.platform_type) as platform_type,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gc.uv,0) as users
from fd_tmp_one gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'user') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.platform_type=gc.platform_type and gi.cat_id = gc.cat_id
),

fd_tmp_three as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform_type,gc.platform_type) as platform_type,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gc.uv,0) as add_session
from fd_tmp_two gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'add_session') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.platform_type=gc.platform_type and gi.cat_id = gc.cat_id
),

fd_tmp_four as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform_type,gc.platform_type) as platform_type,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.add_session,0) as add_session,
    nvl(gc.uv,0) as product_add_session
from fd_tmp_three gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'product_add_session') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.platform_type=gc.platform_type and gi.cat_id = gc.cat_id
),

fd_tmp_five as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.platform_type,gc.platform_type) as platform_type,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.add_session,0) as add_session,
    nvl(gi.product_add_session,0) as product_add_session,
    nvl(gc.uv,0) as orders
from fd_tmp_four gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'orders') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.platform_type=gc.platform_type and gi.cat_id = gc.cat_id
),

fd_tmp_one_all as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.uv, 0) as impressions,
    nvl(gc.uv,0) as clicks
from (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_impression') gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
),

fd_tmp_two_all as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gc.uv,0) as users
from fd_tmp_one_all gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_user') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
),

fd_tmp_three_all as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gc.uv,0) as add_session
from fd_tmp_two_all gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_add_session') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
),

fd_tmp_four_all as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.add_session,0) as add_session,
    nvl(gc.uv,0) as product_add_session
from fd_tmp_three_all gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_product_add_session') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
),

fd_tmp_five_all as (
select
    nvl(gi.goods_id,gc.goods_id) as goods_id,
    nvl(gi.cat_id,gc.cat_id) as cat_id,
    nvl(gi.country,gc.country) as country,
    nvl(gi.project,gc.project) as project,
    'all' as platform_type,
    nvl(gi.impressions, 0) as impressions,
    nvl(gi.clicks,0) as clicks,
    nvl(gi.users,0) as users,
    nvl(gi.add_session,0) as add_session,
    nvl(gi.product_add_session,0) as product_add_session,
    nvl(gc.uv,0) as orders
from fd_tmp_four_all gi
full join (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_orders') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
)

insert overwrite table ads.ads_fd_goods_target_14day
select
/*+ REPARTITION(20) */
*
from
fd_tmp_five

union all

select
/*+ REPARTITION(20) */
*
from
fd_tmp_five_all
;














































SET hive.exec.compress.output=true;

insert overwrite table ads.ads_fd_goods_target_14day
select
/*+ REPARTITION(20) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    nvl(gi.uv,0) as impressions,
    nvl(gc.uv,0) as click,
    nvl(gu.uv,0) as users,
    nvl(gas.uv,0) as add_session,
    nvl(gpas.uv,0) as product_add_session,
    nvl(go.uv,0) as orders
from
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id and gi.platform_type = gc.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'user') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id and gi.platform_type = gu.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id and gi.platform_type = gas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id and gi.platform_type = gpas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id and gi.platform_type = go.platform_type

union all

select
/*+ REPARTITION(20) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    nvl(gi.uv,0) as impressions,
    nvl(gc.uv,0) as click,
    nvl(gu.uv,0) as users,
    nvl(gas.uv,0) as add_session,
    nvl(gpas.uv,0) as product_add_session,
    nvl(go.uv,0) as orders
from
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_user') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id
;






















SET hive.exec.compress.output=true;

insert overwrite table ads.ads_fd_goods_target_7day
select
/*+ REPARTITION(20) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    nvl(gi.uv,0) as impressions,
    nvl(gc.uv,0) as click,
    nvl(gu.uv,0) as users,
    nvl(gas.uv,0) as add_session,
    nvl(gpas.uv,0) as product_add_session,
    nvl(go.uv,0) as orders
from
    (select * from tmp.tmp_goods_some_kpi_week where name = 'impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id and gi.platform_type = gc.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'user') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id and gi.platform_type = gu.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id and gi.platform_type = gas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id and gi.platform_type = gpas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id and gi.platform_type = go.platform_type

union all

select
/*+ REPARTITION(20) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    nvl(gi.uv,0) as impressions,
    nvl(gc.uv,0) as click,
    nvl(gu.uv,0) as users,
    nvl(gas.uv,0) as add_session,
    nvl(gpas.uv,0) as product_add_session,
    nvl(go.uv,0) as orders
from
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_user') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_week where name = 'all_orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id
;