SET hive.exec.compress.output=true;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_product_add_session')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'add' and page_code = 'product'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_product_add_session')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'add' and page_code = 'product'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;


insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_click')
     SELECT
     /*+ REPARTITION(10) */
         goods_id,
         cat_id,
         country,
         project,
         platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_druid_goods_event_temp
     WHERE record_type = 'click' and list_type in ('list-category','list-pre-order')
     GROUP BY
         project,
         goods_id,
         cat_id,
         country,
         platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_click')
     SELECT
     /*+ REPARTITION(10) */
         goods_id,
         cat_id,
         country,
         project,
         'all' as platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_druid_goods_event_temp
     WHERE record_type = 'click' and list_type in ('list-category','list-pre-order')
     GROUP BY
         project,
         goods_id,
         cat_id,
         country;


insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_impression')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'impression' and list_type in ('list-category','list-pre-order')
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_impression')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'impression' and list_type in ('list-category','list-pre-order')
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;


insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_orders')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        sum(paying_order_num) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    GROUP BY
        goods_id,
        project,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_orders')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        sum(paying_order_num) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    GROUP BY
        goods_id,
        project,
        cat_id,
        country;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_add_session')
     SELECT
     /*+ REPARTITION(10) */
         goods_id,
         cat_id,
         country,
         project,
         platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_druid_goods_event_temp
     WHERE record_type = 'add'
     GROUP BY
         project,
         goods_id,
         cat_id,
         country,
         platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_add_session')
     SELECT
     /*+ REPARTITION(10) */
         goods_id,
         cat_id,
         country,
         project,
         'all' as platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_druid_goods_event_temp
     WHERE record_type = 'add'
     GROUP BY
         project,
         goods_id,
         cat_id,
         country;


insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'test_users')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'detail_view'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_collect partition (name = 'all_test_users')
    SELECT
    /*+ REPARTITION(10) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_druid_goods_event_temp
    WHERE record_type = 'detail_view'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;


insert overwrite table ads.ads_fd_goods_test_goods_target_20day
select
/*+ REPARTITION(10) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    gi.uv as impressions,
    gc.uv as click,
    gu.uv as users,
    gas.uv as add_session,
    gpas.uv as product_add_session,
    go.uv as orders
from
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id and gi.platform_type = gc.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_users') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id and gi.platform_type = gu.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id and gi.platform_type = gas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id and gi.platform_type = gpas.platform_type
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'test_orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id and gi.platform_type = go.platform_type

union all

select
/*+ REPARTITION(10) */
    gi.goods_id,
    gi.cat_id,
    gi.country,
    gi.project,
    gi.platform_type,
    gi.uv as impressions,
    gc.uv as click,
    gu.uv as users,
    gas.uv as add_session,
    gpas.uv as product_add_session,
    go.uv as orders
from
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_impression') gi
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_click') gc
on gi.goods_id = gc.goods_id and gi.project = gc.project and gi.country = gc.country and gi.cat_id = gc.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_users') gu
on gi.goods_id = gu.goods_id and gi.project = gu.project and gi.country = gu.country and gi.cat_id = gu.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_add_session') gas
on gi.goods_id = gas.goods_id and gi.project = gas.project and gi.country = gas.country and gi.cat_id = gas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_product_add_session') gpas
on gi.goods_id = gpas.goods_id and gi.project = gpas.project and gi.country = gpas.country and gi.cat_id = gpas.cat_id
left join
    (select * from tmp.tmp_goods_some_kpi_collect where name = 'all_test_orders') go
on gi.goods_id = go.goods_id and gi.project = go.project and gi.country = go.country and gi.cat_id = go.cat_id
;
