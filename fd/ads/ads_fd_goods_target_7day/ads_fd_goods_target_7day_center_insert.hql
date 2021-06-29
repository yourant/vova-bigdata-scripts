SET hive.exec.compress.output=true;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'impression')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'impression' and list_type in ('list-category','list-pre-order')
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_impression')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'impression' and list_type in ('list-category','list-pre-order')
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'click')
     SELECT
     /*+ REPARTITION(20) */
         goods_id,
         cat_id,
         country,
         project,
         platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_goods_event_week
     WHERE record_type = 'click' and list_type in ('list-category','list-pre-order')
     GROUP BY 
         project,
         goods_id,
         cat_id,
         country,
         platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_click')
     SELECT
     /*+ REPARTITION(20) */
         goods_id,
         cat_id,
         country,
         project,
         'all' as platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_goods_event_week
     WHERE record_type = 'click' and list_type in ('list-category','list-pre-order')
     GROUP BY
         project,
         goods_id,
         cat_id,
         country;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'user')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'detail_view'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_user')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'detail_view'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'add_session')
     SELECT
     /*+ REPARTITION(20) */
         goods_id,
         cat_id,
         country,
         project,
         platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_goods_event_week
     WHERE record_type = 'add'
     GROUP BY
         project,
         goods_id,
         cat_id,
         country,
         platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_add_session')
     SELECT
     /*+ REPARTITION(20) */
         goods_id,
         cat_id,
         country,
         project,
         'all' as platform_type,
         COUNT(DISTINCT session_id) as uv
     FROM tmp.tmp_fd_goods_event_week
     WHERE record_type = 'add'
     GROUP BY
         project,
         goods_id,
         cat_id,
         country;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'product_add_session')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'add' and page_code = 'product'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_product_add_session')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        COUNT(DISTINCT session_id) as uv
    FROM tmp.tmp_fd_goods_event_week
    WHERE record_type = 'add' and page_code = 'product'
    GROUP BY
        project,
        goods_id,
        cat_id,
        country;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'orders')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        platform_type,
        sum(paying_order_num) as uv
    FROM tmp.tmp_fd_goods_event_week
    GROUP BY
        goods_id,
        project,
        cat_id,
        country,
        platform_type;

insert overwrite table tmp.tmp_goods_some_kpi_week partition (name = 'all_orders')
    SELECT
    /*+ REPARTITION(20) */
        goods_id,
        cat_id,
        country,
        project,
        'all' as platform_type,
        sum(paying_order_num) as uv
    FROM tmp.tmp_fd_goods_event_week
    GROUP BY
        goods_id,
        project,
        cat_id,
        country;
