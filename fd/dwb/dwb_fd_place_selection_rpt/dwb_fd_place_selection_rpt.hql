with
tmp_ps as
(
    select
        lower(project_name) as project_name,
        goods_id,
        virtual_goods_id,
        country_code,
        cat_name,
        platform,
        impressions,
        sales_order,
        clicks,
        users,
        (clicks/impressions) * 100                           as ctr,
        (clicks / impressions) * (sales_order / users) * 10000 as cr,
        (detail_add_cart / users) *100 as add_rate,
        (checkout /users)*100 as KR,
        (sales_order / users)*100 rate
    from dwd.dwd_fd_place_selection_detail
)


insert overwrite table dwb.dwb_fd_place_selection_rpt
select
    /*+ REPARTITION(1) */
    'sales',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(

    select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    FROM
    (
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where  project_name='floryday'
        and impressions > 1000
        and sales_order >= 100
        and users > 10
        and cr >10
        and ctr>1
        and users > clicks
    )t1
    where rn_cr<=4

)t2
where rn_cr_cr<=8

union ALL

SELECT
    'potential',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where  project_name='floryday'
        and impressions > 1000
        and (sales_order > 2 and sales_order<100)
        and users > 10
        and cr >10
        and ctr>1
        and users > clicks

)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union ALL

SELECT
    'cart',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where   project_name='floryday'
        and impressions > 1000
        and sales_order >= 2
        and users > 100
        and add_rate>25
        and users > clicks

)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union ALL

SELECT
    'KR',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where  project_name='floryday'
        and impressions > 1000
        and sales_order >= 2
        and users > 100
        and KR>15
        and users > clicks

)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union ALL

SELECT
    'rate',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by rate desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by rate desc) rn_cr
        from tmp_ps
        where  project_name='floryday'
        and impressions > 10000
        and clicks>0
        and sales_order >= 2
        and users>9
        and rate>5
        and users > clicks

)t1
where rn_cr<=4
)t2

where rn_cr_cr<=8

union all

SELECT
    'sales',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where project_name='airydress'
        and impressions > 500
        and sales_order >= 20
        and users > 5
        and cr>5
        and ctr>1
        and users > clicks
)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union all

SELECT
    'potential',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where project_name='airydress'
        and impressions > 500
        and (sales_order >1 and sales_order<20)
        and users > 5
        and cr>5
        and ctr>1
        and users > clicks
)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union all

SELECT
    'cart',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where project_name='airydress'
        and impressions > 1000
        and sales_order >= 2
        and users > 100
        and add_rate>25
        and users > clicks
)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union all

SELECT
    'KR',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by cr desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where project_name='airydress'
        and impressions > 1000
        and sales_order >= 2
        and users > 100
        and KR>15
        and users > clicks
)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union all

SELECT
    'rate',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    rn_cr_cr
from
(
   select
            *,
            row_number() over(partition by cat_name,platform order by rate desc) rn_cr_cr
    from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by rate desc) rn_cr
        from tmp_ps
        where project_name='airydress'
        and impressions > 10000
        and clicks>0
        and sales_order >= 2
        and users > 9
        and rate>5
        and users > clicks
)t1
where rn_cr<=4
)t2
where rn_cr_cr<=8

union ALL

SELECT
    'potential',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    rn_cr,
    0
from
(
        SELECT
            *,
            row_number() over(partition by country_code,cat_name,platform order by cr desc) rn_cr
        from tmp_ps
        where project_name='tendaisy'
        and impressions > 100
        and sales_order >= 4
        and users >0
        and cr>0
        and users > clicks
)t1
where rn_cr<=4


union all

SELECT
    'potential',
    project_name,
    goods_id,
    virtual_goods_id,
    country_code,
    cat_name,
    platform,
    impressions,
    sales_order,
    clicks,
    users,
    ctr,
    cr,
    0,
    0
from
(
        SELECT
            *
        from tmp_ps
        where project_name='tendaisy'
        and sales_order >=10
)t1;