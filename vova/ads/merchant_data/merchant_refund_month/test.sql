tmp_total_order_number as (
select
    event_date,
    mct_id,
    nvl(first_cat_id,-1) as first_cat_id,
    nvl(region_code,'00') as region_code,
    nvl(shipping_type,0) as shipping_type,
    count(order_goods_id) as order_number
from tmp_total_order
        where shipping_type != 4
        group by
            event_date,
            mct_id,
            first_cat_id,
            region_code,
            shipping_type
            grouping sets(
            (event_date,mct_id),
            (event_date,mct_id,first_cat_id,region_code),
            (event_date,mct_id,region_code,shipping_type),
            (event_date,mct_id,first_cat_id,shipping_type),
            (event_date,mct_id,first_cat_id),
            (event_date,mct_id,region_code),
            (event_date,mct_id,shipping_type),
            (event_date,mct_id,first_cat_id,region_code,shipping_type)
            )
)