SET hive.exec.compress.output=true;

with fd_goods_test_channel_success as (
select
    cat_name,
    selection_channel,
    channel_type,
    count(distinct virtual_goods_id) as number_of_success_products,
    sum(last_7_days_goods_sales) as last_7_days_goods_sales,
    count(distinct if(last_7_days_goods_sales > 0 and round(last_7_days_goods_sales, 4)/last_7_days_cat_sales > 0.01, virtual_goods_id, null)) as number_of_popular_products
from
dwb.dwb_fd_goods_test_channel
where test_result = '1' and end_day >= '${pt_begin}' and end_day <= '${pt_end}'
group by
    cat_name,
    selection_channel,
    channel_type with cube
),

fd_goods_test_channel_all as (
select
    cat_name,
    selection_channel,
    channel_type,
    count(distinct virtual_goods_id) as number_of_end_products
from
    dwb.dwb_fd_goods_test_channel
where end_day >= '${pt_begin}' and end_day <= '${pt_end}'
group by
    cat_name,
    selection_channel,
    channel_type with cube
)

insert overwrite table ads.ads_fd_goods_test_channel
select
distinct *
from
(
select
    fs.cat_name,
    fs.selection_channel,
    fs.channel_type,
    fs.number_of_success_products,
    fa.number_of_end_products,
    cast((fs.number_of_success_products/fa.number_of_end_products) as decimal(15,4)) as success_rate,
    fs.last_7_days_goods_sales,
    cast(fs.last_7_days_goods_sales/fa.number_of_end_products as decimal(15,4)) as channel_contribution,
    fs.number_of_popular_products
from
    fd_goods_test_channel_success fs
left join
    fd_goods_test_channel_all fa
on fs.cat_name = fa.cat_name
and fs.selection_channel = fa.selection_channel
and fs.channel_type = fa.channel_type
)
where cat_name is not null and selection_channel is not null and channel_type is not null
;





