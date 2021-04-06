with goods_click_impression as (
    select vg.goods_id                                    as goods_id,
           sum(if(event_name = "goods_click", 1, 0))      as click_num,
           sum(if(event_name = "goods_impression", 1, 0)) as impression_num
    from ods_fd_snowplow.ods_fd_snowplow_goods_event ge
             left join ods_fd_vb.ods_fd_virtual_goods vg on ge.goods_event_struct.virtual_goods_id = vg.virtual_goods_id
    where pt BETWEEN date_sub('${pt}', 30) AND date_sub('${pt}', 1)
      and project in ('floryday','airydress')
    group by goods_id),


     goods_sales as (
         select goods_id,
                sum(goods_number) as sales_num
         from dwd.dwd_fd_order_goods
         where project_name in ('floryday','airydress')
           and date(from_unixtime(order_time)) BETWEEN date_sub('${pt}', 30) AND date_sub('${pt}', 1)
         group by goods_id
     )
insert overwrite table ads.ads_fd_goods_performance_30d partition (pt = "${pt}")
select
/*+ REPARTITION(3) */
       goods_id                   as goods_id,
       nvl(gci.click_num, 0)      as click,
       nvl(gci.impression_num, 0) as impression,
       nvl(gs.sales_num, 0)       as sales
from goods_click_impression gci
         full outer join goods_sales gs using (goods_id);