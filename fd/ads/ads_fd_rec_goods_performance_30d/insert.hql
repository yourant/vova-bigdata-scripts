with goods_click_impression as (
    select vg.goods_id                                    as goods_id,
           goods_event_struct.virtual_goods_id            as virtual_goods_id,
           lower(project_name)                            as project_name,
           sum(if(event_name = "goods_click", 1, 0))      as click_num,
           sum(if(event_name = "goods_impression", 1, 0)) as impression_num
    from ods_fd_snowplow.ods_fd_snowplow_goods_event ge
             left join ods_fd_vb.ods_fd_virtual_goods vg on ge.goods_event_struct.virtual_goods_id = vg.virtual_goods_id
    where pt BETWEEN date_sub('${pt}', 30) AND date_sub('${pt}', 1)
      and project = 'floryday'
    group by goods_id, goods_event_struct.virtual_goods_id, project_name),


     goods_sales as (
         select goods_id,
                virtual_goods_id,
                project_name,
                sum(goods_number) as sales_num
         from dwd.dwd_fd_order_goods
         where project_name = 'floryday'
           and date(from_unixtime(order_time)) BETWEEN date_sub('${pt}', 30) AND date_sub('${pt}', 1)
         group by goods_id, virtual_goods_id, project_name
     )
insert overwrite table ads.ads_fd_goods_performance_30d partition (pt = "${pt}")
/*+ REPARTITION(3) */
select goods_id                   as goods_id,
       virtual_goods_id           as virtual_goods_id,
       project_name               as project_name,
       nvl(gci.click_num, 0)      as click,
       nvl(gci.impression_num, 0) as impression,
       nvl(gs.sales_num, 0)       as sales
from goods_click_impression gci
         full outer join goods_sales gs using (goods_id, virtual_goods_id, project_name);