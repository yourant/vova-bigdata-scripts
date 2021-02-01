
insert overwrite table dwd.dwd_fd_category_data_analyze_order_detail partition (pt='${pt}')
select
     t1.order_id,
     t3.goods_id ,
     t4.cat_id,
     t4.cat_name,
     t3.goods_number ,
     t3.shop_price ,
     t1.project_name,
     if(t2.region_code in ('DE','FR','GB','PL','MX','US','IT','SE','ES','BR','CZ','NL','CL','AU','RU','AT','CO','DK','NO','CH','SK','IL','FL','SA') ,t2.region_code ,'others') as country
from ods_fd_vb.ods_fd_order_info t1
left join ods_fd_vb.ods_fd_order_goods t3
on t1.order_id=t3.order_id
left join dim.dim_fd_region t2
on t2.region_id =t1.country
left join (select goods_id,max(cat_id) as cat_id,max(cat_name) as cat_name  from dim.dim_fd_goods group by goods_id) t4
on t3.goods_id=t4.goods_id
where t1.pay_status=2
and t1.order_time > '${pt}' and t1.order_time < date_sub('${pt}',-1);
