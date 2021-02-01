
insert overwrite table dwd.dwd_fd_category_data_analyze_order_detail partition (pt='${pt}')
select
     order_id,
     goods_id ,
     cat_id,
     cat_name,
     goods_number ,
     shop_price ,
     project_name,
     if(country_code  in ('DE','FR','GB','PL','MX','US','IT','SE','ES','BR','CZ','NL','CL','AU','RU','AT','CO','DK','NO','CH','SK','IL','FL','SA') ,country_code  ,'others') as country
from dwd.dwd_fd_order_goods t1
where pay_status=2
and order_time > unix_timestamp(concat('${pt}',' 00:00:00')) and order_time < unix_timestamp(concat(date_sub('${pt}',-1),' 00:00:00'));
