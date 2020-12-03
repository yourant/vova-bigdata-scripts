
insert overwrite table dwb.dwb_fd_last_7_days_goods_rpt
SELECT 
    to_date(from_utc_timestamp('${pt}','America/Los_Angeles')),
    oi.project_name,
    og.goods_id,
    sum(og.goods_number) as goods_num
FROM ods_fd_vb.ods_fd_order_info oi left join ods_fd_vb.ods_fd_order_goods og on oi.order_id=og.order_id
     left join ods_fd_vb.ods_fd_goods_project gp on og.goods_id=gp.goods_id
where
    oi.email  NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
     and to_date(oi.order_time)>= date_add(to_date(from_utc_timestamp('${pt}','America/Los_Angeles')),-7)
     and to_date(oi.order_time) < to_date(from_utc_timestamp('${pt}','America/Los_Angeles'))
     and pay_status>=1
     and gp.is_on_sale=0
     group by oi.project_name,og.goods_id
     having sum(og.goods_number)>=35;
