
insert overwrite table dwb.dwb_fd_last_7_days_goods_rpt
SELECT
    project_name,
    goods_id,
    sum(goods_number) as goods_num
FROM (
select
    oi.project_name,
    oi.pay_status,
    og.goods_id,
    og.goods_number
    from (select
                order_id,
                project_name,
                order_time
        from ods_fd_vb.ods_fd_order_info
    where   oi.email  NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
    and to_date(oi.order_time)>= date_add(to_date(from_utc_timestamp('${pt}','America/Los_Angeles')),-6)
                           and to_date(oi.order_time) < to_date(from_utc_timestamp('${pt}','America/Los_Angeles'))
                           and pay_status>=1
         ) oi
    inner join ods_fd_vb.ods_fd_order_goods og
             on  oi.order_id=og.order_id
     left join ods_fd_vb.ods_fd_goods_project gp
     on og.goods_id=gp.goods_id and gp.is_on_sale=0
     )tab1
     group by project_name,goods_id
     having sum(goods_number)>=35;


