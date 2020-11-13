CREATE TABLE if not EXISTS dwb.dwb_fd_last_7_days_goods_report(
    dt string comment'统计时间',
    project_name string comment'组织名称',
    goods_id int comment'商品ID',
    goods_num int comment'近7天销售件数'
)comment'最近7天销售件数大于35件且下架商品的明细表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


insert overwrite table dwb.dwb_fd_last_7_days_goods_report
SELECT 
    from_utc_timestamp(1000*unix_timestamp('${hiveconf:dt}'),'America/Los_Angeles'),
    oi.project_name,
    og.goods_id,
    sum(og.goods_number) as goods_num
FROM ods_fd_vb.ods_fd_order_info oi left join ods_fd_vb.ods_fd_order_goods og on oi.order_id=og.order_id
     left join ods_fd_vb.ods_fd_goods_project gp on og.goods_id=gp.goods_id
where (            oi.email not like '%%@tetx.com%%'
                 and oi.email not like '%%@i9i8.com%%'
                 and oi.email not like '%%@qq.com%%'
                 and oi.email not like '%%@163.com%%'
                 and oi.email not like '%%@jjshouse.com%%'
                 and oi.email not like '%%@jenjenhouse.com%%'
    )and from_unixtime(oi.order_time)>= date_add(from_utc_timestamp(1000*unix_timestamp('${hiveconf:dt}'),'America/Los_Angeles'),-7)
     and from_unixtime(oi.order_time) < from_utc_timestamp(1000*unix_timestamp('${hiveconf:dt}'),'America/Los_Angeles')
     and pay_status>=1
     and gp.is_on_sale=0
     group by oi.project_name,og.goods_id
     having sum(og.goods_number)>=35;
