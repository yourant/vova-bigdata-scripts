CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc (
    id bigint comment '自增id',
	bak_id bigint comment '',
	bak_order_date bigint comment '',
	external_goods_id bigint comment '',
	on_sale_time bigint comment '',
	7d_sale decimal(10,6) comment '',
	14d_sale decimal(10,6) comment '',
	28d_sale decimal(10,6) comment '',
	uniq_sku string comment ''
) comment '同步的近14天日销数据表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
from (

    select 
        dt,id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                id,
                bak_id,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(bak_order_date != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(bak_order_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as bak_order_date,
                external_goods_id,
                /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
                if(on_sale_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(on_sale_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as on_sale_time,
                7d_sale,
                14d_sale,
                28d_sale,
                uniq_sku
        from tmp.tmp_fd_ecs_fd_stock_ecs_order_sale_bak_detail_full
        UNION

        select dt,id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
        from (

            select  dt
                    id,
                    bak_id,
                    bak_order_date,
                    external_goods_id,
                    on_sale_time,
                    7d_sale,
                    14d_sale,
                    28d_sale,
                    uniq_sku,
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    )  arc 
) tab where tab.rank = 1;
