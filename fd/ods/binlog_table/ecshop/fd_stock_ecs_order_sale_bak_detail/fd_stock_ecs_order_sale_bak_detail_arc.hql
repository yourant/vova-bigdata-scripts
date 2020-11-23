CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc (
    id bigint comment '自增id',
	bak_id bigint comment '',
	bak_order_date bigint comment '',
	external_goods_id bigint comment '',
	on_sale_time bigint comment '',
	7d_sale decimal(15, 4) comment '',
	14d_sale decimal(15, 4) comment '',
	28d_sale decimal(15, 4) comment '',
	uniq_sku string comment ''
) comment '同步的近14天日销数据表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc PARTITION (pt = '${hiveconf:pt}')
select 
     id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
from (

    select 
        pt,id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  pt
                id,
                bak_id,
                bak_order_date,
                external_goods_id,
                on_sale_time,
                7d_sale,
                14d_sale,
                28d_sale,
                uniq_sku
        from ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc where pt = '${hiveconf:pt_last}'

        UNION

        select pt,id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
        from (

            select  pt
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
            from ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_inc where pt='${hiveconf:pt}'

        ) inc where inc.rank = 1
    )  arc 
) tab where tab.rank = 1;
