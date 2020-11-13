CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail
select `(dt)?+.+` from ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc where dt = '${hiveconf:dt}';
