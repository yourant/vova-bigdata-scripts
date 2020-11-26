CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_14d_avg_sale (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`14d_avg_sale` decimal(10, 6) COMMENT '最近14天每天平均销量'
) COMMENT 'erp最近14天每天平均销量'
PARTITIONED BY(dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS PARQUETFILE
LOCATION 's3a://artemis-data-lake/floryday/middle/fd_mid_erp_14d_avg_sale'
TBLPROPERTIES ("parquet.compress"="SNAPPY");


CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_goods_sale_monthly (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`goods_number_month` bigint COMMENT '未预定上的订单需求数'
) COMMENT 'erp可预订库存'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_goods_sku (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku'
) COMMENT 'erp商品id和SKU'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_goods_stock (
`goods_id` bigint COMMENT '商品id',
`stock_days` decimal(10, 2) COMMENT '备货时间'
) COMMENT 'erp备货天数指标数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_reserve_goods (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`reserve_num` bigint COMMENT '未预定上的订单需求数'
) COMMENT 'erp可预订库存'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_unreserve_order (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`goods_number` bigint COMMENT '未预定上的订单需求数'
) COMMENT 'erp未预定上的订单需求数'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


CREATE TABLE IF NOT EXISTS  dwb.dwb_fd_erp_unsale_detail (
`unsale_level` string COMMENT '滞销程度',
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品SKU',
`stock_days` decimal(10, 2) COMMENT '备货天数',
`14d_avg_sale` decimal(10, 6) COMMENT '最近14天每天平均销量',
`goods_number` bigint COMMENT '未预定上的订单需求数',
`reserve_num` bigint COMMENT '可预订库存',
`goods_number_month` bigint COMMENT '当月销量',
`can_sale_days` decimal(10, 2) COMMENT '可售天数',
`back_days` decimal(10, 2) COMMENT 'max(30,备货天数)',
`unsale_goods_num` decimal(10, 2) COMMENT 'max(30,备货天数)'
) COMMENT 'romeo组织的配置表'
partitioned by (`dt` string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as orc;

CREATE EXTERNAL TABLE IF NOT EXISTS  dwd.dwd_fd_erp_unsale_rpt (
`unsale_level` string COMMENT '滞销程度',
`unsale_rate` decimal(10, 6) COMMENT '滞销率',
`unsale_goods_num` bigint COMMENT '滞销件数',
`goods_number_total` bigint COMMENT '月销量',
`ws_goods_number_rate` decimal(10, 6) COMMENT '库存变化率'
) COMMENT 'erp滞销报表指标汇总'
partitioned by (`dt` string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as orc;