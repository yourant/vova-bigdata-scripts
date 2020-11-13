CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_purchase_price(
  `id` bigint COMMENT 'id',
  `goods_id` bigint COMMENT '商品id',
  `purchase_price` decimal(10,2) COMMENT '商品采购价',
  `verify_purchase_price` decimal(10,2) COMMENT '',
  `created` string COMMENT '创建时间',
  `modified` string COMMENT '修改时间'
 )comment '申请调价核价操作历史表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_purchase_price
select `(dt)?+.+` from ods_fd_vb.ods_fd_goods_purchase_price_arc where dt = '${hiveconf:dt}';
