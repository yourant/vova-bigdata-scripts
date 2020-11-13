CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_purchase_price_arc
(
  `id` bigint COMMENT 'id',
  `goods_id` bigint COMMENT '商品id',
  `purchase_price` decimal(10,2) COMMENT '商品采购价',
  `verify_purchase_price` decimal(10,2) COMMENT '',
  `created` string COMMENT '创建时间',
  `modified` string COMMENT '修改时间'
 )comment '商品采购价'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_goods_purchase_price_arc PARTITION (dt='${hiveconf:dt}')
select  
  id,
  goods_id,
  purchase_price,
  verify_purchase_price,
  created,
  modified
from tmp.tmp_fd_goods_purchase_price_full;
