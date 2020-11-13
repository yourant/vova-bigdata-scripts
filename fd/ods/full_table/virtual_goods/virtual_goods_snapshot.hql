CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_virtual_goods(
  `virtual_goods_id` bigint COMMENT '虚拟商品id',
  `goods_id` bigint COMMENT '商品id',
  `project_name` string COMMENT '组织'
 )comment '虚拟商品id 商品id'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table ods_fd_vb.ods_fd_virtual_goods
select  
    virtual_goods_id,
    goods_id,
    project_name
from ods_fd_vb.ods_fd_virtual_goods_arc
where dt = '${hiveconf:dt}';
