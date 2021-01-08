CREATE  TABLE if not exists dwd.dwd_fd_goods_project_extension
(
  `id` bigint COMMENT '主键',
  `goods_id` bigint COMMENT '商品ID',
  `project_name` string COMMENT '组织',
  `ext_name` string COMMENT '扩展名称',
  `ext_value` string COMMENT '扩展值',
  `created` timestamp COMMENT '创建时间',
  `modified` timestamp COMMENT '修改时间')
 COMMENT '商品组织扩展表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS parquet
    TBLPROPERTIES ("parquet.compress"="SNAPPY");