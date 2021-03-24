CREATE TABLE if not exists `dwd.dwd_fd_goods_test_goods_detail`(
  `goods_id` bigint COMMENT '商品id',
  `goods_name` string COMMENT '商品名',
  `virtual_goods_id` bigint COMMENT '商品虚拟id',
  `cat_id` bigint COMMENT '商品类目ID',
  `cat_name` string COMMENT '商品类目名',
  `goods_selector` string COMMENT '选款人',
  `selection_mode` string COMMENT '选款方式',
  `selection_channel` string COMMENT '选款渠道',
  `channel_type` string COMMENT '渠道类型',
  `project` string COMMENT '组织',
  `result` bigint COMMENT '测款结果',
  `reason` string COMMENT '测款原因',
  `type_name` string COMMENT '商品类型：贸综/正常',
  `create_time` timestamp COMMENT '创建时间',
  `test_time` timestamp COMMENT '入测时间',
  `end_time` timestamp COMMENT '结束测试时间',
  `last_update_time` timestamp COMMENT '最后更新时间'
 )comment '测款商品明细表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");