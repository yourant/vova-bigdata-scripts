CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_finished_goods_test_info`(
           goods_id bigint COMMENT '商品id',
           virtual_goods_id bigint COMMENT '商品虚拟ID',
           pipeline_id  bigint COMMENT '线程id',
           project string comment '数据平台',
           platform string comment '平台',
           country  string comment '国家',
           cat_id bigint COMMENT '商品类目ID',
           cat_name string COMMENT '商品类目名',
           state bigint COMMENT '状态',
           type_id bigint COMMENT '类型id',
           result bigint COMMENT '测款结果',
           reason string COMMENT '原因',
           production_reached bigint COMMENT '产品到达',
           goods_type string COMMENT '商品类型',
           goods_source string COMMENT '商品来源',
           test_count bigint COMMENT '测试次数',
           create_time string COMMENT '创建时间,UTC时间',
           last_update_time string COMMENT '最后更新时间,UTC时间'
           )COMMENT '测款商品事实表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");