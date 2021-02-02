
CREATE  TABLE IF NOT EXISTS dwd.dwd_fd_category_data_analyze_goods_event_detail
(
    goods_id string comment '商品id',
    virtual_goods_id string comment '虚拟商品id',
    cat_id string comment '品类id',
    category_name string comment '品类名称',
    project string comment '组织',
    country  string comment '国家',
    event_name  string comment '事件名称',
	platform string comment '平台',
	dvce_type string comment '设备类型',
	os_type string comment '操作系统类型',
    platform_type string comment '平台类型',
    page_code string comment '页面代码',
    mkt_source string comment '广告来源',
    source_type string comment '数据来源PC,APP,H5或者others'
) COMMENT '品类数据分析表商品打点数据明细表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS parquet;
