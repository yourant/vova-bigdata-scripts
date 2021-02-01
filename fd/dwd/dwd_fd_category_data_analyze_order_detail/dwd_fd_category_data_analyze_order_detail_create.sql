CREATE  TABLE IF NOT EXISTS dwd.dwd_fd_category_data_analyze_order_detail
(
    order_id string comment '订单id',
    goods_id string comment '商品id',
    cat_id string comment '品类id',
    category_name string comment '品类名称',
    goods_number  string comment '商品数量',
    shop_price  string comment '商品价格',
    project string comment '组织',
    country  string comment '国家'
) COMMENT '品类数据分析表数据明细表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS parquet;
