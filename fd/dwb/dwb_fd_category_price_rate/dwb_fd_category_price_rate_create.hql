CREATE TABLE IF NOT EXISTS dwb.dwb_fd_category_price_rate
(
    project_name string comment '组织',
    first_cat_id bigint comment '一级品类',
    first_cat_name string comment '一级品类名',
    second_cat_id bigint comment '二级品类',
    second_cat_name string comment '二级品类名',
    price_rate bigint comment '价格倍率取整，价格倍率：售价USD*6.7汇率/采购价RMB',
    goods_num bigint comment '商品件数',
    sales_num bigint comment '商品销量',
    total_purchase_cost_rmb decimal(15, 4) comment '总成本：RMB',
    total_sales_volume_usd decimal(15, 4) comment '总销售额：USD',
    adjusted_sales_volume_usd decimal(15, 4) comment '去除0成本的总销售额：USD'
)comment '商品综合倍率表'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;