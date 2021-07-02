create table if not exists dwb.dwb_fd_goods_structure_analysis
(
    project_name            STRING COMMENT '组织',
	cat_name                STRING COMMENT '品类名',
	provider_attribute_name STRING COMMENT '商品供应商',
	goods_style		        STRING COMMENT '商品款式，新款、老款',

    on_sale_goods_num      BIGINT COMMENT '在架商品数',
    dynamic_goods_num	   BIGINT COMMENT '动销商品数',
    up_sale_gooods         BIGINT COMMENT '上架商品数',
    down_sale_gooods	   BIGINT COMMENT '下架商品数',

    goods_sale_100		   BIGINT COMMENT '日销100件商品数',
    goods_sale_50		   BIGINT COMMENT '日销50件商品数',
    goods_sale_10          BIGINT COMMENT '日销10件商品数',
    sale_good_goods		   BIGINT COMMENT '畅销的商品数',
    goods_sale_2		   BIGINT COMMENT '日销2件商品数',
    goods_sale_0           BIGINT COMMENT '日销0-2件商品数'
) comment "商品结构分析表"
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;