CREATE TABLE IF NOT EXISTS dim.dim_fd_goods
(
    project_name     string comment '数据平台',
    goods_id         bigint COMMENT '商品ID',
    virtual_goods_id bigint COMMENT '商品虚拟ID',
    cp_goods_id      bigint COMMENT '克隆商品ID',
    brand_id         bigint COMMENT '侵权商品',
    goods_sn         string COMMENT '商品所属sn',
    goods_name       string COMMENT '商品名称',
    goods_desc       string COMMENT '商品描述',
    keywords         string COMMENT '关键词',
    add_time         timestamp COMMENT '添加时间',
    is_complete      bigint comment '编辑是否完成',
    is_new           bigint COMMENT '是否是新品',
    cat_id           bigint COMMENT '商品类目ID',
    cat_name         string COMMENT '商品类目名',
    first_cat_id     bigint COMMENT '商品一级类目',
    first_cat_name   string COMMENT '商品一级类目',
    second_cat_id    bigint COMMENT '商品二级类目',
    second_cat_name  string COMMENT '商品二级类目',
    third_cat_id     bigint COMMENT '商品三级类目',
    third_cat_name   string COMMENT '商品三级类目',
    shop_price       DECIMAL(15, 4) comment '商品价格',
    goods_weight     DECIMAL(15, 4) comment '商品重量',
    goods_selector   string  comment '商品选款人'
) COMMENT '商品维度'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

