drop table dim.dim_vova_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_goods
(
    datasource       string comment '数据平台',
    goods_id         BIGINT COMMENT '商品ID',
    virtual_goods_id BIGINT COMMENT '商品虚拟ID',
    cp_goods_id      BIGINT COMMENT '克隆商品ID',
    brand_id         BIGINT COMMENT '侵权商品',
    goods_sn         string COMMENT '商品所属sn',
    goods_name       string COMMENT '商品名称',
    goods_desc       string COMMENT '商品描述',
    sale_status      string comment '销售状态',
    keywords         string COMMENT '关键词',
    add_time         timestamp COMMENT '添加时间',
    is_on_sale       BIGINT COMMENT '真实是否在售,1:已上架，0：已下架',
    is_complete      BIGINT comment '编辑是否完成',
    is_new           BIGINT COMMENT '是否是新品',
    cat_id           BIGINT COMMENT '商品类目ID',
    cat_name         string COMMENT '商品类目name',
    first_cat_id     BIGINT COMMENT '商品一级类目',
    first_cat_name   string COMMENT '商品一级类目',
    second_cat_id    BIGINT COMMENT '商品二级类目',
    second_cat_name  string COMMENT '商品二级类目',
    third_cat_id     BIGINT COMMENT '商品三级类目',
    third_cat_name   string COMMENT '商品三级类目',
    mct_id           BIGINT COMMENT '商品所属商家',
    mct_name           string COMMENT '商品所属商家',
    shop_price       DECIMAL(14, 4) comment '商品价格',
    shipping_fee     DECIMAL(14, 4) comment '商品运费',
    goods_weight     DECIMAL(14, 4) comment '商品重量',
    first_on_time    timestamp COMMENT '第一次上线时间',
    first_off_time   timestamp COMMENT '第一次下线时间',
    last_on_time     timestamp COMMENT '最后一次上线时间',
    last_off_time    timestamp COMMENT '最后一次下线时间',
    goods_thumb      string COMMENT '商品主图',
    old_goods_id      string COMMENT '来源平台的ID',
    group_id          BIGINT COMMENT '原始分组id，不是低价商品组的分组id，没有分到组则为-1'
) COMMENT '商品维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




