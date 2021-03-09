drop table dim.dim_goods_sku;
CREATE external TABLE IF NOT EXISTS dim.dim_vova_goods_sku
(
    datasource       string comment '数据平台',
    sku_id           BIGINT COMMENT '商品ID',
    sku              string COMMENT 'sku',
    goods_id         BIGINT COMMENT '商品ID',
    is_delete        BIGINT COMMENT '是否删除,1:已删除，0：未删除',
    img_id           BIGINT COMMENT '图片id',
    img_color        string COMMENT '图片颜色',
    color_is_show    BIGINT COMMENT '图片颜色是否展示,1展示，0不展示',
    shop_price       DECIMAL(14, 4) comment 'sku价格',
    shipping_fee     DECIMAL(14, 4) comment 'sku运费',
    goods_weight     DECIMAL(14, 4) comment 'sku重量',
    sale_status      string comment '销售状态',
    create_time      timestamp COMMENT '添加时间'
) COMMENT 'sku维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
