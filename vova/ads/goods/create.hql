drop table ads.ads_vova_goods_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_d
(
    goods_id         BIGINT COMMENT '商品ID',
    virtual_goods_id BIGINT COMMENT '商品虚拟ID',
    shop_price       DECIMAL(14, 4) comment '商品价格',
    shipping_fee     DECIMAL(14, 4) comment '商品运费',
    cat_id           BIGINT COMMENT '品类ID',
    brand_id         BIGINT COMMENT '品牌ID'
) COMMENT '商品维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE if not exists dim_goods (
  goods_id bigint(20) NOT NULL COMMENT '商品id',
  virtual_goods_id bigint(20) NOT NULL COMMENT '虚拟商品id',
  shop_price decimal(11,4) NOT NULL COMMENT '商品价格',
  shipping_fee decimal(11,4) NOT NULL COMMENT '商品运费',
  cat_id int(11) NOT NULL COMMENT '品类id',
  PRIMARY KEY (`goods_id`),
  KEY virtual_goods_id (virtual_goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='商品维表';