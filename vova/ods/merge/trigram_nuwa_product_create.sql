DROP TABLE ods_gyl_gnw.ods_gyl_product;
CREATE TABLE ods_gyl_gnw.ods_gyl_product(
  product_id bigint COMMENT '商品唯一ID(版本)',
  commodity_id string COMMENT '同一商品唯一id(集合),md5(origin_uri),业务唯一主键',
  cat_id bigint COMMENT '分类ID'
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;