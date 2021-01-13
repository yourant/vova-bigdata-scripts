drop table dim.dim_vova_category;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_category(
  cat_id bigint COMMENT 'pk ID',
  cat_name string COMMENT '分类名',
  depth bigint COMMENT '',
  first_cat_id bigint COMMENT '一级分类id',
  first_cat_name string COMMENT '一级分类名称',
  second_cat_id bigint COMMENT '二级分类id',
  second_cat_name string COMMENT '二级分类名称',
  three_cat_id bigint COMMENT '三级分类id',
  three_cat_name string COMMENT '三级分类名称',
  four_cat_id bigint COMMENT '四级分类id',
  four_cat_name string COMMENT '四级分类名称',
  is_leaf bigint COMMENT '是否子节点')
COMMENT '商品分类'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE

