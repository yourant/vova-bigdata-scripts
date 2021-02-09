-- 知识图谱商品
CREATE external TABLE `ads.ads_vova_goods_knowledge_graph`(
  `goods_id`      bigint COMMENT 'd_商品id',
  `goods_name`    string COMMENT 'i_商品名称',
  `goods_desc`    string COMMENT 'i_商品描述',
  `goods_sn`      string COMMENT 'i_商品sn',
  `second_cat_id` bigint COMMENT 'i_二级品类id',
  `brand_id`      bigint COMMENT 'i_brand id',
  `is_on_sale`    int    COMMENT 'i_是否在售',
  `is_delete`     int    COMMENT 'i_是否删除'
)
COMMENT '知识图谱商品'
PARTITIONED BY ( `pt` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "|",
"escapeChar" = "\\",
"LOCATION"="s3://vova-chatbot/kg/origin_data/ads_vova_goods_knowledge_graph"
)
stored as textfile;