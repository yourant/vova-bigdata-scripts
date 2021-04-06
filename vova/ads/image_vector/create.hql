drop table ads.ads_vova_image_vector_source;
CREATE external  TABLE IF NOT EXISTS ads.ads_vova_image_vector_source
(
    vector_id                   bigint COMMENT '商品id',
    img_id                      bigint COMMENT '图片id',
    goods_id                    bigint COMMENT '商品id',
    img_url                     string COMMENT '图片链接',
    class_id                    bigint COMMENT 'class_id',
    box_point                   string COMMENT '检测框',
    vector_base64               string COMMENT '图片向量',
    sku_id                      bigint COMMENT 'sku id',
    cat_id                      bigint COMMENT '品类id',
    first_cat_id                bigint COMMENT '一级品类id',
    second_cat_id               bigint COMMENT '二级品类id',
    brand_id                    bigint COMMENT '品牌id'
) COMMENT '图片向量原始数据'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' location 's3://vova-computer-vision/product_data/vova_image_retrieval/image_vector/';



drop table ads.ads_vova_image_vector_target_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_image_vector_target_d
(
    vector_id                   bigint COMMENT '商品id',
    img_id                      bigint COMMENT '图片id',
    goods_id                    bigint COMMENT '商品id',
    class_id                    bigint COMMENT 'class_id',
    img_url                     string COMMENT '图片链接',
    vector_base64               string COMMENT 'base64向量结果',
    event_date                  string COMMENT '日期',
    sku_id                      bigint COMMENT 'sku id',
    cat_id                      bigint COMMENT '品类id',
    first_cat_id                bigint COMMENT '一级品类id',
    second_cat_id               bigint COMMENT '二级品类id',
    brand_id                    bigint COMMENT '品牌id',
    is_delete                   bigint COMMENT '向量是否删除',
    is_on_sale                  bigint COMMENT '商品是否在架',
    is_update                   bigint COMMENT '是否更新'
) COMMENT '图片向量原始数据'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_image_vector_target_his;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_image_vector_target_his
(
    vector_id                   bigint COMMENT '商品id',
    img_id                      bigint COMMENT '图片id',
    goods_id                    bigint COMMENT '商品id',
    class_id                    bigint COMMENT 'class_id',
    event_date                  string COMMENT '日期',
    is_delete                   bigint COMMENT '向量是否删除',
    is_on_sale                  bigint COMMENT '商品是否在架',
    is_update                  bigint COMMENT '是否更新'
) COMMENT '图片向量历史累计每日状态快照'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




CREATE TABLE `ads_image_vector_v3` (
  `vector_id` bigint(11) NOT NULL,
  `img_id` int(11) NOT NULL DEFAULT '0' COMMENT '图片id',
  `goods_id` int(11) NOT NULL DEFAULT '0' COMMENT '商品id',
  `class_id` int(11) NOT NULL DEFAULT '0' COMMENT 'class_id',
  `img_url` varchar(255) NOT NULL DEFAULT '' COMMENT '图片链接',
  `vector_base64` varchar(1400) NOT NULL DEFAULT '' COMMENT '向量',
  `event_date` varchar(10) NOT NULL DEFAULT '' COMMENT '日期',
  `is_delete` int(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
  `is_on_sale` int(4) NOT NULL DEFAULT '1' COMMENT '是否在架',
  `is_update` int(4) NOT NULL DEFAULT '0' COMMENT '是否更新',
  `sku_id` int(11) NOT NULL DEFAULT '0' COMMENT 'sku id',
  `cat_id` int(11) NOT NULL DEFAULT '0' COMMENT '品类id',
  `first_cat_id` int(11) NOT NULL DEFAULT '0' COMMENT '一级品类id',
  `second_cat_id` int(11) NOT NULL DEFAULT '0' COMMENT '二级品类id',
  `brand_id` int(11) NOT NULL DEFAULT '0' COMMENT '品牌id',
  `last_update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`vector_id`),
  KEY `event_date` (`event_date`) USING BTREE,
  KEY `img_id` (`img_id`) USING BTREE,
  KEY `goods_id` (`goods_id`) USING BTREE,
  KEY `class_id` (`class_id`) USING BTREE,
  KEY `is_delete` (`is_delete`) USING BTREE,
  KEY `is_on_sale` (`is_on_sale`) USING BTREE,
  KEY `is_update` (`is_update`) USING BTREE,
  KEY `brand_id` (`brand_id`) USING BTREE,
  KEY `first_cat_id` (`first_cat_id`) USING BTREE,
  KEY `vgi_key`(`vector_id`,`goods_id`,`img_id`) USING BTREE,
  KEY `last_update_time` (`last_update_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
