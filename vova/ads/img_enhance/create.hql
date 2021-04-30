drop table ads.ads_vova_img_enhance_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_img_enhance_d
(
    img_id                    string COMMENT '图片id',
    goods_id                  bigint COMMENT '商品id',
    img_url                   string COMMENT '图片链接',
    is_default                bigint COMMENT '是否首图'
) COMMENT '热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE location 's3://vova-computer-vision/product_data/vova_image_enhancement/src_data/';



drop table ads.ads_vova_img_enhance_result_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_img_enhance_result_d
(
    goods_id                  bigint COMMENT '商品id',
    img_id                    bigint COMMENT '图片id',
    img_url_aws               string COMMENT 'aws图片链接',
    img_url_gcs               string COMMENT 'google图片链接',
    is_default                bigint COMMENT '是否主图'
) COMMENT '热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vova-computer-vision/product_data/vova_image_enhancement/dst_data/';



CREATE TABLE `ads_vova_goods_img_enhance` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `img_id` int(11) NOT NULL COMMENT '图片id',
  `img_url_aws` varchar(255) NOT NULL COMMENT 'aws图片url',
  `img_url_gcs` varchar(255) NOT NULL COMMENT 'gcs图片url',
  `is_default` tinyint(1) NOT NULL COMMENT '是否为主图，‘1’ : 是，‘0’ : 否',
  `last_update_time` datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ux_img_id` (`img_id`) USING BTREE,
  KEY `ix_goods_id` (`goods_id`) USING BTREE,
  KEY `ix_goods_id_default` (`is_default`,`goods_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;