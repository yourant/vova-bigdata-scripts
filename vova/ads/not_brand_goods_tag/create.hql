需求：【9497 非Brand增加标签】
--https://confluence.gitvv.com/pages/viewpage.action?pageId=21275603
--mysql themis库
CREATE TABLE IF NOT EXISTS `ads_vova_not_brand_goods_tag_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `tag_id` int(4) NOT NULL COMMENT '标签id',
  `weight` int(4) NOT NULL COMMENT '权重',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY goods_id (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='非brand商品标签表';

CREATE TABLE IF NOT EXISTS `ads_vova_tag_name_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tag_id` int(4) NOT NULL COMMENT '标签id',
  `tag_name` VARCHAR(255) NOT NULL COMMENT '标签名称(多语言版本)',
  `language_id` int(4) NOT NULL COMMENT '语言id',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY tag_id (`tag_id`,`language_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='标签名称表';



--hive
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_not_brand_goods_tag_data
(
    goods_id bigint COMMENT '商品id',
    tag_id bigint COMMENT '标签id',
    weight bigint COMMENT '权重'
) COMMENT '非brand商品标签表' PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_tag_name_data
(
    tag_id bigint COMMENT '标签id',
    tag_name string COMMENT '标签名称(多语言版本)',
    language_id bigint COMMENT '语言id'
) COMMENT '标签名称表' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;