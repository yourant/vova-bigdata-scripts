[9616] 场景式榜单优化

https://confluence.gitvv.com/pages/viewpage.action?pageId=21275895

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_scene_bod_original_data
     (
      bod_name string COMMENT '榜单名称',
      bod_separate_word string COMMENT '榜单分词',
      goods_list string COMMENT '商品列表'
     ) COMMENT '场景式榜单原始数据' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
-- 算法提供的场景榜单数据指定位置 s3://bigdata-offline/warehouse/ads/ads_vova_scene_bod_original_data/

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_scene_bod_goods_rank_data
     (
      bod_id bigint COMMENT '榜单id',
      goods_id bigint COMMENT '商品id',
      rank bigint COMMENT '商品排名'
     ) COMMENT '场景式榜单数据' PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE `ads_vova_scene_bod_goods_rank_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bod_id` int(11) NOT NULL COMMENT '榜单id',
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `rank` int(11) NOT NULL COMMENT '商品评分排名',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `bod_id` (`bod_id`),
  KEY `goods_id` (`goods_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1164203 DEFAULT CHARSET=utf8mb4 COMMENT='场景式榜单数据'


