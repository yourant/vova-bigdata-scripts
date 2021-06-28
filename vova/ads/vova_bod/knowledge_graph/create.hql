【数据】[9922] 榜单逻辑推荐优化
产品文档见：https://confluence.gitvv.com/pages/viewpage.action?pageId=21272665

CREATE EXTERNAL TABLE `ads`.`ads_vova_knowledge_graph_bod_goods_rank_data`(
`bod_id` BIGINT COMMENT '榜单id',
`goods_id` BIGINT COMMENT '商品id',
`rank` BIGINT COMMENT '商品评分排名'
) COMMENT '知识图谱-榜单商品评分排名统计' PARTITIONED BY (`pt` STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS `rec_recall`.`ads_vova_knowledge_graph_bod_goods_rank_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bod_id` int(11) NOT NULL COMMENT '榜单id',
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `rank` int(11) NOT NULL COMMENT '商品评分排名',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `bod_id` (`bod_id`),
  KEY `goods_id` (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱榜单商品评分排名统计';