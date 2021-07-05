【数据】[9922] 榜单逻辑推荐优化
产品文档见：https://confluence.gitvv.com/pages/viewpage.action?pageId=21272665

CREATE EXTERNAL TABLE `ads`.`ads_vova_bod_heat_rank`(
`bod_id` BIGINT COMMENT '榜单id',
`rank` BIGINT COMMENT '榜单热度排名'
) COMMENT '榜单热度排名(知识图谱榜单&场景式榜单)' PARTITIONED BY (`pt` STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS `rec_recall`.`ads_vova_bod_heat_rank` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`bod_id` int(11) NOT NULL COMMENT '榜单id',
`rank` int(11) NOT NULL COMMENT '榜单热度排名',
`update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`id`) USING BTREE,
KEY `bod_id` (`bod_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='榜单热度排名(知识图谱榜单&场景式榜单)';