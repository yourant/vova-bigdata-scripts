
CREATE EXTERNAL TABLE IF NOT EXISTS MLB.MLB_VOVA_SEARCH_EXPANSION_A_D
(
   query           STRING COMMENT '原始搜索query',
   expansion       STRING COMMENT '拓展query',
   score           double COMMENT '得分'
)COMMENT '搜索词拓展'
PARTITIONED BY (PT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS textfile
LOCATION "s3://vova-mlb/REC/data/search/expansion/mlb_vova_search_expansion_a_d"


CREATE TABLE `mlb_vova_search_expansion_a_d` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `query` varchar(128) NOT NULL COMMENT '原始搜索query',
  `expansion` varchar(128) DEFAULT NULL COMMENT '拓展query',
  `score` double(11,4) DEFAULT NULL COMMENT '得分',
  `last_update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '日期',
  PRIMARY KEY (`id`),
  KEY `ix_query` (`query`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词拓展表';