drop table ads.ads_vova_search_words_pool;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_search_words_pool
(
    goods_id               bigint COMMENT '店铺ID',
    goods_sn               string COMMENT '店铺ID',
    query                  string COMMENT '当前时间',
    language               string COMMENT '当前时间',
    rank                   bigint COMMENT '创建时间',
    goods_count              bigint COMMENT '创建时间',
    query_count              bigint COMMENT '创建时间'
) COMMENT '搜索召回池'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_search_words_pool_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_search_words_pool_d
(
    goods_id               bigint COMMENT '商品ID',
    goods_sn               string COMMENT '商品sn',
    query                  string COMMENT '搜索词',
    goods_count            bigint COMMENT '商品数量',
    query_count            bigint COMMENT '搜索次数'
) COMMENT '搜索召回池'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE `ads_search_words_pool` (
  `goods_id` bigint(20)  NOT NULL COMMENT '商品id',
  `query` varchar(128) NOT NULL DEFAULT '',
  `language` varchar(20) NOT NULL DEFAULT '',
  `rank` bigint(10) NOT NULL DEFAULT '0' COMMENT '商家等级',
  `goods_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '物品数量',
  `query_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '查询数量',
  PRIMARY KEY (`goods_id`,`query`,`language`),
  KEY `query` (`query`) USING BTREE,
  KEY `goods_id` (`goods_id`) USING BTREE
) ENGINE=InnoDB ;

