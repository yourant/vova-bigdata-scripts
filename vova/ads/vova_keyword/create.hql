【数据】[10041] 列表页&搜索结果页新增关键词卡片算法逻辑
需求描述：

#关键词词库：

目前关键词共分为三种，

品牌词：来自规整后的品牌词表，包括主品牌和子品牌；

品类词：包括一、二、三、四级品类名称；

属性+品类词：同榜单使用的属性词，拼接方法为：属性词+二级品类、属性词+三级品类、属性词+四级品类。



#关键词翻译：

需要对关键词中的品类词和属性+品类词进行翻译，翻译成英法德意西版本。

**取不到翻译结果的情况下取英语结果。



#关键词计算：

goods_id-关键词：计算商品评分>=30的商品所带有的品牌词（单个）、品类词（多个）和属性+品类词（多个），优先级为：属性+品类词>品牌词>品类词；

cat_id-关键词：计算二、三、四级品类下需要展示的关键词，根据每个cat_id下商品评分top1000商品计算该cat_id下最热门的品类词（多个）和属性+品类词（多个），计算逻辑为如果top1000商品每个商品带有某个关键词，则该关键词+1分，最终按照关键词得分由大到小排序后输出，需要过滤掉当前cat_id对应的cat_name重复的关键词；

query-关键词：计算top7000 query（翻译后）每个query下需要展示的关键词，根据每个query下高频搜索词结果计算该query下最热门的品牌词（多个）、品类词（多个）和属性+品类词（多个），计算逻辑为如果该query下每个高频搜索词结果带有某个关键词，则该关键词+1分，最终按照关键词得分由大到小排序后输出，需要过滤掉和当前query重复的关键词。

**需要对关键词中的品牌词进行标识，在屏蔽状态下过滤掉这些词。

**关键词列表里均不适用一级品类。
CREATE TABLE IF NOT EXISTS `rec_recall`.`ads_vova_keyword` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `keyword_id` int(11) NOT NULL COMMENT '关键词id',
  `keyword_name` varchar(255) NOT NULL COMMENT '关键词名称',
  `keyword_type` int(4) NOT NULL COMMENT '关键词类型',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `keyword_id` (`keyword_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关键词';

CREATE TABLE IF NOT EXISTS `rec_recall`.`ads_vova_keyword_name_translation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `keyword_id` int(11) NOT NULL COMMENT '关键词id',
  `keyword_name_translation` varchar(255) NOT NULL COMMENT '关键词名称(转译)',
  `language_id` int(4) NOT NULL COMMENT '语言id',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `keyword_id` (`keyword_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关键词翻译表';

CREATE TABLE IF NOT EXISTS `rec_recall`.`ads_vova_result_page_keyword_rank_data` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`type` int(4) NOT NULL COMMENT '计算类型(1 goods_id,2 cat_id,3 query)',
`type_value` varchar(255) NOT NULL COMMENT '类型值',
`keyword_id` int(4) NOT NULL COMMENT '关键词id',
`rank` int(4) NOT NULL COMMENT '排名',
`update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`id`) USING BTREE,
KEY `type_value` (`type_value`),
KEY `keyword_id` (`keyword_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='列表页&搜索结果页关键词排名数据';

CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_result_page_keyword_rank_data
 (
  type bigint COMMENT '计算类型(1 goods_id,2 cat_id,3 query)',
  type_value string COMMENT '类型值',
  keyword_id bigint COMMENT '关键词id',
  rank bigint COMMENT '排名'
 ) COMMENT '列表页&搜索结果页关键词排名数据' PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;