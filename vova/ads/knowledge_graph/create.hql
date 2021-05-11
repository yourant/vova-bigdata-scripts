【数据】[9034]知识图谱个性化热榜数据计算
榜单计算：

根据算法计算出来的标签组合+二级品类名称计算榜单数据，每种标签1+标签2+二级类目的组合即视为一个榜单；

每个榜单下的商品需要同时具有标签1和标签2的属性，并且在同一二级类目内；

每个榜单下的商品要保证合规性，即要符合商品画像是否可推荐中可推荐（is_recommend=1）的标准；

每个榜单要进行相似组去重，在榜单数据初步计算完成后，根据低价组c策略替换后去重；

每个榜单下的商品按照商品评分进行排序后输出；

最终线上应用时只保留榜单数量大于100的商品（算法需要同步进行过滤）；

榜单数据需要按日进行更新；

每个榜单对应唯一一个榜单id，且每个榜单对应的榜单id每日保持不变。

CREATE TABLE IF NOT EXISTS `ads_vova_bod_goods_rank_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bod_id` int(11) NOT NULL COMMENT '榜单id',
  `bod_name` varchar(255) NOT NULL COMMENT '榜单名称',
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `rank` int(11) NOT NULL COMMENT '商品评分排名',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `bod_id` (`bod_id`),
  KEY `goods_id` (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-榜单商品评分排名统计'

CREATE TABLE IF NOT EXISTS `ads_vova_bod_name_translation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bod_id` int(11) NOT NULL COMMENT '榜单id',
  `bod_name` VARCHAR(255) NOT NULL COMMENT '榜单名称',
  `bod_name_translation` VARCHAR(255) NOT NULL COMMENT '榜单名称(转译)',
  `language_id` int(4) NOT NULL COMMENT '语言id',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY bod_id (`bod_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-榜单名称转译'

CREATE TABLE IF NOT EXISTS `ads_vova_rec_m_user_kg_tag_d` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `buyer_id` int(11) NOT NULL COMMENT '用户id',
  `kg_tag_combine_list` mediumtext NOT NULL COMMENT '标签列表',
  `bod_id_list` mediumtext NOT NULL COMMENT '榜单id列表',
  `score_list` mediumtext NOT NULL COMMENT '分数列表',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `buyer_id` (`buyer_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1488901 DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-用户榜单偏好'

CREATE TABLE IF NOT EXISTS `ads_vova_rec_m_tagcombine_d` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `region_id` int(11) NOT NULL COMMENT '区域',
  `gender` varchar(10) NOT NULL COMMENT '性别',
  `user_age_group` varchar(100) NOT NULL COMMENT '用户年龄组',
  `kg_tag_combine` varchar(100) NOT NULL COMMENT '标签组合',
  `goods_id` int(11) NOT NULL COMMENT '商品id',
  `rank` int(11) NOT NULL COMMENT '商品排名',
  `tag_score` double(20,20) NOT NULL COMMENT '标签打分',
  `bod_id` int(11) NOT NULL COMMENT '榜单id',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `region_id` (`region_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识图谱-偏好召回'



CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_bod
(
    bod_id bigint COMMENT '榜单id',
    bod_name string COMMENT '榜单名称',
    bod_name_translation string COMMENT '榜单名称(转译)',
    language_id bigint COMMENT '语言id'
) COMMENT '知识图谱-榜单维表' ROW FORMAT DELIMITED FIELDS TERMINATED BY ':'  STORED AS TEXTFILE;

