drop table dwb.dwb_vova_shield_goods;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_shield_goods
(
event_date          string   COMMENT '事件发生日期',
goods_id            bigint   COMMENT '商品id',
goods_sn            string   COMMENT '商品sn',
virtual_goods_id    bigint   COMMENT '虚拟id',
shield_cnt          bigint   COMMENT '屏蔽次数',
is_flow             string   COMMENT '是否有五级商家跟卖'
flag                string   COMMENT '屏蔽类型'
)COMMENT '商品屏蔽报表'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE `rpt_shield_goods` (
  `event_date` varchar(32) NOT NULL DEFAULT '' COMMENT '日期yyyy-MM-dd',
  `goods_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '商品id',
  `goods_sn` varchar(64) NOT NULL DEFAULT '' COMMENT '商品sn',
  `virtual_goods_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '虚拟商品id',
  `shield_cnt` bigint(20) NOT NULL DEFAULT '0' COMMENT '屏蔽次数',
  `is_flow` varchar(10) NOT NULL DEFAULT '' COMMENT '是否五级跟卖',
  `flag` varchar(10) NOT NULL,
  PRIMARY KEY (`event_date`,`goods_id`,`goods_sn`,`virtual_goods_id`,`flag`),
  KEY `event_date` (`event_date`),
  KEY `goods_id` (`goods_id`),
  KEY `goods_sn` (`goods_sn`),
  KEY `virtual_goods_id` (`virtual_goods_id`),
  KEY `is_flow` (`is_flow`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品屏蔽报表';

drop table dwb.dwb_vova_shield_goods_sn;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_shield_goods_sn
(
event_date          string   COMMENT '事件发生日期',
goods_sn            string   COMMENT '商品sn',
first_cat_name      string   COMMENT '一级品类名称',
second_cat_name     string   COMMENT '二级品类名称',
shield_cnt          bigint   COMMENT '屏蔽次数',
gmv_1w              decimal(10, 4)   COMMENT '当周gmv',
gmv_last_w          decimal(10, 4)   COMMENT '上一周gmv',
is_flow             string   COMMENT '是否有五级商家跟卖'
)COMMENT '商品sn屏蔽报表'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE rpt_shield_goods_sn (
  `event_date` varchar(32) NOT NULL DEFAULT '' COMMENT '日期yyyy-MM-dd',
  `goods_sn` varchar(64) NOT NULL DEFAULT '' COMMENT '商品sn',
  `first_cat_name` varchar(64) NOT NULL DEFAULT '' COMMENT '一级品类名称',
  `second_cat_name` varchar(64) NOT NULL DEFAULT '' COMMENT '二级品类名称',
  `shield_cnt` bigint(20) NOT NULL DEFAULT '0' COMMENT '屏蔽次数',
  `gmv_1w` decimal(10,4) NOT NULL DEFAULT '0' COMMENT '当周gmv',
  `gmv_last_w` decimal(10,4) NOT NULL DEFAULT '0' COMMENT '上一周gmv',
  `is_flow` varchar(10) NOT NULL DEFAULT '' COMMENT '是否五级跟卖',
  PRIMARY KEY (`event_date`,`goods_sn`),
  KEY `event_date` (`event_date`),
  KEY `goods_sn` (`goods_sn`),
  KEY `is_flow` (`is_flow`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品sn屏蔽报表';
