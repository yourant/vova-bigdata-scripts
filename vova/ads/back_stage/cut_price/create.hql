drop table ads.ads_vova_goods_sn_cut_price;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_sn_cut_price
(
    event_date             string COMMENT '日期',
    goods_sn               string COMMENT '商品sn'
) COMMENT '降价系统top100'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_gsn_avg_price_h;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_gsn_avg_price_h
(
    goods_sn               string COMMENT '商品sn',
    gsn_avg_price          DECIMAL(14, 4) COMMENT '近7天售出均价'
) COMMENT 'gsn近七日商品均价表'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_gsn_reduce_valid_goods;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_gsn_reduce_valid_goods
(
    goods_id               bigint COMMENT '商品id',
    add_cycle              bigint COMMENT '加入周期',
    expre                  bigint COMMENT '曝光数',
    sales_order            bigint COMMENT '商品销量',
    expre_cr               DECIMAL(14, 4) COMMENT '转化率'
) COMMENT 'gsn降价商品统计'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




CREATE TABLE `ads_goods_sn_cut_price` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods_sn` varchar(64)  NOT NULL DEFAULT '',
  `event_date` varchar(16) NOT NULL DEFAULT '',
  `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY ux_goods_sn (goods_sn,event_date),
  KEY `goods_sn` (`goods_sn`),
  KEY `event_date` (`event_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;



CREATE TABLE `ads_gsn_avg_price` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods_sn` varchar(64)  NOT NULL DEFAULT '',
  `gsn_avg_price` DECIMAL(14, 4) NOT NULL DEFAULT '0',
  `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY ux_goods_sn (goods_sn),
  KEY `goods_sn` (`goods_sn`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ads_gsn_reduce_valid_goods` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods_id` int(11)  NOT NULL DEFAULT '0' COMMENT '商品id',
  `add_cycle` int(4)  NOT NULL DEFAULT '0' COMMENT '加入周期',
  `expre` int(11)  NOT NULL DEFAULT '0' COMMENT '曝光量',
  `sales_order` int(11)  NOT NULL DEFAULT '0' COMMENT '销量',
  `expre_cr` DECIMAL(14, 4) NOT NULL DEFAULT '0' COMMENT '转化率',
  `last_update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY ux_goods_id (goods_id,add_cycle),
  KEY `goods_id` (`goods_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;


