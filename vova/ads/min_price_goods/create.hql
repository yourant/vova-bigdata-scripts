drop table ads.ads_vova_min_price_goods_h;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_min_price_goods_h
(
    goods_id                   bigint COMMENT '商品id',
    min_price_goods_id         bigint COMMENT '分组最低价商品id',
    strategy                   string COMMENT '统计策略',
    group_number               string COMMENT '组号',
    min_show_price             DECIMAL(14, 4) COMMENT '最低价',
    avg_sku_price              DECIMAL(14, 4) COMMENT '商品的sku均价'
) COMMENT '最低价商品'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_min_price_goods_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_min_price_goods_d
(
    goods_id                   bigint COMMENT '商品id',
    min_price_goods_id         bigint COMMENT '分组最低价商品id',
    strategy                   string COMMENT '统计策略',
    group_number               string COMMENT '组号',
    min_show_price             DECIMAL(14, 4) COMMENT '最低价',
    avg_sku_price              DECIMAL(14, 4) COMMENT '商品的sku均价'
) COMMENT '最低价商品'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table ads.ads_vova_min_price_goods;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_min_price_goods
(
    goods_id                   bigint COMMENT '商品id',
    min_price_goods_id         bigint COMMENT '分组最低价商品id',
    group_number               string COMMENT '组号',
    min_show_price             DECIMAL(14, 4) COMMENT '最低价',
    avg_sku_price              DECIMAL(14, 4) COMMENT '商品的sku均价'
) COMMENT '最低价商品'
PARTITIONED BY ( pt string,strategy string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table ads.ads_vova_min_price_goods_h;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_min_price_goods_h
(
    goods_id                   bigint COMMENT '商品id',
    min_price_goods_id         bigint COMMENT '分组最低价商品id',
    strategy                   string COMMENT '统计策略',
    group_number               string COMMENT '组号',
    min_show_price             DECIMAL(14, 4) COMMENT '最低价',
    avg_sku_price              DECIMAL(14, 4) COMMENT '商品的sku均价'
) COMMENT '最低价商品'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.ads_min_price_goods_h;
CREATE TABLE IF NOT EXISTS tmp.ads_min_price_goods_h
(
    goods_id                   bigint COMMENT '商品id',
    min_price_goods_id         bigint COMMENT '分组最低价商品id',
    strategy                   string COMMENT '统计策略',
    group_number               string COMMENT '组号',
    min_show_price             DECIMAL(14, 4) COMMENT '最低价',
    avg_sku_price              DECIMAL(14, 4) COMMENT '商品的sku均价'
) COMMENT '最低价商品'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



CREATE TABLE ads_min_price_goods_h_test (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE ads_min_price_goods_d (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  avg_sku_price decimal(14,4) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;