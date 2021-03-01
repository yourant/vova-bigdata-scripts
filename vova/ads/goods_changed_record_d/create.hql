drop table ads.ads_vova_goods_changed_record_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_changed_record_d
(
    goods_id                   bigint COMMENT '商品id',
    goods_name_flag            bigint COMMENT 'goods_name字段是否被更新，1表示更新，0表示不更新',
    goods_desc_flag            bigint COMMENT 'goods_desc字段是否被更新，1表示更新，0表示不更新',
    goods_thumb_flag           bigint COMMENT 'goods_thumb字段是否被更新，1表示更新，0表示不更新',
    daytime                    string COMMENT '时间（最小维度为1天）如2020-06-10'
) COMMENT '商品内容变更记录表'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



CREATE TABLE goods_changed_day_record  (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  goods_name_flag tinyint  (11) NOT NULL,
  goods_desc_flag tinyint  (11) NOT NULL,
  goods_thumb_flag tinyint  (11) NOT NULL,
  daytime varchar(16) NOT NULL,
  update_time datetime NOT NULL default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,daytime)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;