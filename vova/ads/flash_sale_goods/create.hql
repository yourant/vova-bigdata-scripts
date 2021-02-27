drop table ads.ads_vova_flash_sale_goods_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_flash_sale_goods_d
(
    goods_id                   bigint COMMENT '商品id',
    second_cat_id              bigint COMMENT '二级分类id',
    region_id                  bigint COMMENT '国家id',
    event_type                 string COMMENT '类型',
    flash_sale_date            string COMMENT '活动日期',
    rank                       bigint COMMENT '序号'
) COMMENT 'flash_sale活动商品'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;





CREATE TABLE ads_flash_sale_goods  (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL  COMMENT '商品id',
  second_cat_id int(11) NOT NULL  COMMENT '二级分类id',
  region_id int(11) NOT NULL COMMENT '国家id',
  event_type varchar(10) NOT NULL COMMENT '类型',
  flash_sale_date varchar(50) NOT NULL DEFAULT '' COMMENT '活动日期',
  rank int(11) NOT NULL  COMMENT '' COMMENT '序号',
  update_time datetime NOT NULL default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,second_cat_id,region_id,rank,flash_sale_date,event_type),
  KEY flash_sale_date (flash_sale_date),
  KEY goods_id (goods_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;