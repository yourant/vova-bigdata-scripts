drop table ads.ads_vova_davinci_banner;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_davinci_banner
(
    goods_id                 bigint COMMENT '商品id',
    img_id                   bigint COMMENT '图片id',
    lng                      string COMMENT '语言code',
    img_url                  string COMMENT '图片链接',
    is_banner                bigint COMMENT '是否使用'
) COMMENT '国家topN热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location 's3://vomkt-emr-rec/data/ads_davinci_banner';

drop table ads.ads_vova_davinci_banner_handle;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_davinci_banner_handle(
  goods_id     bigint COMMENT '商品id',
  languages_id bigint,
  img_url      string COMMENT 's3图片url'
) PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE ads_davinci_banner_tmp (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11)  NOT NULL COMMENT '商品ID',
  languages_id int(4) NOT NULL  COMMENT '语言id',
  img_url varchar(255) NOT NULL  COMMENT '商品图片',
  last_update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,languages_id),
  KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='达芬奇banner';



drop table tmp.tmp_goods_m;
CREATE TABLE IF NOT EXISTS tmp.tmp_goods_m
(
    goods_id                 bigint COMMENT '商品id',
    img_url                  string COMMENT '图片链接',
    is_banner                bigint COMMENT '是否使用'
) COMMENT '国家topN热搜词'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location 's3://vomkt-emr-rec/data/tmp_goods_m';

msck repair table tmp.tmp_goods_m;