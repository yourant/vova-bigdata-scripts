drop table if exists dwb.dwb_vova_goods_img_group_d;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_goods_img_group_d
(
    event_date      string COMMENT 'd_日期',
    one_gp_cnt      int    COMMENT '每日只组内有一个商品的组数',
    gp_cnt          int    COMMENT '每日商品分组数',
    g_cnt           int    COMMENT '每日全站在架商品数'
) COMMENT '商品图片分组统计报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



CREATE TABLE rpt_goods_img_group_d (
  event_date varchar(16) NOT NULL DEFAULT '',
  one_gp_cnt int(11)  NOT NULL DEFAULT '0' COMMENT '每日只组内有一个商品的组数',
  gp_cnt int(11)  NOT NULL DEFAULT '0' COMMENT '每日商品分组数',
  g_cnt int(11)  NOT NULL DEFAULT '0' COMMENT '每日全站在架商品数',
  primary KEY (event_date)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4;
