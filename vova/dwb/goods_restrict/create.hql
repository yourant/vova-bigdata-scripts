 --商品维度汇总表
drop table dwb.dwb_vova_goods_restrict_d;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_goods_restrict_d
(
    event_date                string COMMENT '日期',
    goods_id                  BIGINT COMMENT '商品所属商家ID',
    first_cat_name            string COMMENT '一级品类',
    times                     DECIMAL(14, 4)  COMMENT '退款率大于一级品类退款率倍数',
    impressions               BIGINT COMMENT '曝光量'
) COMMENT '高退款率商品流量监控报表'
 PARTITIONED BY (pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE rpt_goods_restrict_d (
  goods_id int(11)  NOT NULL DEFAULT '0' COMMENT '商品id',
  event_date datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  first_cat_name varchar(64)  NOT NULL DEFAULT '0' COMMENT '一级品类',
  times DECIMAL(4, 2)  NOT NULL DEFAULT '0' COMMENT '退款率大于一级品类退款率倍数',
  impressions int(11)  NOT NULL DEFAULT '0' COMMENT '曝光量',
  PRIMARY KEY (goods_id,event_date)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;