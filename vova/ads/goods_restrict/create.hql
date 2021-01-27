 --商品维度汇总表
drop table ads.ads_vova_goods_restrict_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_restrict_d
(
    goods_id                  BIGINT COMMENT '商品所属商家ID',
    sales_order               BIGINT COMMENT '销量',
    nlrf_rate_5_8w            DECIMAL(14, 4)  COMMENT '五到八周非物流退款率'
) COMMENT '高退款率商品'
 PARTITIONED BY (pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE ads_goods_restrict_d (
  goods_id int(11)  NOT NULL DEFAULT '0' COMMENT '商品id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '高退款率商品表';