 --商品维度汇总表
drop table ads.ads_vova_mct_black_list_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_mct_black_list_d
(
    mct_id                  BIGINT COMMENT '商家id'
) COMMENT '商家黑名单'
 PARTITIONED BY (pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE ads_vova_mct_black_list (
  mct_id int(11)  NOT NULL DEFAULT '0' COMMENT '商家id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (mct_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商家黑名单';