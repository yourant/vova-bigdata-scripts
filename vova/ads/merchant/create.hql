drop table ads.ads_mct_d;
CREATE  TABLE IF NOT EXISTS ads.ads_mct_d
(
    merchant_id    BIGINT COMMENT '商家id',
    act_mct_2m     BIGINT COMMENT '最近两月激活的商家，1是，0否'
) COMMENT '商家信息'
    PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE TABLE if not exists ads_mct_d (
  merchant_id int(11) NOT NULL COMMENT '商品id',
  act_mct_2m int(4) NOT NULL COMMENT '最近两月激活的商家，1是，0否',
  PRIMARY KEY (`merchant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家信息';

drop table ads.ads_vova_mct_fulfillment_order;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_mct_fulfillment_order
(
    mct_id    BIGINT COMMENT '商家id',
    reg_time       timestamp COMMENT '店铺注册时间',
    is_new         BIGINT COMMENT '是否新商家，1是，0否',
    fulfillment_order_cnt BIGINT COMMENT '履约订单数',
    last_update_time timestamp COMMENT 'last_update_time'
) COMMENT '商家信息'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


select
       *
       from
(
    select goods_id
from (select 'a,b' AS goods_string) t
lateral view explode(split(goods_string,',')) num as goods_id
) a