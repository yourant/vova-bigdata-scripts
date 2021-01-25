DROP TABLE IF EXISTS dwb.dwb_vova_register_time_dau_rate;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_register_time_dau_rate
(
    `dau`                    bigint          COMMENT 'i_dau',
    `reg_rate_1d`            decimal(13,2)   COMMENT 'i_当天注册dau占比',
    `reg_rate_2_7d`          decimal(13,2)   COMMENT 'i_2~7天注册dau占比',
    `reg_rate_8_30d`         decimal(13,2)   COMMENT 'i_8~30天注册dau占比',
    `reg_rate_30d`           decimal(13,2)   COMMENT 'i_30天以上注册dau占比',
    `pay_rate_1d`            decimal(13,2)   COMMENT 'i_当天注册转化率',
    `pay_rate_2_7d`          decimal(13,2)   COMMENT 'i_2~7天注册转化率',
    `pay_rate_8_30d`         decimal(13,2)   COMMENT 'i_8~30当天注册转化率',
    `pay_rate_30d`           decimal(13,2)   COMMENT 'i_30天以上注册转化率',
    `order_rate_1d`          decimal(13,2)   COMMENT 'i_当天注册加购转化率',
    `order_rate_2_7d`        decimal(13,2)   COMMENT 'i_2~7天注册加购转化率',
    `order_rate_8_30d`       decimal(13,2)   COMMENT 'i_8~30天注册加购转化率',
    `order_rate_30d`         decimal(13,2)   COMMENT 'i_30天以上注册加购转化率'
) COMMENT '注册用户占比表' PARTITIONED BY (pt string)
    STORED AS PARQUETFILE;