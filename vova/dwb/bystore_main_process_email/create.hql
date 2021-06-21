drop table if exists dwb.dwb_vova_bystore_main_process;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_bystore_main_process
(
    is_new                     string COMMENT 'd_是否新用户',
    dau                        BIGINT COMMENT 'i_dau',
    homepage_dau               BIGINT COMMENT 'i_首页dau',
    first_order_num            BIGINT COMMENT 'i_首单订单量',
    payed_user_num             BIGINT COMMENT 'i_支付成功uv',
    payed_order_num            BIGINT COMMENT 'i_支付成功订单量',
    gmv                        DOUBLE COMMENT 'i_gmv',
    ordered_user_num           BIGINT COMMENT 'i_订单总人数',
    ordered_order_num          BIGINT COMMENT 'i_总单数',
    brand_gmv                  DOUBLE COMMENT 'i_brand_gmv',
    no_brand_gmv               DOUBLE COMMENT 'i_no_brand_gmv'
) COMMENT '主流程报表' PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;