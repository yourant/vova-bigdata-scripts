DROP TABLE dwb.dwb_vova_payment;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_payment
(
    event_date                                                      date COMMENT 'd_日期',
    datasource                                                      string COMMENT 'd_datasource',
    region_code                                                     string COMMENT 'd_国家',
    platform                                                        string COMMENT 'd_平台',
    payment_name                                                    string COMMENT 'd_支付方式',
    order_count                                                     bigint COMMENT 'i_总订单数',
    user_count                                                      bigint COMMENT '',
    pay_success_order_count                                         bigint COMMENT 'i_支付成功订单数',
    pay_success_user_count                                          bigint COMMENT '',
    try_order_count                                                 bigint COMMENT 'i_尝试支付订单数',
    try_order_count_pv                                              bigint COMMENT 'i_支付尝试总次数',
    try_user_count                                                  bigint COMMENT '',
    try_insufficient_amount_order_count                             bigint COMMENT '',
    try_insufficient_amount_user_count                              bigint COMMENT '',
    last_pay_success_order_count                                    bigint COMMENT '',
    last_try_order_count                                            bigint COMMENT '',
    log_try_pv                                                      bigint COMMENT '',
    log_try_uv                                                      bigint COMMENT '',
    try_order_count_div_order_count                                 string COMMENT 'i_订单尝试支付率',
    try_user_count_div_user_count                                   string COMMENT 'i_用户尝试支付率',
    pay_success_order_count_div_try_order_count                     string COMMENT 'i_订单尝试支付成功率',
    pay_success_order_count_div_try_order_count_compare_last        string COMMENT 'i_相比上一日成功率波动',
    pay_success_order_count_div_order_count                         string COMMENT 'i_订单支付成功率',
    pay_success_order_count_div_try_insufficient_amount_order_count string COMMENT 'i_（除金额不足）成功率',
    pay_success_order_count_div_try_order_count_pv                  string COMMENT 'i_单次尝试支付成功率'
) COMMENT '支付成功率报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_payment_error_code_base;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_payment_error_code_base
(
    event_date   date COMMENT 'd_日期',
    order_sn     string COMMENT 'i_order_sn',
    payment_code string COMMENT 'i_payment_code',
    txn_post     string COMMENT 'i_txn_post'
) COMMENT '支付日志error code'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_payment_error_code;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_payment_error_code
(
    event_date date COMMENT 'd_日期',
    order_sn   string COMMENT 'i_order_sn',
    error_code string COMMENT 'i_error_code'
) COMMENT '支付日志error code'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




