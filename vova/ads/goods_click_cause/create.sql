DROP TABLE IF EXISTS tmp.tmp_vova_user_clk_behave_link_d;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_vova_user_clk_behave_link_d
(
    session_id    STRING,
    buyer_id      BIGINT,
    gender        STRING,
    language_id   STRING,
    country_id    STRING,
    os_type       STRING,
    device_model  STRING,
    goods_id      BIGINT,
    first_cat_id  BIGINT,
    second_cat_id BIGINT,
    cat_id        BIGINT,
    mct_id        BIGINT,
    brand_id      BIGINT,
    shop_price    DECIMAL(14, 4),
    shipping_fee  DECIMAL(14, 4),
    click_time    STRING,
    page_code     STRING,
    list_type     STRING,
    clk_from      STRING,
    enter_ts      STRING,
    leave_ts      STRING,
    stay_time     DOUBLE,
    is_add_cart   INT,
    is_collect    INT,
    device_id     STRING,
    goods_name    STRING,
    expre_time    STRING,
    is_click      INT,
    is_order      INT
) COMMENT '商品点击链路' PARTITIONED BY (pt STRING)
    row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

DROP TABLE IF EXISTS tmp.tmp_vova_link_order_pay;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_vova_link_order_pay
(
    buyer_id    STRING,
    device_id        STRING,
    order_id   BIGINT,
    pay_time    STRING,
    virtual_goods_id    BIGINT
) COMMENT 'tmp'
    row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
;

DROP TABLE IF EXISTS ads.ads_vova_user_behave_link_d;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_user_behave_link_d
(
    session_id       STRING,
    buyer_id         BIGINT,
    gender           STRING,
    language_id         STRING,
    country_id      STRING,
    os_type          STRING,
    device_model       STRING,
    goods_id         BIGINT,
    first_cat_id     BIGINT,
    second_cat_id    BIGINT,
    cat_id           BIGINT,
    mct_id           BIGINT,
    brand_id         BIGINT,
    shop_price       DECIMAL(14, 4),
    shipping_fee     DECIMAL(14, 4),
    click_time       STRING,
    page_code        STRING,
    list_type        STRING,
    clk_from       STRING,
    enter_ts         STRING,
    leave_ts         STRING,
    stay_time        DOUBLE,
    is_add_cart      INT,
    is_collect       INT,
    device_id     STRING,
    goods_name    STRING,
    expre_time    STRING,
    is_click      INT,
    is_order      INT
) COMMENT '商品点击链路' PARTITIONED BY (pt STRING,pagecode STRING)
    row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/base/ads_vova_user_behave_link_d'
;

alter table ads.ads_vova_user_behave_link_d add columns(`goods_clk_list` string,
`goods_cart_list` string,
`goods_wish_list` string,
`search_words_list` string
 ) cascade;
