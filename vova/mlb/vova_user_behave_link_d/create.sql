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

ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(geo_city string COMMENT 'snowplow上传城市') CASCADE;
ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(geo_latitude string COMMENT 'snowplow上传位置纬度') CASCADE;
ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(geo_longitude string COMMENT 'snowplow上传位置经度') CASCADE;
ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(geo_region string COMMENT 'snowplow上传国家和地区的代码ISO-3166-2') CASCADE;
ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(absolute_position string COMMENT '绝对位置') CASCADE;
ALTER TABLE tmp.tmp_vova_user_clk_behave_link_d ADD COLUMNS(imsi string COMMENT 'imsi') CASCADE;

DROP TABLE IF EXISTS mlb.mlb_vova_user_behave_link_d;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_user_behave_link_d
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
    is_order      INT,
    near_clk_goods      STRING,
    near_collect_goods      STRING,
    near_add_cart_goods      STRING,
    near_query      STRING
) COMMENT '商品点击链路' PARTITIONED BY (pt STRING,pagecode STRING)
    row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/base/mlb_vova_users_behave_link_d'
;

