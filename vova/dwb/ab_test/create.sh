DROP TABLE IF EXISTS dwb.dwb_vova_ab_test;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_ab_test
(
    cur_date       string COMMENT 'd_日期',
    datasource     string COMMENT 'd_datasource',
    platform       string COMMENT 'd_平台',
    os             string COMMENT 'd_os',
    rec_page_code  string COMMENT 'd_页面',
    rec_code       string COMMENT 'd_实验号',
    rec_version    string COMMENT 'd_实验版本',
    expre_pv       string COMMENT 'i_曝光pv',
    clk_pv         string COMMENT 'i_点击pv',
    ctr            string COMMENT 'i_ctr',
    expre_uv       string COMMENT 'i_曝光uv',
    clk_uv         string COMMENT 'i_点击uv',
    cart_uv        string COMMENT 'i_加购uv',
    cart_rate      string COMMENT 'i_加购率',
    gmv            string COMMENT 'i_gmv',
    goods_number   string COMMENT 'i_goods_number',
    pay_uv         string COMMENT 'i_支付uv',
    cr             string COMMENT 'i_cr',
    impressions_cr string COMMENT 'i_impressions_cr',
    gmv_cr         string COMMENT 'i_gmv_cr',
    brand_status   string COMMENT 'i_brand_status'
) COMMENT 'ab实验' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwb.dwb_vova_ab_test ADD COLUMNS (
order_cnt STRING COMMENT '订单数'
) CASCADE;


DROP TABLE IF EXISTS dwb.dwb_vova_gcr_ab;
CREATE EXTERNAL TABLE dwb.dwb_vova_gcr_ab
(
    cur_date      STRING COMMENT 'd_日期',
    platform      STRING COMMENT 'd_平台',
    rec_page_code STRING COMMENT 'd_page_code',
    expre_pv_a    STRING COMMENT 'i_a版曝光',
    expre_pv_b    STRING COMMENT 'i_b版曝光',
    expre_incom_a STRING COMMENT 'i_a版曝光收益',
    expre_incom_b STRING COMMENT 'i_b版曝光收益',
    change_rate_a STRING COMMENT 'i_a版转化率',
    change_rate_b STRING COMMENT 'i_b版转化率'
)
    COMMENT 'gcr排序AB报表'
    PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;


DROP TABLE IF EXISTS dwb.dwb_vova_ab_test_h;
CREATE EXTERNAL TABLE dwb.dwb_vova_ab_test_h
(
    cur_date       STRING COMMENT 'd_日期',
    cur_hour       STRING COMMENT 'd_小时',
    datasource     STRING COMMENT 'd_datasource',
    platform       STRING COMMENT 'd_平台',
    os             STRING COMMENT 'd_os',
    rec_page_code  STRING COMMENT 'd_页面',
    rec_code       STRING COMMENT 'd_实验号',
    rec_version    STRING COMMENT 'd_实验版本',
    expre_pv       STRING COMMENT 'i_曝光pv',
    clk_pv         STRING COMMENT 'i_点击pv',
    ctr            STRING COMMENT 'i_ctr',
    expre_uv       STRING COMMENT 'i_曝光uv',
    clk_uv         STRING COMMENT 'i_点击uv',
    cart_uv        STRING COMMENT 'i_加购uv',
    cart_rate      STRING COMMENT 'i_加购率',
    gmv            STRING COMMENT 'i_gmv',
    goods_number   STRING COMMENT 'i_goods_number',
    pay_uv         STRING COMMENT 'i_支付uv',
    cr             STRING COMMENT 'i_cr',
    impressions_cr STRING COMMENT 'i_impressions_cr',
    gmv_cr         STRING COMMENT 'i_gmv_cr'
)
    COMMENT 'ab实验分时'
    PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE IF EXISTS dwb.dwb_vova_ab_test_norepeat;
CREATE EXTERNAL TABLE dwb.dwb_vova_ab_test_norepeat
(
    cur_date       STRING COMMENT 'd_日期',
    datasource     STRING COMMENT 'd_datasource',
    platform       STRING COMMENT 'd_平台',
    os             STRING COMMENT 'd_os',
    rec_page_code  STRING COMMENT 'd_页面',
    rec_code       STRING COMMENT 'd_实验号',
    rec_version    STRING COMMENT 'd_实验版本',
    expre_pv       STRING COMMENT 'i_曝光pv',
    clk_pv         STRING COMMENT 'i_点击pv',
    ctr            STRING COMMENT 'i_ctr',
    expre_uv       STRING COMMENT 'i_曝光uv',
    clk_uv         STRING COMMENT 'i_点击uv',
    cart_uv        STRING COMMENT 'i_加购uv',
    cart_rate      STRING COMMENT 'i_加购率',
    gmv            STRING COMMENT 'i_gmv',
    goods_number   STRING COMMENT 'i_goods_number',
    pay_uv         STRING COMMENT 'i_支付uv',
    cr             STRING COMMENT 'i_cr',
    impressions_cr STRING COMMENT 'i_impressions_cr',
    gmv_cr         STRING COMMENT 'i_gmv_cr'
)
    PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwb.dwb_vova_ab_test_norepeat ADD COLUMNS (
order_cnt STRING COMMENT '订单数'
) CASCADE;


DROP TABLE IF EXISTS tmp.vova_ab_clk_tmp_distinct_h;
CREATE EXTERNAL TABLE tmp.vova_ab_clk_tmp_distinct_h
(
    datasource    STRING,
    hour          string,
    platform      string,
    rec_page_code string,
    rec_code      string,
    rec_version   string,
    device_id_clk string,
    cnt           bigint

)
    STORED AS PARQUETFILE;