drop table if exists dwd.dwd_vova_ab_test_expre;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_ab_test_expre
(
    datasource    string,
    platform      string,
    os            string,
    rec_page_code string,
    rec_code      string,
    rec_version   string,
    rp_name       string,
    is_single     string,
    device_id     string,
    buyer_id      string,
    virtual_goods_id      string
) COMMENT 'dwd_vova_fact_ab_test_expre'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




alter table dwd.dwd_vova_ab_test_expre ADD COLUMNS (
is_brand STRING COMMENT '是否品牌'
) CASCADE;

drop table if exists dwd.dwd_vova_ab_test_clk;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_ab_test_clk
(
    datasource    string,
    platform      string,
    os            string,
    rec_page_code string,
    rec_code      string,
    rec_version   string,
    rp_name       string,
    is_single     string,
    device_id     string,
    buyer_id      string,
    virtual_goods_id      string
) COMMENT 'dwd_vova_fact_ab_test_clk'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwd.dwd_vova_ab_test_clk ADD COLUMNS (
is_brand STRING COMMENT '是否品牌'
) CASCADE;

drop table if exists dwd.dwd_vova_ab_test_cart;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_ab_test_cart
(
    datasource    string,
    platform      string,
    os            string,
    rec_page_code string,
    rec_code      string,
    rec_version   string,
    rp_name       string,
    is_single     string,
    device_id     string,
    buyer_id      string,
    virtual_goods_id      string
) COMMENT 'fact_ab_cart'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwd.dwd_vova_ab_test_cart ADD COLUMNS (
is_brand STRING COMMENT '是否品牌'
) CASCADE;

drop table if exists dwd.dwd_vova_ab_test_pay;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_ab_test_pay
(
    datasource    string,
    platform      string,
    os            string,
    rec_page_code string,
    rec_code      string,
    rec_version   string,
    rp_name       string,
    is_single     string,
    price         double,
    device_id     string,
    buyer_id      string,
    virtual_goods_id      string
) COMMENT 'fact_ab_pay'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwd.dwd_vova_ab_test_pay ADD COLUMNS (
is_brand STRING COMMENT '是否品牌'
) CASCADE;

alter table dwd.dwd_vova_ab_test_pay ADD COLUMNS (
order_goods_id STRING COMMENT '是否品牌'
) CASCADE;