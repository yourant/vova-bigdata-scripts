drop table if exists dwd.dwd_vova_fact_ab_test_expre;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_ab_test_expre
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
    buyer_id      string
) COMMENT 'dwd_vova_fact_ab_test_expre'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwd.dwd_vova_fact_ab_test_clk;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_ab_test_clk
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
    buyer_id      string
) COMMENT 'dwd_vova_fact_ab_test_clk'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwd.dwd_vova_fact_ab_test_cart;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_ab_test_cart
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
    buyer_id      string
) COMMENT 'fact_ab_cart'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwd.dwd_vova_fact_ab_test_pay;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_ab_test_pay
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
    buyer_id      string
) COMMENT 'fact_ab_pay'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
