drop table if exists dwd.dwd_vova_rec_report_clk_expre;
CREATE EXTERNAL TABLE dwd.dwd_vova_rec_report_clk_expre
(
    datasource      string,
    country         string,
    os_type         string,
    page_code       string,
    element_type    string,
    list_type       string,
    rec_page_code   string,
    activate_time   string,
    device_id_clk   string,
    device_id_expre string,
    clks            bigint,
    expres          bigint,
    is_brand        string,
    brand_status    string
)
    COMMENT 'dwd_vova_rec_report_clk_expre'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

drop table if exists dwd.dwd_vova_rec_report_cart_cause;
CREATE EXTERNAL TABLE dwd.dwd_vova_rec_report_cart_cause
(
    datasource    string,
    os_type       string,
    country       string,
    page_code     string,
    element_type  string,
    list_type     string,
    rec_page_code string,
    activate_time string,
    device_id     string,
    is_brand      string,
    brand_status  string
)
    COMMENT 'dwd_vova_rec_report_cart_cause'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

drop table if exists dwd.dwd_vova_rec_report_order_cause;
CREATE EXTERNAL TABLE dwd.dwd_vova_rec_report_order_cause
(
    datasource     string,
    country        string,
    os_type        string,
    page_code      string,
    element_type   string,
    list_type      string,
    rec_page_code  string,
    activate_time  string,
    device_id      string,
    buyer_id       string,
    order_goods_id string,
    is_brand       string,
    brand_status   string
)
    COMMENT 'dwd_vova_rec_report_order_cause'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;

drop table if exists dwd.dwd_vova_rec_report_pay_cause;
CREATE EXTERNAL TABLE dwd.dwd_vova_rec_report_pay_cause
(
    datasource     string,
    country        string,
    os_type        string,
    page_code      string,
    element_type   string,
    list_type      string,
    rec_page_code  string,
    activate_time  string,
    device_id      string,
    buyer_id       string,
    order_goods_id string,
    goods_number   string,
    gmv            string,
    is_brand       string,
    brand_status   string
)
    COMMENT 'dwd_vova_rec_report_pay_cause'
    PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;
