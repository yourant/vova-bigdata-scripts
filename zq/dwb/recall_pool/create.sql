drop table if exists dwb.dwb_fn_recall_pool;
CREATE EXTERNAL TABLE dwb.dwb_fn_recall_pool
(
    pts           string COMMENT 'd_日期',
    region_code   string COMMENT 'd_国家',
    datasource    string COMMENT 'd_datasource',
    rp_name       string COMMENT 'd_rp_name',
    is_single     string COMMENT 'd_is_single',
    rec_code      string COMMENT 'd_rec_code',
    rec_version   string COMMENT 'd_rec_version',
    rec_page_code string COMMENT 'd_rec_page_code',
    is_test       string COMMENT 'd_is_test',
    expre_pv      bigint COMMENT 'i_曝光pv',
    clk_pv        bigint COMMENT 'i_点击pv',
    ctr           bigint COMMENT 'i_ctr',
    expre_uv      bigint COMMENT 'i_曝光uv',
    clk_uv        bigint COMMENT 'i_点击uv',
    cart_uv       bigint COMMENT 'i_加购uv',
    cart_rate     string COMMENT 'i_加购率',
    pay_num       bigint COMMENT 'i_支付子订单数',
    pay_uv        bigint COMMENT 'i_支付uv',
    pay_rate      string COMMENT 'i_支付率',
    gmv           bigint COMMENT 'i_gmv',
    gmv_cr        string COMMENT 'i_gmv_cr'
)
    COMMENT 'FN召回监控报表'
    PARTITIONED BY (pt string) STORED AS PARQUETFILE;