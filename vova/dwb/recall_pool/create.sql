drop table if exists dwb.dwb_vova_recall_pool_v2;
CREATE EXTERNAL TABLE dwb.dwb_vova_recall_pool_v2
(
    pts           STRING COMMENT 'd_日期',
    datasource    STRING COMMENT 'd_datasource',
    rec_page_code STRING COMMENT 'd_rec_page_code',
    rp_name       STRING COMMENT 'd_rp_name',
    is_single     STRING COMMENT 'd_is_single',
    rec_code      STRING COMMENT 'd_rec_code',
    rec_version   STRING COMMENT 'd_rec_version',
    expre_pv      BIGINT COMMENT 'i_曝光pv',
    clk_pv        BIGINT COMMENT 'i_点击pv',
    expre_uv      BIGINT COMMENT 'i_曝光uv',
    pay_uv        BIGINT COMMENT 'i_支付pv',
    goods_number  BIGINT COMMENT 'i_商品数量',
    gmv           BIGINT COMMENT 'i_gmv',
    expre_pv_rate STRING COMMENT 'i_rp覆盖率',
    expre_uv_rate STRING COMMENT 'i_rp覆盖用户比例',
    all_expre_pv  STRING COMMENT 'i_all_expre_pv',
    all_expre_uv  STRING COMMENT 'i_all_expre_uv'
)
    COMMENT '召回监控报表'
    PARTITIONED BY (pt STRING)  STORED AS PARQUETFILE;

alter table dwb.dwb_vova_recall_pool_v2 ADD COLUMNS (
order_cnt STRING COMMENT '订单数'
) CASCADE;




drop table if exists tmp.tmp_vova_recall_pool_v2_tmp;
CREATE TABLE tmp.tmp_vova_recall_pool_v2_tmp
(
    datasource1    string,
    rec_page_code1 string,
    rp_name1       string,
    is_single1     string,
    rec_code1      string,
    rec_version1   string,
    expre_pv       bigint,
    clk_pv         bigint,
    expre_uv       bigint,
    pay_uv         bigint,
    gmv            double,
    order_cnt      bigint
)
    COMMENT 'tmp召回监控报表'
    STORED AS PARQUETFILE;