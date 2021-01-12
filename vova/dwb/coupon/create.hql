drop table  dwb.dwb_vova_coupon;
CREATE external TABLE IF NOT EXISTS  dwb.dwb_vova_coupon
(
    event_date          date                comment 'd_日期'                  ,
    datasource          string              comment 'd_datasource',
    region_code         string              comment 'd_region_code',
    cpn_cfg_type        string              comment 'i_cpn_type',
    cpn_cfg_type_id     bigint              comment 'i_cpn_type_id',
    cpn_cfg_type_name   string              comment 'i_cpn_type_name',
    currency            string              comment 'i_currency',
    give_num            bigint              comment 'i_give_num',
    give_amount         DECIMAL(14, 2)      comment 'i_give_amount',
    give_user           bigint              comment 'i_give_user',
    use_num             bigint              comment 'i_use_num',
    use_amount          DECIMAL(14, 2)      comment 'i_use_amount',
    use_user            bigint              comment 'i_use_user',
    gmv                 DECIMAL(14, 2)      comment 'i_gmv'
) COMMENT 'rpt_coupon' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;