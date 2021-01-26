drop table if exists dwb.dwb_vova_banner_ab;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_banner_ab
(
    cur_date    string COMMENT 'd_日期',
    geo_country string COMMENT 'd_国家',
    app_version string COMMENT 'd_app版本',
    rec_version string COMMENT 'd_实验版本',
    is_gods     string COMMENT 'd_是否带goods id',
    expre_pv    bigint COMMENT 'i_banner曝光量',
    clk_pv      bigint COMMENT 'i_banner点击量',
    ctr         string COMMENT 'i_ctr',
    expre_uv    bigint COMMENT 'i_banner曝光量uv',
    clk_uv      bigint COMMENT 'i_banner点击量uv',
    ctr_uv      string COMMENT 'i_ctr_uv'
) COMMENT 'banner_ab报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
