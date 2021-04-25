CREATE TABLE IF NOT EXISTS tmp.dwb_fd_ads_gmv
(
    event_date   string COMMENT "时间",
    project_name  string COMMENT 'datasource',
    country_code string COMMENT 'region_code',
    ga_channel  string COMMENT 'ga_channel',
    platform    string COMMENT 'platform',
    gmv_1d      decimal(20, 2) COMMENT 'gmv_1d',
    gmv_7d      decimal(20, 2) COMMENT 'gmv_7d',
    gmv_30d     decimal(20, 2) COMMENT 'gmv_30d',
    gmv_90d     decimal(20, 2) COMMENT 'gmv_90d',
    gmv_180d    decimal(20, 2) COMMENT 'gmv_180d'
) COMMENT 'dwb_fd_ads_gmv'
    STORED AS PARQUETFILE;