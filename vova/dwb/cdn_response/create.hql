DROP TABLE dwb.dwb_vova_app_response;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_app_response
(
    event_date    DATE COMMENT 'd_日期',
    region_code   string COMMENT 'd_国家',
    resource_type string COMMENT 'd_资源类型',
    res_100       bigint COMMENT '',
    res_200       bigint COMMENT '',
    res_300       bigint COMMENT '',
    res_400       bigint COMMENT '',
    res_500       bigint COMMENT '',
    res_600       bigint COMMENT '',
    res_700       bigint COMMENT '',
    res_800       bigint COMMENT '',
    res_900       bigint COMMENT '',
    res_1000      bigint COMMENT '',
    res_1100      bigint COMMENT '',
    res_1200      bigint COMMENT '',
    res_1300      bigint COMMENT '',
    res_1400      bigint COMMENT '',
    res_1500      bigint COMMENT '',
    res_1600      bigint COMMENT '',
    res_1700      bigint COMMENT '',
    res_1800      bigint COMMENT '',
    res_1900      bigint COMMENT '',
    res_2000      bigint COMMENT '',
    res_2100      bigint COMMENT '',
    res_2200      bigint COMMENT '',
    res_2300      bigint COMMENT '',
    res_2400      bigint COMMENT '',
    res_2500      bigint COMMENT '',
    res_2600      bigint COMMENT '',
    res_2700      bigint COMMENT '',
    res_2800      bigint COMMENT '',
    res_2900      bigint COMMENT '',
    res_3000      bigint COMMENT '',
    res_3100      bigint COMMENT '',
    res_total     bigint COMMENT '',
    res_rate_100  string COMMENT 'i_100占比',
    res_rate_200  string COMMENT 'i_200占比',
    res_rate_300  string COMMENT 'i_300占比',
    res_rate_400  string COMMENT 'i_400占比',
    res_rate_500  string COMMENT 'i_500占比',
    res_rate_600  string COMMENT 'i_600占比',
    res_rate_700  string COMMENT 'i_700占比',
    res_rate_800  string COMMENT 'i_800占比',
    res_rate_900  string COMMENT 'i_900占比',
    res_rate_1000 string COMMENT 'i_1000占比',
    res_rate_1100 string COMMENT 'i_1100占比',
    res_rate_1200 string COMMENT 'i_1200占比',
    res_rate_1300 string COMMENT 'i_1300占比',
    res_rate_1400 string COMMENT 'i_1400占比',
    res_rate_1500 string COMMENT 'i_1500占比',
    res_rate_1600 string COMMENT 'i_1600占比',
    res_rate_1700 string COMMENT 'i_1700占比',
    res_rate_1800 string COMMENT 'i_1800占比',
    res_rate_1900 string COMMENT 'i_1900占比',
    res_rate_2000 string COMMENT 'i_2000占比',
    res_rate_2100 string COMMENT 'i_2100占比',
    res_rate_2200 string COMMENT 'i_2200占比',
    res_rate_2300 string COMMENT 'i_2300占比',
    res_rate_2400 string COMMENT 'i_2400占比',
    res_rate_2500 string COMMENT 'i_2500占比',
    res_rate_2600 string COMMENT 'i_2600占比',
    res_rate_2700 string COMMENT 'i_2700占比',
    res_rate_2800 string COMMENT 'i_2800占比',
    res_rate_2900 string COMMENT 'i_2900占比',
    res_rate_3000 string COMMENT 'i_3000占比',
    res_rate_3100 string COMMENT 'i_3100占比'
) COMMENT 'app响应速度占比' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_vova_app_response_top;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_app_response_top
(
    event_date    DATE COMMENT 'd_日期',
    region_code   STRING COMMENT 'd_国家',
    resource_type STRING COMMENT 'd_资源类型',
    tp50       DECIMAL(14, 2) COMMENT 'i_asc_tp50',
    asc_tp90       DECIMAL(14, 2) COMMENT 'i_asc_tp90',
    asc_tp95       DECIMAL(14, 2) COMMENT 'i_asc_tp95',
    asc_tp100       DECIMAL(14, 2) COMMENT 'i_asc_tp100',
    asc_avg50       DECIMAL(14, 2) COMMENT 'i_asc_avg50',
    asc_avg90       DECIMAL(14, 2) COMMENT 'i_asc_avg90',
    asc_avg95       DECIMAL(14, 2) COMMENT 'i_asc_avg95',
    avg100       DECIMAL(14, 2) COMMENT 'i_asc_avg100',
    desc_tp90       DECIMAL(14, 2) COMMENT 'i_desc_tp90',
    desc_tp95       DECIMAL(14, 2) COMMENT 'i_desc_tp95',
    desc_tp100       DECIMAL(14, 2) COMMENT 'i_desc_tp100',
    desc_avg50       DECIMAL(14, 2) COMMENT 'i_desc_avg50',
    desc_avg90       DECIMAL(14, 2) COMMENT 'i_desc_avg90',
    desc_avg95       DECIMAL(14, 2) COMMENT 'i_desc_avg95'
) COMMENT 'app响应速度top' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_app_response2;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_app_response2
(
    geo_country    STRING COMMENT 'd_国家',
    resource_type   STRING COMMENT 'd_资源类型',
    res_time DOUBLE COMMENT 'i_响应时间',
    pt       STRING COMMENT 'd_日期'
) COMMENT 'temp'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_app_response;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_app_response
(
    geo_country    STRING COMMENT 'd_国家',
    resource_type   STRING COMMENT 'd_资源类型',
    res_time DOUBLE COMMENT 'i_响应时间',
    pt       STRING COMMENT 'd_日期'
) COMMENT 'temp'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;