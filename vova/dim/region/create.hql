drop table dim.dim_vova_region;
CREATE EXTERNAL TABLE dim.dim_vova_region
(
    datasource            string comment '数据平台',
    region_id             bigint,
    parent_id             bigint,
    country_id            bigint,
    country_code          string,
    country_name          string,
    country_name_cn       string,
    first_region_id       bigint,
    first_region_name     string,
    first_region_name_cn  string,
    second_region_id      bigint,
    second_region_name    string,
    second_region_name_cn string,
    area_id               bigint,
    area_name             string,
    area_code             string
) COMMENT '地区维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

