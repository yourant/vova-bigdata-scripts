drop table dim.dim_vova_languages;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_languages
(
    datasource     string comment '数据平台',
    languages_id   bigint comment '语言id',
    languages_name string comment '语言名称',
    languages_code string comment '语言code'
) COMMENT '语言维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;