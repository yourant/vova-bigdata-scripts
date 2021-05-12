DROP TABLE IF EXISTS mlb.mlb_vova_user_query_freqency_d;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_user_query_freqency_d
(
    key_words         STRING,
    translation_query STRING,
    pv                BIGINT,
    uv                BIGINT,
    pt_uv             BIGINT,
    session_uv        BIGINT
) COMMENT '搜索词频率统计表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/base/mlb_vova_user_query_freqency_d'
;