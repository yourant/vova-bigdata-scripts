CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_feed_shopping_performance_report_arc (
    `fspr_id` bigint  COMMENT 'auto increase feed_shopping_performance_report id',
    `project` string COMMENT 'project name',
    `goods_id` int  COMMENT 'real goods id',
    `country` string COMMENT 'country code 2 digit',
    `language` string COMMENT 'language code 2 digit',
    `platform` string COMMENT 'campaign_name contains MB or not',
    `clicks` int ,
    `impressions` int ,
    `conversions` double,
    `ctr` double,
    `conversion_rate` double,
    `insert_date` string 
) COMMENT ''
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

INSERT overwrite table ods_fd_vb.ods_fd_feed_shopping_performance_report_arc PARTITION (dt='${hiveconf:dt}')
select 
    fspr_id,
    project,
    goods_id,
    country,
    language,
    platform,
    clicks,
    impressions,
    conversions,
    ctr,
    conversion_rate,
    insert_date

from (
    select 
            fspr_id,
            project,
            goods_id,
            country,
            language,
            platform,
            clicks,
            impressions,
            conversions,
            ctr,
            conversion_rate,
            insert_date,
            row_number () OVER (PARTITION BY fspr_id ORDER BY dt DESC) AS rank
    from(
        select
            '2020-01-01' as dt,
            fspr_id,
            project,
            goods_id,
            country,
            language,
            platform,
            clicks,
            impressions,
            conversions,
            ctr,
            conversion_rate,
            insert_date
        from tmp.tmp_fd_feed_shopping_performance_report_full

        UNION

        select
            '${hiveconf:dt}' as dt,
            fspr_id,
            project,
            goods_id,
            country,
            language,
            platform,
            clicks,
            impressions,
            conversions,
            ctr,
            conversion_rate,
            insert_date
        from ods_fd_vb.ods_fd_feed_shopping_performance_report_inc where dt = '${hiveconf:dt}'
    )inc
) arc where arc.rank =1;
