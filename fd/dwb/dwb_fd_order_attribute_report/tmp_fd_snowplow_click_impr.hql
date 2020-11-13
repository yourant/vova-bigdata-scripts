CREATE TABLE IF NOT EXISTS tmp.tmp_fd_snowplow_click_impr (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`domain_userid` string COMMENT '设备id',
`event_name` string COMMENT '只有(goods click和impression事件)',
`session_id` string COMMENT 'session_id',
`derived_tstamp` bigint COMMENT '时间'
) COMMENT '打点数据中的goods click事件和impression事件'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


insert overwrite table tmp.tmp_fd_snowplow_click_impr partition (dt = '${hiveconf:dt}')
SELECT
    project AS project_name, /* 组织 */
    upper(country) as country, /* 国家 */
    platform_type, /* 平台 */
    page_code, /* page_code */
    goods_event_struct.list_type AS list_type, /* list_type */
    domain_userid,
    event_name,
    session_id,
    derived_ts as derived_tstamp
FROM ods.ods_fd_snowplow_goods_event
WHERE event_name in ('goods_click', 'goods_impression')
AND dt = '${hiveconf:dt}'
AND project is not null
AND project != ''
AND length(country) = 2
AND platform_type is not null
AND platform_type != ''
AND page_code != '404'
AND page_code != ''
AND goods_event_struct.list_type is not null
AND goods_event_struct.list_type != 'null'
AND goods_event_struct.list_type != 'NULL'
distribute by pmod(cast(rand()*1000 as int),120);

