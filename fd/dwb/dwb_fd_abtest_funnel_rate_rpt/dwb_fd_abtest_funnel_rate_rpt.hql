CREATE TABLE IF NOT EXISTS dwb.dwb_fd_abtest_funnel_rate_rpt
(
    project                    string,
    platform_type              string,
    country                    string,
    app_version                string,
    abtest_name                string,
    abtest_version             string,
    uv                         bigint,
    product_uv                 bigint,
    add_uv                     bigint,
    checkout_uv                bigint,
    checkout_option_uv         bigint,
    purchase_uv                bigint
) comment'utc时间每天的abtest打点转化明细表'
    PARTITIONED BY ( pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");

set hive.new.job.grouping.set.cardinality=128;
set mapred.reduce.tasks = 1;

INSERT OVERWRITE TABLE dwb.dwb_fd_abtest_funnel_rate_rpt PARTITION (pt = '${hiveconf:pt}')
select project,
       platform_type,
       country,
       app_version,
       substr(abtest_info, 1, instr(abtest_info, '=') - 1)  as abtest_name,
       substr(abtest_info, instr(abtest_info, '=') + 1)     as abtest_version,
       session_id,
       IF(event_name in('page_view', 'screen_view') and page_code = 'product', session_id, '')   as product_session_id,
       IF(event_name = 'add', session_id, '')               as add_session_id,
       IF(event_name = 'checkout', session_id, '')          as checkout_session_id,
       IF(event_name = 'checkout_option', session_id, '')   as checkout_option_session_id,
       IF(event_name = 'purchase', session_id, '')          as purchase_session_id
from (
         select project,
                platform_type,
                event_name,
                page_code,
                referrer_page_code,
                country,
                app_version,
                session_id,
                abtest
         from ods_fd_snowplow.ods_fd_snowplow_all_event
         where event_name in ('page_view', 'screen_view', 'add', 'checkout', 'checkout_option', 'purchase')
           and abtest != ''
           and abtest != '-'
           and pt = '${hiveconf:pt}'
     ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info;

