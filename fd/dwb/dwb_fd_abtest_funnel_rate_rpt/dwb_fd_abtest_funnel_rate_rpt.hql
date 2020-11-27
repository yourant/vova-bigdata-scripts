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



INSERT OVERWRITE TABLE dwb.dwb_fd_abtest_funnel_rate_rpt PARTITION (pt = '${pt}')

select
    nvl(project,'all'),
    nvl(platform_type,'all'),
    nvl(country,'all'),
    nvl(app_version,'all'),
    nvl(abtest_name,'all'),
    nvl(abtest_version,'all'),
    count(distinct session_id),
    count(distinct product_session_id),
    count(distinct add_session_id),
    count(distinct checkout_session_id),
    count(distinct checkout_option_session_id),
    count(distinct purchase_session_id)
from
(
select nvl(project,'other') as project,
       nvl(platform_type,'other') as platform_type,
       nvl(country,'other') as country,
       nvl(app_version,'other') as app_version,
       nvl(substr(abtest_info, 1, instr(abtest_info, '=') - 1),'other')  as abtest_name,
       nvl(substr(abtest_info, instr(abtest_info, '=') + 1),'other')     as abtest_version,
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
           and pt = '${pt}'
     ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info

     )tab1
     group by project,platform_type,country,app_version,abtest_name,abtest_version with cube;
