INSERT OVERWRITE TABLE dwb.dwb_fd_abtest_funnel_rate_rpt PARTITION (pt = '${pt}')

select   /*+ REPARTITION(2) */
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
select project,
       platform_type,
       country,
       app_version,
       substr(abtest_info, 1, instr(abtest_info, '=') - 1) as abtest_name,
       substr(abtest_info, instr(abtest_info, '=') + 1)   as abtest_version,
       session_id,
       IF(event_name in('page_view', 'screen_view') and page_code = 'product', session_id, null)   as product_session_id,
       IF(event_name = 'add', session_id, null)               as add_session_id,
       IF(event_name = 'checkout', session_id, null)          as checkout_session_id,
       IF(event_name = 'checkout_option', session_id, null)   as checkout_option_session_id,
       IF(event_name = 'purchase', session_id, null)          as purchase_session_id
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

     )tab1 where project is not null
         and platform_type is not null
         and country is not null
          and app_version is not null
           and abtest_name is not null
           and abtest_version is not null
     group by project,platform_type,country,app_version,abtest_name,abtest_version with cube;
