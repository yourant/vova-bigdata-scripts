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
select nvl(project,'NALL') as project,
       nvl(platform_type,'NALL') as platform_type,
       nvl(country,'NALL') as country,
       nvl(app_version,'NALL') as app_version,
       nvl(substr(abtest_info, 1, instr(abtest_info, '=') - 1),'NALL') as abtest_name,
       nvl(substr(abtest_info, instr(abtest_info, '=') + 1),'NALL')    as abtest_version,
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

     )tab1
     group by project,platform_type,country,app_version,abtest_name,abtest_version with cube;
