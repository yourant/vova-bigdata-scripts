#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last

#脚本路径
#shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_abtest_funnel_rate_rpt"

#hive -hiveconf pt=$pt  -f ${shell_path}/dwb_fd_abtest_funnel_rate_rpt.hql


sql="

INSERT OVERWRITE TABLE dwb.dwb_fd_abtest_funnel_rate_rpt PARTITION (pt = '$pt')

select   /*+ REPARTITION(1) */
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
    select
           project,
           platform_type,
            country,
            app_version,
           if(abtest_name regexp 'amp',substr(abtest_name,5),abtest_name) as abtest_name,
           abtest_version,
           session_id,
           product_session_id,
           add_session_id,
           checkout_session_id,
           checkout_option_session_id,
           purchase_session_id

    from

    (

    select nvl(project,'NALL') as project,
           nvl(platform_type,'NALL') as platform_type,
           nvl(country,'NALL') as country,
           nvl(app_version,'NALL') as app_version,
           substr(abtest_info, 1, instr(abtest_info, '=') - 1)  as abtest_name,
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
               where abtest != ''
               and abtest != '-'
               and pt = '$pt'
               and session_id is not null
         ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info

     )tab1

)result
     group by project,platform_type,country,app_version,abtest_name,abtest_version with cube;
"

spark-sql \
--conf "spark.app.name=dwb_fd_abtest_funnel_rate_rpt_yjzhang"   \
-d pt=$pt \
-e "$sql"
