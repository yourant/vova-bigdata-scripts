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
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_prc_abtest_funnel_rpt"

#hive -hiveconf pt=$pt  -f ${shell_path}/dwb_fd_rpt_prc_abtest_funnel.hql


sql="
INSERT OVERWRITE TABLE dwb.dwb_fd_prc_abtest_funnel_rpt PARTITION (pt = '$pt')
select    /*+ REPARTITION(1) */
           nvl(project,'all'),
           nvl(platform_type,'all'),
           nvl(country,'all'),
           nvl(app_version,'all'),
           nvl(abtest_name,'all'),
           nvl(abtest_version,'all'),
           count(distinct session_id),
           count(distinct homepage_session_id),
           count(distinct list_session_id),
           count(distinct product_session_id),
           count(distinct cart_session_id),
           count(distinct add_session_id),
           count(distinct remove_session_id),
           count(distinct checkout_session_id),
           count(distinct checkout_option_session_id),
           count(distinct purchase_session_id),
           count(distinct checkout_page_session_id),
           count(distinct order_id),
           sum(goods_amount),
           sum(bonus),
           sum(shipping_fee)
from(


select
     project,
     platform_type,
     country,
     app_version,
     if(abtest_name regexp 'amp;',substr(abtest_name,5),abtest_name) as abtest_name,
     abtest_version,
     session_id,
     homepage_session_id,
     list_session_id,
     product_session_id,
     cart_session_id,
     add_session_id,
     remove_session_id,
     checkout_session_id,
     checkout_option_session_id,
     purchase_session_id,
     checkout_page_session_id,
     order_id,
     goods_amount,
     bonus,
     shipping_fee

from(
    select project,
           platform_type,
           country,
           app_version,
           substr(abtest_info, 1, instr(abtest_info, '=') - 1)  as abtest_name,
           substr(abtest_info, instr(abtest_info, '=') + 1)   as abtest_version,
           session_id,
           IF(page_code = 'homepage', session_id, null)           as homepage_session_id,
           IF(page_code in ('list', 'landing'), session_id, null) as list_session_id,
           IF(page_code = 'product', session_id, null)            as product_session_id,
           IF(page_code = 'cart', session_id, null)               as cart_session_id,
           IF(event_name = 'add', session_id, null)               as add_session_id,
           IF(event_name = 'remove', session_id, null)            as remove_session_id,
           IF(event_name = 'checkout', session_id, null)          as checkout_session_id,
           IF(event_name = 'checkout_option', session_id, null)   as checkout_option_session_id,
           IF(event_name = 'purchase', session_id, null)          as purchase_session_id,
           IF(((page_code = 'addressedit' and referrer_page_code in ('product', 'cart')) or (page_code = 'checkout')),
              session_id, null)                                   as checkout_page_session_id,
           NULL                                                 as order_id,
           0.0                                                  as goods_amount,
           0.0                                                  as bonus,
           0.0                                                  as shipping_fee
    from (
             select nvl(project,'NALL') as project,
                    nvl(platform_type,'NALL') as platform_type,
                    event_name,
                    page_code,
                    referrer_page_code,
                    nvl(country,'NALL') as country,
                    nvl(app_version,'NALL') as app_version,
                    session_id,
                    abtest
             from ods_fd_snowplow.ods_fd_snowplow_all_event
             where event_name in ('page_view', 'screen_view', 'add', 'remove', 'checkout', 'checkout_option', 'purchase')
               and abtest != ''
               and abtest != '-'
               and abtest is not null
               and session_id is not null
               and ((pt='$pt_last' and hour >='16' and hour<='23')
                 or (pt='$pt' and hour >= '00' and hour<='15'))

         ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info
         )abtest_session

    union all

    select fboi.project_name                                 as project,
           fboi.platform_type                               as platform_type,
           fboi.country_code                                as country,
           fboi.version                                   as app_version,
           substr(abtest_info, 1, instr(abtest_info, '=') - 1) as abtest_name,
           substr(abtest_info, instr(abtest_info, '=') + 1)   as abtest_version,
           NULL                                                as session_id,
           NULL                                                as homepage_session_id,
           NULL                                                as list_session_id,
           NULL                                                as product_session_id,
           NULL                                                as cart_session_id,
           NULL                                                as add_session_id,
           NULL                                                as remove_session_id,
           NULL                                                as checkout_session_id,
           NULL                                                as checkout_option_session_id,
           NULL                                                as purchase_session_id,
           NULL                                                as checkout_page_session_id,
           cast(fboi.order_id as bigint),
           fboi.goods_amount,
           fboi.bonus,
           fboi.shipping_fee

    from (
        select oi.pay_time,
                oi.order_id,
                oi.project_name,
                oi.goods_amount,
                oi.bonus,
                oi.shipping_fee,
                oi.platform_type,
                oi.country_code,
                oi.version,
                oe.ext_value
        from (
            select
                order_id,
                nvl(project_name,'NALL') as project_name,
                goods_amount,
                pay_time,
                bonus,
                shipping_fee,
                nvl(platform_type,'NALL') as platform_type,
                nvl(country_code,'NALL') as country_code,
                nvl(version,'NALL') as version
            from dwd.dwd_fd_order_info
            where   pay_time is not null
            and date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '$pt'
            and pay_status = 2
            and order_id is not null
            and email NOT REGEXP 'tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com'
        )oi
        left join (select order_id,ext_value from ods_fd_vb.ods_fd_order_extension where ext_name = 'abtestInfo' and ext_value is not null) oe on oi.order_id = oe.order_id

    ) fboi LATERAL VIEW OUTER explode(split(fboi.ext_value, '&')) fboi as abtest_info

)tab1

group by              project,
                      platform_type,
                      country,
                      app_version,
                      abtest_name,
                      abtest_version
                       with cube;
"

spark-sql \
--conf "spark.app.name=dwb_fd_rpt_prc_abtest_funnel_yjzhang"   \
-d pt=$pt \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi

echo " prc abtest rpt  is finished !"