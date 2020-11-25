set mapred.reduce.tasks=1;
insert overwrite table dwb.dwb_fd_newsletter_snowplow_rpt  partition (pt = '${hiveconf:pt_last}')
select
        year('${hiveconf:pt_last}') as year, /* 年 */
        substr('${hiveconf:pt_last}',1,7) as month, /* 月 */
        concat(substring('${hiveconf:pt_last}',1,4),concat('年|第',concat(weekofyear('${hiveconf:pt_last}'),'周'))) as weekofyear, /* 一年中的第几周 */
        duid_nl.project as project, /* 组织 */
        duid_nl.nl_code_num as nl_code_num, /* nl期数 */
        duid_nl.nl_code as nl_code, /* nl code */
        duid_nl.nl_type as nl_type, /* nl type */
        duid_nl.nl_module as nl_module, /*模块*/
        duid_nl.domain_userid as domain_userid,
        add_event.domain_userid as add_domain_userid, /*add*/
        click_event.domain_userid as goods_click_domian_userid, /*goods_click*/
        impression_event.domain_userid as goods_impression_domain_userid, /*goods_impression*/
        if(oa.order_id is not null,orders.sp_duid,null) as order_domain_userid, /*下单成功*/
        if(oa.order_id is not null and duid_nl.rn = 1,orders.order_id,null) as order_id, /*订单id*/
        if(oa.order_id is not null and duid_nl.rn = 1,orders.gmv,null) as goods_amount /*销售金额*/
from (
        select
            fms.project,
            fms.domain_userid,
            fms.nl_code_num,
            fms.nl_code,
            fms.nl_type,
            fms.nl_module,
            fms.rn
    from (
            select
                    project,
                    domain_userid,
                    split(regexp_extract(page_url, 'utm_campaign=([A-Za-z0-9_]+)', 0), '_')[2] as nl_code_num,
                    substr(regexp_extract(page_url, 'utm_campaign=([A-Za-z0-9_]+)', 0), 25)    as nl_code,
                    regexp_extract(page_url, 'utm_source=([A-Za-z0-9]+)', 0)                   as utm_source,
                    regexp_extract(page_url, 'nl_type=([A-Za-z0-9]+)', 0)                      as nl_type_full,
                    split(regexp_extract(page_url, 'nl_type=([A-Za-z0-9]+)', 0), '=')[1]       as nl_type,
                    regexp_extract(page_url, 'nl_module=([A-Za-z0-9]+)', 0)                    as nl_module_full,
                    split(regexp_extract(page_url, 'nl_module=([A-Za-z0-9]+)', 0), '=')[1]     as nl_module,
                    row_number() over (partition by project,domain_userid,page_url order by derived_ts desc) as rn
            from ods.ods_fd_snowplow_view_event
            where pt = '${hiveconf:pt_last}'
            and event_name = 'page_view'
            and lower(regexp_extract(page_url, 'utm_source=[A-Za-z0-9_]+', 0)) = 'utm_source=newsletter'
            and lower(page_url) like '%nl_type%'
            and lower(page_url) like '%nl_module%'
            and split(regexp_extract(page_url, 'nl_type=([A-Za-z0-9]+)', 0), '=')[1] is not null
    ) fms

) duid_nl
left join (
    select domain_userid
    from ods.ods_fd_snowplow_ecommerce_event
    where pt = '${hiveconf:pt_last}'
      and event_name = 'add'
    group by domain_userid
) add_event on add_event.domain_userid = duid_nl.domain_userid
left join (
    select domain_userid
    from ods.ods_fd_snowplow_goods_event
    where pt = '${hiveconf:pt_last}'
    and event_name = 'goods_click'
    group by domain_userid
) click_event on click_event.domain_userid = duid_nl.domain_userid
left join (
    select domain_userid
    from ods.ods_fd_snowplow_goods_event
    where pt = '${hiveconf:pt_last}'
    and event_name = 'goods_impression'
    group by domain_userid
) impression_event  on impression_event.domain_userid = duid_nl.domain_userid
left join (
    select
        sp_duid,
        order_id,
        sum(shop_price * goods_number) gmv
   from dwd.dwd_fd_order_goods
   where (
      date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) >= '${hiveconf:pt_last}'
      or date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) >= '${hiveconf:pt_last}'
    )
   and sp_duid is not null
   group by sp_duid,order_id
)orders on orders.sp_duid = duid_nl.domain_userid
left join (
  select distinct t1.order_sn,t1.country,t1.nl_code,t1.order_id,t1.region_code
  from (
    select
        t0.order_sn as order_sn,
        t0.country as country,
        substr(t0.campaign,12) as nl_code,
        t0.order_id as order_id,
        t1.region_code as region_code,
        Row_Number() OVER (partition by oa_id ORDER BY t0.last_update_time desc) rank
    from ods_fd_ar.ods_fd_order_analytics t0
    left join dim.dim_fd_region t1 on t1.region_name_en = t0.country
    where (split(t0.campaign, '_')[0] = 'NewsLetter' or t0.ga_channel = 'EDM')
    and substr(t0.campaign,12) !=''
  )t1 where rank = 1

) oa on orders.order_id = oa.order_id;
