
INSERT OVERWRITE TABLE dwb.dwb_fd_rpt_ecommerce_funnel_report PARTITION (pt = '${pt}')
select oi.project_name                                                                            as project
, oi.country_code                                                                            as country
, oi.platform_type                                                                           as platform_type
, if(fdpsc.ga_channel is null or fdpsc.ga_channel = '', 'Others', fdpsc.ga_channel)          as ga_channel
, nvl(fms.is_new_user, 'old')                                                                as is_new_user
, if(fdpsc.mkt_source is null or fdpsc.mkt_source = '', 'Others', fdpsc.mkt_source)          as mkt_source
, if(fdpsc.mkt_medium is null or fdpsc.mkt_medium = '', 'Others', fdpsc.mkt_medium)          as mkt_medium
, if(fdpsc.campaign_name is null or fdpsc.campaign_name = '', 'Others', fdpsc.campaign_name) as campaign_name
, om.sp_session_id                                                                           as session_id
, NULL                                                                                       as product_session_id
, NULL                                                                                       as add_session_id
, NULL                                                                                       as checkout_session_id
, NULL                                                                                       as checkout_option_session_id
, NULL                                                                                       as purchase_session_id
, oi.order_id                                                                                as order_id
, oi.goods_amount                                                                            as goods_amount
, oi.bonus                                                                                   as bonus
, oi.shipping_fee                                                                            as shipping_fee
from (
select project_name,country_code,platform_type,order_id,goods_amount,bonus,shipping_fee
from dwd.dwd_fd_order_info
where (date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) = '${pt}'
or date(from_unixtime(pay_time,'yyyy-MM-dd hh:mm:ss')) = '${pt}'
)
and pay_status = 2
and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
) oi
left join (select order_id,sp_session_id from ods_fd_vb.ods_fd_order_marketing_data group by order_id,sp_session_id) om on om.order_id = oi.order_id
left join (
select soe.session_id,collect_set(soe.is_new_user)[0] as is_new_user
from (
select
session_id,
case
when platform = 'web' and session_idx = 1 then 'new'
when platform = 'web' and session_idx > 1 then 'old'
when platform = 'mob' and session_idx = 1 then 'new'
when platform = 'mob' and session_idx > 1 then 'old'
end as is_new_user
from ods_fd_snowplow.ods_fd_snowplow_view_event
where pt BETWEEN date_sub('${pt}', 5) AND '${pt}' and event_name = 'page_view'
) soe group by soe.session_id
) fms on om.sp_session_id = fms.session_id
left join (
select
session_id,
collect_set(mkt_source)[0]    as mkt_source,
collect_set(mkt_medium)[0]    as mkt_medium,
collect_set(campaign_name)[0] as campaign_name,
collect_set(ga_channel)[0]    as ga_channel
from dwd.dwd_fd_session_channel
where pt BETWEEN date_sub('${pt}', 10) AND date_add('${pt}', 1)
group by session_id
) fdpsc on fdpsc.session_id = om.sp_session_id;


INSERT INTO TABLE dwb.dwb_fd_rpt_ecommerce_funnel_report PARTITION (pt = '${pt}')
select fms.project
, fms.country
, fms.platform_type
, if(fdpsc.ga_channel is null or fdpsc.ga_channel = '', 'Others', fdpsc.ga_channel)          as ga_channel
, nvl(fms.is_new_user, 'old')                                                                as is_new_user
, if(fdpsc.mkt_source is null or fdpsc.mkt_source = '', 'Others', fdpsc.mkt_source)          as mkt_source
, if(fdpsc.mkt_medium is null or fdpsc.mkt_medium = '', 'Others', fdpsc.mkt_medium)          as mkt_medium
, if(fdpsc.campaign_name is null or fdpsc.campaign_name = '', 'Others', fdpsc.campaign_name) as campaign_name
, fms.session_id
, fms.product_session_id
, fms.add_session_id
, fms.checkout_session_id
, fms.checkout_option_session_id
, fms.purchase_session_id
, NULL                                                                                       as order_id
, NULL                                                                                       as goods_amount
, NULL                                                                                       as bonus
, NULL                                                                                       as shipping_fee
from (
select project
, country
, platform_type
, app_version
, case
when platform = 'web' and session_idx = 1 then 'new'
when platform = 'web' and session_idx > 1 then 'old'
when platform = 'mob' and session_idx = 1 then 'new'
when platform = 'mob' and session_idx > 1 then 'old'
end as is_new_user
, session_id
, if(event_name in ('page_view', 'screen_view') and page_code = 'product', session_id,NULL)  as product_session_id
, if(event_name = 'add', session_id, NULL)             as add_session_id
, if(event_name = 'checkout', session_id, NULL)        as checkout_session_id
, if(event_name = 'checkout_option', session_id, NULL) as checkout_option_session_id
, if(event_name = 'purchase', session_id, NULL)        as purchase_session_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}'
and event_name in ('page_view', 'screen_view', 'add', 'checkout', 'checkout_option', 'purchase')
) fms
left join (
select
session_id,
collect_set(mkt_source)[0]    as mkt_source,
collect_set(mkt_medium)[0]    as mkt_medium,
collect_set(campaign_name)[0] as campaign_name,
collect_set(ga_channel)[0]    as ga_channel
from dwd.dwd_fd_session_channel
where pt BETWEEN date_sub('${pt}', 10) AND date_add('${pt}', 1)
group by session_id
) fdpsc on fdpsc.session_id = fms.session_id;
