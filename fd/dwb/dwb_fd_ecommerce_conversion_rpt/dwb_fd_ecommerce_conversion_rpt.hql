insert overwrite table dwb.dwb_fd_ecommerce_conversion_rpt partition (pt='${pt}')
SELECT  /*+ REPARTITION(1) */
    session_table.project,
    session_table.country,
    session_table.platform_type,
    session_table.ga_channel,
    add_uv,
    checkout_uv,
    all_uv,
    checkout_option_uv,
    purchase_uv,
    product_view_uv,
    orders
from
(
select
  project,
  country,
  platform_type,
  ga_channel,
  count(distinct fms.add_session_id)             as add_uv,
  count(distinct fms.checkout_session_id)        as checkout_uv,
  count(distinct fms.session_id)                 as all_uv,
  count(distinct fms.checkout_option_session_id) as checkout_option_uv,
  count(distinct fms.purchase_session_id)        as purchase_uv,
  count(distinct fms.product_view_session_id)    as product_view_uv
from(
  SELECT
    project,
    country,
    platform_type,
    session_id,
    if(event_name in ('page_view', 'screen_view') and page_code = 'product', session_id,NULL)  as product_view_session_id,
    if(event_name == 'add', session_id, NULL)             as add_session_id,
    if(event_name == 'checkout', session_id, NULL)        as checkout_session_id,
    if(event_name == 'checkout_option', session_id, NULL) as checkout_option_session_id,
    if(event_name == 'purchase', session_id, NULL)        as purchase_session_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where ((pt='${pt_last}' and hour >='16' and hour<='23') or (pt='${pt}' and hour >= '00' and hour<='15'))
and event_name in('page_view','screen_view','add','checkout','checkout_option','purchase')
)fms
  left join (select session_id,collect_set(ga_channel)[0] as ga_channel from dwd.dwd_fd_session_channel where ga_channel is not null and  pt between date_add('${pt}',-3) and date_add('${pt}',1)  group by session_id)sc on fms.session_id=sc.session_id
  group by   project,country,platform_type,ga_channel
  
)session_table

left JOIN
(
  SELECT
    project_name as project,
    platform_type,
    country_code as country,
    ga_channel,
    count(distinct oi.order_id) as orders
  from 
  (select project_name,platform_type,country_code,order_id
  from dwd.dwd_fd_order_info 
  where  pay_time is not null and date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}'
  and pay_status=2   
  and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
  )oi
  left join (select order_id,sp_session_id from ods_fd_vb.ods_fd_order_marketing_data group by order_id,sp_session_id) om on om.order_id = oi.order_id
  left join (select session_id,collect_set(ga_channel)[0] as ga_channel from dwd.dwd_fd_session_channel where ga_channel is not  null and  pt between date_add('${pt}',-3) and date_add('${pt}',1)   group by session_id)sc on om.sp_session_id=sc.session_id
  group by project_name,platform_type,country_code,ga_channel
)order_table

on  session_table.country = order_table.country
and session_table.project = order_table.project
and session_table.platform_type = order_table.platform_type
and session_table.ga_channel = order_table.ga_channel;
