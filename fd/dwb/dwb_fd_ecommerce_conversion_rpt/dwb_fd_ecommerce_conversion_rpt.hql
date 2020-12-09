insert overwrite table dwb.dwb_fd_ecommerce_conversion_rpt partition (pt='${pt}')
SELECT  /*+ REPARTITION(1) */
  nvl(project,'all') as project,
  nvl(country,'all') as country,
  nvl(platform_type,'all') as platform_type,
  nvl(ga_channel,'all') as ga_channel,
  count(distinct add_session_id)             as add_uv,
  count(distinct checkout_session_id)        as checkout_uv,
  count(distinct session_id)                 as all_uv,
  count(distinct checkout_option_session_id) as checkout_option_uv,
  count(distinct purchase_session_id)        as purchase_uv,
  count(distinct product_view_session_id)    as product_view_uv,
  count(distinct order_id) as order_num
from(


select

        nvl('project','NALL') as project,
        nvl('country','NALL') as country,
        nvl('platform_type','NALL') as platform_type,
        if(sc.ga_channel is null or sc.ga_channel = '', 'Others', sc.ga_channel)          as ga_channel,
        fms.session_id as session_id,
        fms.product_view_session_id as product_view_session_id,
        fms.add_session_id as add_session_id,
        fms.checkout_session_id as checkout_session_id,
        fms.checkout_option_session_id as checkout_option_session_id,
        fms.purchase_session_id as purchase_session_id,
        null as order_id
from
    (SELECT
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
      left join (select session_id,collect_set(ga_channel)[0] as ga_channel from dwd.dwd_fd_session_channel
          where ga_channel is not null and  pt between date_add('${pt}',-3) and date_add('${pt}',1)
          group by session_id
        )sc on fms.session_id=sc.session_id


      union all

      SELECT
        nvl(project_name,'NALL') as project,
        nvl(country_code,'NALL') as country,
        nvl(platform_type,'NALL') as platform_type,
        if(sc.ga_channel is null or sc.ga_channel = '', 'Others', sc.ga_channel)          as ga_channel,
        null ,
        null,
        null,
        null,
        null,
        null,
        oi.order_id as order_id
      from
      (select project_name,platform_type,country_code,order_id
      from dwd.dwd_fd_order_info
      where  pay_time is not null  and pay_status=2
      and date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}'
      and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
      )oi
      left join (select order_id,sp_session_id
          from ods_fd_vb.ods_fd_order_marketing_data
          group by order_id,sp_session_id
          ) om on om.order_id = oi.order_id

      left join (select session_id,collect_set(ga_channel)[0] as ga_channel
                    from dwd.dwd_fd_session_channel
                    where ga_channel is not  null and  pt between date_add('${pt}',-3) and date_add('${pt}',1)
                    group by session_id
                    )sc on om.sp_session_id=sc.session_id

)result group by   project,
                   country,
                   platform_type,
                   ga_channel with cube;
