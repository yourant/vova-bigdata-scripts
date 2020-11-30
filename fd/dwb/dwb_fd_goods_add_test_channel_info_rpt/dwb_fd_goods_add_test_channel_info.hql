
INSERT overwrite table dwb.dwb_fd_goods_add_test_channel_info PARTITION (pt = '${pt}')

select
    nvl(project_name,'all'),
    nvl(platform,'all'),
    nvl(country,'all'),
    nvl(cat_name,'all'),
    nvl(ga_channel,'all'),
    count(distinct add_session_id),
    count(distinct view_session_id),
    count(distinct order_id),
    sum(goods_amount),
    count(distinct goods_test_goods_id),
    count(distinct success_goods_test_goods_id),
    count(distinct success_order_id),
    sum(success_goods_amount)

from(
select
        nvl(t.project_name,'NALL') as project,
        nvl(t.platform,'NALL') as platform,
        nvl(t.country,'NALL') as country,
        nvl(t.cat_id,'NALL') as cat_id,
        nvl(t.cat_name,'NALL') as cat_name,
       nvl(s.ga_channel,'NALL') as ga_channel,
       t.add_session_id,
       t.view_session_id,
       null as order_id,
       null as goods_amount,
       null as goods_test_goods_id,
       null as success_goods_test_goods_id,
       null as success_order_id,
       null as success_goods_amount
from (
select 
        project_name,
        platform,
        country,
        cat_id,
        cat_name,
        if(add_session_id is null,view_session_id, add_session_id) as session_id,
        view_session_id,
        add_session_id,
        pt
    from dwd.dwd_fd_goods_add_info
    where pt <= '${pt}'
    and cat_name is not null
) t
    left join
(
    select session_id, ga_channel
    from dwd.dwd_fd_session_channel
    where pt = '${pt}'
) s on t.session_id = s.session_id

union all
select nvl(project_name,'NALL') as project_name,
       case
           when platform = 'mob' then 'APP'
           when platform = 'web' and platform_type = 'pc_web' then 'PC'
           when platform = 'others' and platform_type = 'others' then 'others'
           else 'H5' end as platform,
       nvl(country_code,'NALL') as country,
       nvl(cat_id,'NALL') as cat_id,
       nvl(cat_name,'NALL') as cat_name,
       nvl(ga_channel,'NALL') as ga_channel,
       null                      as add_session_id,
       null                      as view_session_id,
       order_id,
       goods_number * shop_price as goods_amount,
       null                      as goods_test_goods_id,
       null                      as success_goods_test_goods_id,
       null                      as success_order_id,
       null                      as success_goods_amount
from dwd.dwd_fd_order_channel_analytics
where  date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
  and pay_status = 2

union all
select nvl(a.project_name,'NALL') as project_name,
       nvl(a.platform_name,'NALL')    as platform,
       nvl(a.country_code,'NALL') as country,
       nvl(a.cat_id,'NALL') as cat_id,
       nvl(a.cat_name,'NALL') as cat_name,
       nvl(b.ga_channel,'NALL') as ga_channel,
       null               as add_session_id,
       null               as view_session_id,
       null               as order_id,
       null               as goods_amount,
       a.virtual_goods_id as goods_test_goods_id,
       if(a.result = 1,a.virtual_goods_id,null)  as success_goods_test_goods_id,
       null               as success_order_id,
       null               as success_goods_amount
from (select project_name,platform_name,country_code,cat_id,cat_name,virtual_goods_id,result from dwd.dwd_fd_finished_goods_test where to_date(finish_time) = '${pt}' ) a
left join (select virtual_goods_id,project_name,ga_channel from dwd.dwd_fd_order_channel_analytics where to_date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}') b on a.project_name = b.project_name and a.virtual_goods_id = b.virtual_goods_id

union all
select nvl(a.project_name,'NALL') as project_name,
       nvl(a.platform_type,'NALL') as platform,
       nvl(a.country_code,'NALL') as country,
       nvl(a.cat_id,'NALL') as cat_id,
       nvl(a.cat_name,'NALL') as cat_name,
       nvl(b.ga_channel,'NALL') as ga_channel,
       null            as add_session_id,
       null            as view_session_id,
       null            as order_id,
       null            as goods_amount,
       null            as goods_test_goods_id,
       null            as success_goods_test_goods_id,
       b.success_order_id,
       b.success_goods_amount
from (
         select project_name,
                platform_name as platform_type,
                country_code,
                cat_id,
                cat_name,
                virtual_goods_id,
                create_time
         from dwd.dwd_fd_finished_goods_test
         where to_date(finish_time) <= '${pt}'
           and result = 1
     ) a
         inner join
     (
         select project_name,
                case
                    when platform = 'mob' then 'APP'
                    when platform = 'web' and platform_type = 'pc_web' then 'PC'
                    when platform = 'others' and platform_type = 'others' then 'others '
                    else 'H5' end                                as platform,
                country_code,
                cat_id,
                cat_name,
                ga_channel,
                order_id                                         as success_order_id,
                cast(virtual_goods_id as int)                    as virtual_goods_id,
                from_unixtime(order_time, 'yyyy-MM-dd HH:mm:ss') as order_time,
                (goods_number * shop_price)                      as success_goods_amount
         from dwd.dwd_fd_order_channel_analytics
           where  date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
           and pay_status = 2
     ) b on b.project_name = a.project_name
         and b.platform = a.platform_type and b.country_code = a.country_code and b.cat_id = a.cat_id and a.virtual_goods_id = cast(b.virtual_goods_id as int)
     where a.create_time <= b.order_time

     )tab1 group by      project_name,
                            platform,
                            country,
                            cat_name,
                            ga_channel  with cube;


