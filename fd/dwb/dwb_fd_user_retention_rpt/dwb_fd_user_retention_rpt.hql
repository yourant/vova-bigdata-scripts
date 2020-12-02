-- 关闭自动开启mapjoin转换
set hive.auto.convert.join=false;

insert overwrite table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
select
       '${pt}',
       nvl(t1.platform_type,'all'),
       nvl(t1.country,'all'),
       0.0,
       0,
       0,
       0,
       0,

       count(distinct t1.domain_userid)                                         as access_1ago,
       count(distinct if(t2.domain_userid is not null, t1.domain_userid, null)) as access_both1,

       0,
       0,
       0,
       0,
      count(distinct if(t1.is_new_user = 'new', t1.domain_userid, null))       as access_1ago_new,
       count(distinct if(t1.is_new_user = 'new' and t2.domain_userid is not null, t1.domain_userid, null))     as access_both1_new,


       0,
       0,
       0,
       0,
       0
from (select distinct domain_userid,        case
when session_idx=1 then 'new'
when session_idx>1 then 'old'
end  as is_new_user, platform_type, country
      from ods_fd_snowplow.ods_fd_snowplow_all_event
      where pt = date_add('${pt}', -1)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid, case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube;

insert into table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
select
       '${pt}',
       nvl(t1.platform_type,'all'),
       nvl(t1.country,'all'),
       0.0,
       0,
       0,
       0,
       0,

       0,
       0,
        count(distinct t1.domain_userid)                                         as access_7ago,
       count(distinct if(t2.domain_userid is not null, t1.domain_userid, null)) as access_both7,
       0,
       0,
       0,
       0,

      count(distinct if(t1.is_new_user = 'new', t1.domain_userid, null))       as access_7ago_new,
       count(distinct if(t1.is_new_user = 'new' and t2.domain_userid is not null, t1.domain_userid, null))     as access_both7_new,
      0,
       0,
       0

from (select distinct domain_userid,  case
when session_idx=1 then 'new'
when session_idx>1 then 'old'
end  as is_new_user, platform_type, country
      from ods_fd_snowplow.ods_fd_snowplow_all_event
      where pt = date_add('${pt}', -7)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid,  case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube;


insert into table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
select
       '${pt}',
       nvl(t1.platform_type,'all'),
       nvl(t1.country,'all'),
       0.0,
       0,
       0,
       0,
       0,

       0,
       0,
       0,
       0,
        count(distinct t1.domain_userid)                                         as access_28ago,
       count(distinct if(t2.domain_userid is not null, t1.domain_userid, null)) as access_both28,
       0,
       0,
        0,
       0,
      count(distinct if(t1.is_new_user = 'new', t1.domain_userid, null))       as access_28ago_new,
       count(distinct if(t1.is_new_user = 'new' and t2.domain_userid is not null, t1.domain_userid, null))     as access_both28_new,
       0

from (select distinct domain_userid,  case
when session_idx=1 then 'new'
when session_idx>1 then 'old'
end  as is_new_user, platform_type, country
      from ods_fd_snowplow.ods_fd_snowplow_all_event
      where pt = date_add('${pt}', -28)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid,  case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube;


insert into table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
    SELECT
           '${pt}',
            nvl(platform_type,'all'),
            nvl(country_code,'all') ,
            sum(goods_number*shop_price) ,
            count(distinct order_id) ,
	   0,
           0,
           0,

           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0

  from dwd.dwd_fd_order_goods
  where  (date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}' or
     date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}')
  and pay_status=2
  and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
  and project_name='tendaisy'
  group by platform_type,country_code with cube;


insert into table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
select
        '${pt}',
        nvl(platform_type,'all'),
        nvl(country,'all'),
          0.0,
        0,
        count(distinct sp_duid) as paid_users,
        count(distinct domain_userid) as uv,
        count(distinct if(is_new_user = 'new', domain_userid, null)) as access_today_users_new,

        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0

from(
  select
        '${pt}',
        platform_type,
        country,
        case
        when session_idx=1 then 'new'
        when session_idx>1 then 'old'
        end  as is_new_user,
        domain_userid
              from ods_fd_snowplow.ods_fd_snowplow_all_event
              where pt = '${pt}'
                  and project = 'tendaisy'
)t1  left  join (
                  select ud.sp_duid
                    from
		 	(select user_id,pay_status,project_name,email from dwd.dwd_fd_order_info
                      	where  date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}' or
                          date_format(from_utc_timestamp(from_unixtime(order_time), 'PRC'), 'yyyy-MM-dd') = '${pt}'
                    )oi
                    inner join ods_fd_vb.ods_fd_user_duid ud on oi.user_id = ud.user_id
                    where oi.pay_status = 2
                      and oi.project_name = 'tendaisy'
                      AND oi.email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
) t2 on t1.domain_userid = t2.sp_duid
group by platform_type,country with cube;




insert into table dwb.dwb_fd_user_retention_rpt partition (pt='${pt}')
SELECT
      '${pt}',
      nvl(platform_type,'all'),
      nvl(country,'all'),
        0.0,
        0,
        0 ,
        0,
        0,

        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
      count(distinct sp_duid)
from(select sp_duid,platform_type,country_code as country from((select distinct domain_userid
from ods_fd_snowplow.ods_fd_snowplow_all_event  where pt = '${pt}'and project = 'tendaisy'
)t1 left join (select sp_duid,platform_type,country_code from dwd.dwd_fd_order_info where pay_status = 2
                      and project_name = 'tendaisy'
                      AND email not like '%%@tetx.com'
                      AND email not like '%%@i9i8.com')t2
  on t1.domain_userid=t2.sp_duid)where platform_type is not null and  country_code is not NULL
)tab1 GROUP by platform_type,country with cube;
