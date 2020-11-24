CREATE table if not exists dwb.dwb_fd_user_retention_rpt
(
    prc_date                string comment '当前北京时间',
    platform_type           string comment '平台',
    country                 string comment '国家',
    
    goods_amount            decimal(15, 4) comment '销售额',
    paid_orders             bigint  comment '已支付订单量',
    paid_users              bigint  comment'今天访问用户中有历史下单的',
    uv                       bigint comment 'uv(今天访问用户)',
    access_today_users_new   bigint COMMENT '今天新访问用户',

    access_1ago_users       bigint COMMENT '1天前访问用户',
    access_both1_users      bigint COMMENT '1天前和今天都访问用户',
    access_7ago_users       bigint COMMENT '7天前访问用户',
    access_both7_users      bigint COMMENT '7天前和今天都访问用户',
    access_28ago_users      bigint COMMENT '28天前访问用户',
    access_both28_users     bigint COMMENT '28天前和今天都访问用户',
    access_1ago_users_new   bigint COMMENT '1天前新访问用户',
    access_both1_users_new  bigint COMMENT '1天前新访问用户在今天也访问的用户',
    access_7ago_users_new   bigint COMMENT '7天前新访问用户',
    access_both7_users_new  bigint COMMENT '7天前新访问用户在今天也访问的用户',
    access_28ago_users_new  bigint COMMENT '28天前新访问用户',
    access_both28_users_new bigint COMMENT '28天前新访问用户在今天也访问的用户',
    uv_past_paid            bigint COMMENT '今天新访问用户中曾经下过单的用户'
) comment '各维度下的uv，订单量，销售额，用户n天留存分析'
    partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");



insert overwrite table dwb.dwb_fd_user_retention_rpt partition (pt='${hiveconf:pt}')
select
       '${hiveconf:pt}',
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
      where pt = date_add('${hiveconf:pt}', -1)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid, case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${hiveconf:pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube

    union all

select
       '${hiveconf:pt}',
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
      where pt = date_add('${hiveconf:pt}', -7)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid,  case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${hiveconf:pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube

    union all

select
       '${hiveconf:pt}',
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
      where pt = date_add('${hiveconf:pt}', -28)
        and project = 'tendaisy') t1
         left join (select distinct domain_userid,  case
         when session_idx=1 then 'new'
         when session_idx>1 then 'old'
         end  as is_new_user, platform_type, country
                    from ods_fd_snowplow.ods_fd_snowplow_all_event
                    where pt = '${hiveconf:pt}'
                      and project = 'tendaisy'
) t2 on t1.domain_userid = t2.domain_userid
group by t1.platform_type, t1.country with cube

        union all

    SELECT
           '${hiveconf:pt}',
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
  where  (date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${hiveconf:pt}' or
     date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${hiveconf:pt}')
  and pay_status=2
  and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
  and project_name='tendaisy'
  group by platform_type,country_code with cube

  union all

select
        '${hiveconf:pt}',
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
        '${hiveconf:pt}',
        platform_type,
        country,
        case
        when session_idx=1 then 'new'
        when session_idx>1 then 'old'
        end  as is_new_user,
        domain_userid
              from ods_fd_snowplow.ods_fd_snowplow_all_event
              where pt = '${hiveconf:pt}'
                  and project = 'tendaisy'
)t1  left  join (
                  select ud.sp_duid
                    from
		 	(select user_id,pay_status,project_name,email from dwd.dwd_fd_order_info
                      	where  date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${hiveconf:pt}' or
                          date_format(from_utc_timestamp(from_unixtime(order_time), 'PRC'), 'yyyy-MM-dd') = '${hiveconf:pt}'
                    )oi
                    inner join ods_fd_vb.ods_fd_user_duid ud on oi.user_id = ud.user_id
                    where oi.pay_status = 2
                      and oi.project_name = 'tendaisy'
                      AND oi.email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
) t2 on t1.domain_userid = t2.sp_duid
group by platform_type,country with cube


union all


SELECT
      '${hiveconf:pt}',
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
from ods_fd_snowplow.ods_fd_snowplow_all_event  where pt = '${hiveconf:pt}'and project = 'tendaisy'
)t1 left join (select sp_duid,platform_type,country_code from dwd.dwd_fd_order_info where pay_status = 2
                      and project_name = 'tendaisy'
                      AND email not like '%%@tetx.com'
                      AND email not like '%%@i9i8.com')t2
  on t1.domain_userid=t2.sp_duid)where platform_type is not null and  country_code is not NULL
)tab1 GROUP by platform_type,country with cube;
