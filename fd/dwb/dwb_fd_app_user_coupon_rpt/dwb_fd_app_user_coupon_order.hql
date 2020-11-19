CREATE TABLE IF NOT EXISTS dwb.dwb_fd_app_user_coupon_order
(
    project_name     string comment '组织',
    platform_type    string COMMENT '平台',
    country_code 	 string COMMENT '国家',
    coupon_config_id      string COMMENT '优惠券配置ID',
    coupon_give           string COMMENT '红包发放量',
    coupon_used           string COMMENT '红包使用量',
    coupon_used_success   string COMMENT '红包使用成功量',
    coupon_used_1h        string COMMENT '获取红包1h内使用量',
    coupon_used_24h       string comment '获取红包1h-24h内使用量',
    coupon_used_48h       string COMMENT '获取红包24h-48h内使用量',
    coupon_used_72h       string COMMENT '获取红包48h-72h内使用量',
    coupon_used_greater_72h  string COMMENT '获取红包大于72h内使用量'
) COMMENT 'appp用户优惠券使用指标报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT overwrite table dwb.dwb_fd_app_user_coupon_order PARTITION (pt)
select
  t1.project_name as project_name,
  Coalesce(t1.platform_type,t2.platform_type,'unknown') as platform_type,
  Coalesce(t1.country_code,t2.country_code,'unknown') as country_code,
  t1.coupon_config_id,
  t1.coupon_code as coupon_give,
  if(t3.coupon_code is not null,t1.coupon_code,null) as coupon_used,
  if(t3.coupon_code is not null and t3.pay_status = 2,t1.coupon_code,null) as coupon_used_success,
  if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 > 0 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 1,t1.coupon_code,null) as coupon_used_1h,
  if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 1 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 24,t1.coupon_code,null) as coupon_used_24h,
  if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 24 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 48,t1.coupon_code,null) as coupon_used_48h,
  if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 48 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 72,t1.coupon_code,null) as coupon_used_72h,
  if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 72,t1.coupon_code,null) as coupon_used_greater_72h,
  t1.pt as pt
from (
  select
    tab1.user_id as user_id,
    tab1.coupon_code as coupon_code,
    cast(tab1.coupon_config_id as string) as coupon_config_id,
    tab1.coupon_config_comment as coupon_config_comment,
    tab1.coupon_gtime as coupon_gtime,
    tab1.coupon_give_date as coupon_give_date,
    Coalesce(tab2.project_name,tab3.reg_site_name) as project_name,
    Coalesce(tab2.platform,null) as platform_type,
    Coalesce(tab2.country_code,null) as country_code,
    date(tab1.coupon_give_date) as pt
  from (
      select 
            oc.user_id,
            oc.coupon_code,
            oc.coupon_config_id,
            kcc.coupon_config_comment,
            oc.coupon_gtime,
            oc.coupon_give_date
      from (
          select
            user_id,
            coupon_code,
            coupon_config_id,
            coupon_gtime,
            from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss') as coupon_give_date
        from ods_fd_vb.ods_fd_ok_coupon
        where can_use_times = 1
        and length(coupon_code) = 16
        and date(from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss')) >= date_sub('${hiveconf:pt}',30)
        and date(from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss')) <= '${hiveconf:pt}'
      ) oc
      left join (select coupon_config_id,coupon_config_comment from ods_fd_vb.ods_fd_ok_coupon_config ) kcc ON oc.coupon_config_id = kcc.coupon_config_id
        
  )tab1
  left join(

      select 
            t1.user_id,t1.project_name,t1.platform,t1.country_code
      from (
            select
                distinct
                user_id,
                project_name,
                case
                    when platform = 'ios' then 'ios_app'
                    when platform = 'android' then 'android_app'
                    else 'unknown'
                end as platform,
                country_code,
                Row_Number() OVER (partition by user_id  ORDER BY event_time desc) rank
            from ods_fd_vb.ods_fd_app_install_record
            where  user_id is not null and user_id != 0
      ) t1 where t1.rank = 1

  ) tab2 on tab2.user_id = tab1.user_id
  left join (select user_id,reg_site_name from ods_fd_vb.ods_fd_users ) tab3 on tab3.user_id = tab1.user_id

) t1
left join (

    select user_id,project_name,country_code,platform_type
    from (
        select 
                user_id,
                project_name,
                country_code,
                case
                    when is_app = 1 and os_type = 'ios' then 'ios_app'
                    when is_app = 1 and os_type = 'android' then 'android_app'
                    else 'unknown'
                end as platform_type,
                Row_Number() OVER (partition by user_id,project_name ORDER BY order_time desc) rank
        from dwd.dwd_fd_order_info 
        where user_id is not null and user_id != 0
    )t0 WHERE t0.rank = 1

) t2 on (t1.user_id = t2.user_id and t1.project_name = t2.project_name)
left join (
    select
        user_id,
        TO_UTC_TIMESTAMP(order_time, 'America/Los_Angeles') as order_time,
        coupon_code,
        project_name,
        pay_status
    from dwd.dwd_fd_order_info

)t3 on t3.coupon_code = t1.coupon_code;
