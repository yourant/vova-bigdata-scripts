set hive.exec.dynamic.partition.mode=nonstrict;
set hive.new.job.grouping.set.cardinality = 256;
insert overwrite table dwb.dwb_fd_user_repurchase_monthly partition (pt)
select
    /*+ REPARTITION(1) */
    current_month,
    project,
    country_code,
    platform_type,
    user_is_first_pay,
    user_is_first_reg,
    ga_channel,
    purchase_current_month,
    p1m,
    p2m,
    p3m,
    p4m,
    p5m,
    p6m,
    p7m,
    p8m,
    p9m,
    p10m,
    p11m,
    p12m,
    (cast(p1m as float) / cast(purchase_current_month as float)) as p1m_rate,
    (cast(p2m as float) / cast(purchase_current_month as float)) as p2m_rate,
    (cast(p3m as float) / cast(purchase_current_month as float)) as p3m_rate,
    (cast(p4m as float) / cast(purchase_current_month as float)) as p4m_rate,
    (cast(p5m as float) / cast(purchase_current_month as float)) as p5m_rate,
    (cast(p6m as float) / cast(purchase_current_month as float)) as p6m_rate,
    (cast(p7m as float) / cast(purchase_current_month as float)) as p7m_rate,
    (cast(p8m as float) / cast(purchase_current_month as float)) as p8m_rate,
    (cast(p9m as float) / cast(purchase_current_month as float)) as p9m_rate,
    (cast(p10m as float) / cast(purchase_current_month as float)) as p10m_rate,
    (cast(p11m as float) / cast(purchase_current_month as float)) as p11m_rate,
    (cast(p12m as float) / cast(purchase_current_month as float)) as p12m_rate,
    concat(current_month,'-01') as pt
from(
    select nvl(current_month, 'all')     as current_month,
           nvl(project, 'all')           as project,
           nvl(country_code, 'all')      as country_code,
           nvl(platform_type, 'all')     as platform_type,
           nvl(user_is_first_pay, 'all') as user_is_first_pay,
           nvl(user_is_first_reg, 'all') as user_is_first_reg,
           nvl(ga_channel, 'all')        as ga_channel,
           count(distinct purchase_current_month) as purchase_current_month,
           count(distinct p1m) as p1m,
           count(distinct p2m) as p2m,
           count(distinct p3m) as p3m,
           count(distinct p4m) as p4m,
           count(distinct p5m) as p5m,
           count(distinct p6m) as p6m,
           count(distinct p7m) as p7m,
           count(distinct p8m) as p8m,
           count(distinct p9m) as p9m,
           count(distinct p10m) as p10m,
           count(distinct p11m) as p11m,
           count(distinct p12m) as p12m
    from (
             select t1.current_month,
                    t1.project,
                    t1.country_code,
                    t1.platform_type,
                    t1.user_is_first_pay,
                    t1.user_is_first_reg,
                    t1.ga_channel,
                    t1.user_id           as purchase_current_month,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),1),1,7)),t1.user_id, null) as p1m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),2),1,7)),t1.user_id, null) as p2m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),3),1,7)),t1.user_id, null) as p3m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),4),1,7)),t1.user_id, null) as p4m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),5),1,7)),t1.user_id, null) as p5m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),6),1,7)),t1.user_id, null) as p6m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),7),1,7)),t1.user_id, null) as p7m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),8),1,7)),t1.user_id, null) as p8m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),9),1,7)),t1.user_id, null) as p9m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),10),1,7)),t1.user_id, null) as p10m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),11),1,7)),t1.user_id, null) as p11m,
                    if(ARRAY_CONTAINS(t2.all_months,substr(add_months(concat(current_month,'-01'),12),1,7)),t1.user_id, null) as p12m
             from dwd.dwd_fd_user_repurchase_monthly t1
             left join (
                 select user_id, collect_set(current_month) as all_months
                 from dwd.dwd_fd_user_repurchase_monthly
                 where pt='${pt}'
                 group by user_id
             ) t2 on t1.user_id = t2.user_id
             where t1.pt = '${pt}'
               and t1.ga_channel is not null
               and t1.project is not null
               and t1.country_code is not null
               and t1.platform_type is not null
    ) tab1
    group by current_month, project, country_code, platform_type, user_is_first_pay, user_is_first_reg, ga_channel with cube
)tab2 where tab2.current_month != 'all';
