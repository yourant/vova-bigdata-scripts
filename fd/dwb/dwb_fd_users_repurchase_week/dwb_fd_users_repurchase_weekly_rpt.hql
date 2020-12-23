set hive.exec.dynamic.partition.mode=nonstrict;
set hive.new.job.grouping.set.cardinality = 256;
insert overwrite table dwb.dwb_fd_user_repurchase_weekly_rpt partition (pt)
select
    /*+ REPARTITION(1) */
    project,
    country_code,
    platform_type,
    user_is_first_pay,
    user_is_first_reg,
    ga_channel,
    purchase_current_week,
    p1w,
    p2w,
    p3w,
    p4w,
    p5w,
    p6w,
    p7w,
    p8w,
    (cast(p1w as float) / cast(purchase_current_week as float)) as p1w_rate,
    (cast(p2w as float) / cast(purchase_current_week as float)) as p2w_rate,
    (cast(p3w as float) / cast(purchase_current_week as float)) as p3w_rate,
    (cast(p4w as float) / cast(purchase_current_week as float)) as p4w_rate,
    (cast(p5w as float) / cast(purchase_current_week as float)) as p5w_rate,
    (cast(p6w as float) / cast(purchase_current_week as float)) as p6w_rate,
    (cast(p7w as float) / cast(purchase_current_week as float)) as p7w_rate,
    (cast(p8w as float) / cast(purchase_current_week as float)) as p8w_rate,
    pt
from(
    select nvl(current_week, 'all')      as pt,
           nvl(project, 'all')           as project,
           nvl(country_code, 'all')      as country_code,
           nvl(platform_type, 'all')     as platform_type,
           nvl(user_is_first_pay, 'all') as user_is_first_pay,
           nvl(user_is_first_reg, 'all') as user_is_first_reg,
           nvl(ga_channel, 'all')        as ga_channel,
           count(distinct purchase_current_week) purchase_current_week,
           count(distinct p1w) as p1w,
           count(distinct p2w) as p2w,
           count(distinct p3w) as p3w,
           count(distinct p4w) as p4w,
           count(distinct p5w) as p5w,
           count(distinct p6w) as p6w,
           count(distinct p7w) as p7w,
           count(distinct p8w) as p8w
    from (
             select t1.current_week,
                    t1.project,
                    t1.country_code,
                    t1.platform_type,
                    t1.user_is_first_pay,
                    t1.user_is_first_reg,
                    t1.ga_channel,
                    t1.user_id           as purchase_current_week,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 7) as string)),t1.user_id, null) as p1w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 2*7) as string)),t1.user_id, null) as p2w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 3*7) as string)),t1.user_id, null) as p3w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 4*7) as string)),t1.user_id, null) as p4w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 5*7) as string)),t1.user_id, null) as p5w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 6*7) as string)),t1.user_id, null) as p6w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 7*7) as string)),t1.user_id, null) as p7w,
                    if(ARRAY_CONTAINS(t2.all_weeks,cast(date_add(t1.current_week, 8*7) as string)),t1.user_id, null) as p8w
             from dwd.dwd_fd_user_repurchase_weekly t1
             left join (
                 select user_id, collect_set(current_week) as all_weeks
                 from dwd.dwd_fd_user_repurchase_weekly
                 where pt = '${pt}'
                 group by user_id
             ) t2 on t1.user_id = t2.user_id
             where t1.pt = '${pt}'
               and t1.ga_channel is not null
               and t1.project is not null
               and t1.country_code is not null
               and t1.platform_type is not null
    ) tab1
    group by current_week, project, country_code, platform_type, user_is_first_pay, user_is_first_reg, ga_channel with cube
)tab2 where tab2.pt != 'all'