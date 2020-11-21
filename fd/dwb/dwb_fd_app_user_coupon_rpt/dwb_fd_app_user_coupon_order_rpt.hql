set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT overwrite table dwb.dwb_fd_app_user_coupon_order_rpt PARTITION (pt)
select 
    project_name,
    country_code,
    coupon_config_id,
    coupon_give_cnt,
    coupon_used_cnt,
    coupon_used_success_cnt,
    coupon_used_1h_cnt,
    coupon_used_24h_cnt,
    coupon_used_48h_cnt,
    coupon_used_72h_cnt,
    coupon_used_greater_72h_cnt,
    pt
from (
    select 
        nvl(pt,'all') as pt,
        nvl(project_name,'all') as project_name,
        nvl(country_code,'all') as country_code,
        nvl(coupon_config_id,'all') as coupon_config_id,
        count(distinct coupon_give) as coupon_give_cnt , /*红包发放量*/
        count(distinct coupon_used) as coupon_used_cnt, /*红包使用量*/
        count(distinct coupon_used_success) as coupon_used_success_cnt, /*Coupon使用成功量*/
        count(distinct coupon_used_1h) as coupon_used_1h_cnt, /*获取红包1h内使用量*/
        count(distinct coupon_used_24h) as coupon_used_24h_cnt, /*获取红包1h-24h内使用量*/
        count(distinct coupon_used_48h) as coupon_used_48h_cnt, /*获取红包24h-48h内使用量*/
        count(distinct coupon_used_72h) as coupon_used_72h_cnt, /*获取红包48h-72h内使用量*/
        count(distinct coupon_used_greater_72h) as coupon_used_greater_72h_cnt /*获取红包大于72h内使用量*/
    from dwb.dwb_fd_app_user_coupon_order
    where pt >= date_sub('$pt',10)
    group by 
        pt,
        project_name,
        country_code,
        coupon_config_id with cube
)tab where tab.pt != 'all';
