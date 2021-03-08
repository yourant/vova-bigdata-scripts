insert overwrite table dwb.dwb_fd_coupon_used PARTITION (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    project_name,
    coupon_type_name,
    goods_amount_total,
    bonus_total,
    sum(nvl(goods_amount_total, 0.00)) over (partition by project_name) as pt_goods_amount,
    sum(nvl(bonus_total, 0.00)) over (partition by project_name) as pt_bonus
from(

    select
        nvl(project_name,'all') as project_name,
        nvl(coupon_type_name,'all') as coupon_type_name,
        sum(goods_amount) as goods_amount_total,
        sum(bonus) as bonus_total
    from dwd.dwd_fd_coupon_used_detail
    where pt = '${pt}'
    and project_name is not null
    and coupon_type_name is not null
    group by project_name,coupon_type_name with cube
)tab where tab.coupon_type_name !='all';