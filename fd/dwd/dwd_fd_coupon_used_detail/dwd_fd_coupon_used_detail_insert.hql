insert overwrite table dwd.dwd_fd_coupon_used_detail PARTITION (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    oi.project_name,
    oi.coupon_code,
    oi.pay_time_prc,
    oi.pay_status,
    oi.bonus as bonus,
    oi.goods_amount,
    CASE
         WHEN kcc.coupon_config_comment = '10001' THEN 'A-售前折扣红包'
         WHEN kcc.coupon_config_comment = '10002' THEN 'B-售后退款/折扣红包'
         WHEN kcc.coupon_config_comment = '10003' THEN 'C-关税红包'
         WHEN kcc.coupon_config_comment = '10004' THEN 'D-好评红包'
         WHEN kcc.coupon_config_comment = '10005' THEN 'E-survey红包'
         WHEN kcc.coupon_config_comment = '10006' THEN 'F-售中小金额退款红包'
         WHEN kcc.coupon_config_comment = '10011' THEN 'H-注册送红包'
         WHEN kcc.coupon_config_comment = '10007' THEN 'I-newsletter红包'
         WHEN kcc.coupon_config_comment = '10008' THEN 'J-未付款红包'
         WHEN kcc.coupon_config_comment = '10009' THEN 'K-大客户红包'
         WHEN kcc.coupon_config_comment = '10010' THEN 'L-其他'
         WHEN kcc.coupon_config_comment = '10012' THEN 'M-EXTRA5'
         WHEN kcc.coupon_config_comment = '10013' THEN 'N-EXTRA10'
         WHEN kcc.coupon_config_comment = '10014' THEN 'o-测试'
         WHEN kcc.coupon_config_comment = '10017' THEN '10017' -- 用户确认收货赠送coupon
         WHEN kcc.coupon_config_comment = '10101' THEN 'fdapp 用户连续登陆3天,赠送coupon'
         WHEN kcc.coupon_config_comment = '10102' THEN 'fdapp 用户连续登陆6天,赠送coupon'
         WHEN kcc.coupon_config_comment = '10103' THEN '用户完成首单推送后送coupon'
         WHEN kcc.coupon_config_comment = '10104' THEN 'fdapp 注册coupon[3天有效期]'
         WHEN kcc.coupon_config_comment = '10105' THEN 'fdapp 注册coupon[7天有效期]'
         WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 AND oi.integral != 0 THEN 'Points' --积分抵扣
         WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 AND oi.email REGEXP "tetx.com|i9i8.com" THEN '手工无红包抵扣'
         WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 THEN '未知无红包抵扣'
         ELSE LOWER(kcc.coupon_config_comment) END coupon_type_name
from (

    select
        user_id,
        order_time,
        coupon_code,
        project_name,
        pay_status,
        bonus,
        integral,
        email,
        goods_amount,
        pay_time_prc
    from order_info_paiyed
    where pay_time_prc = '${pt}'

)oi
left join(
    select
        user_id,
        coupon_code,
        coupon_config_id,
        from_unixtime(coupon_ctime, 'yyyy-MM-dd HH:mm:ss') as coupon_ctime_date, --红包创建时间UTC
        from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss') as coupon_give_date,  --红包发放时间UTC
        can_use_times --红包可以使用次数
    from ods_fd_vb.ods_fd_ok_coupon

)oc  on oi.coupon_code = oc.coupon_code
left join (select coupon_config_id,coupon_config_comment from ods_fd_vb.ods_fd_ok_coupon_config ) kcc ON oc.coupon_config_id = kcc.coupon_config_id

--计算Wallet,Payment discount,xy_promotion_discount
union all
select
    oi.project_name,
    oi.coupon_code,
    oi.pay_time_prc,
    oi.pay_status,
    oi.bonus as bonus,
    oi.goods_amount,
    oe.coupon_type_name as coupon_type_name --Wallet,Payment discount,xy_promotion_discount
from (
    select
        user_id,
        order_id,
        order_time,
        coupon_code,
        project_name,
        pay_status,
        bonus,
        integral,
        email,
        goods_amount,
        pay_time_prc
    from order_info_paiyed
    where pay_time_prc = '${pt}'

)oi
left join (

    select
        order_id,
        coupon_type_name
    from(

        SELECT order_id,coupon_type_name,Row_Number() OVER (partition by order_id ORDER BY name_weight desc) rank
        from (
            SELECT
                order_id,
                ext_name as coupon_type_name,
                case
                    when ext_name in ('xy_total_bonus') then 3
                    when ext_name in ('paymentReduceExchange') then 2
                    when ext_name in ('wallet_fee_exchange') then 1
                end as name_weight
            FROM ods_fd_vb.ods_fd_order_extension
            where ext_name in ('wallet_fee_exchange','paymentReduceExchange','xy_total_bonus')
            --xy_total_bonus>paymentReduceExchange>wallet_fee_exchange
        )tab1

    )tab2 where tab2.rank = 1


)oe on oi.order_id = oe.order_id;