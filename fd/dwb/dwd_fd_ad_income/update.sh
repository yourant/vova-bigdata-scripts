#!/bin/bash
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo $cur_date

#退货/退款订单数 退款金额
refund_order_cnt_rate=0.1327
refund_order_cnt_rate_send=0.076
#退货红包金额
refund_amount_rate=0.0598
refund_amount_rate_send=0.0598
#代付快递费
pay_express_rate=0.2694
#支付手续费
pay_free_rate_1=0.0161
pay_free_rate_2=0.29

pay_free_rate_1_send=0.0207
pay_free_rate_2_send=0.3
#人员工资
employee_money_order=0.0462
employee_money_send=0.0454
#仓储费
stock_order=0.0099
stock_send=0.0104
#房租
house_amount=3504
#服务器费用
computer_amount=5209

spark-sql   --conf "spark.app.name=dwb_ad_income_order" \
  --conf "spark.sql.crossJoin.enabled=true" \
  --driver-memory 10G \
  --executor-memory 8G \
  --executor-cores 1  \
  --conf "spark.dynamicAllocation.maxExecutors=120"  \
  -e "

insert overwrite table tmp.tmp_ad_income_01
    select count(distinct if(to_date(a.order_time) = '${cur_date}' and d.order_id is null, a.order_id,
                             null))                                           day_order_cnt,    --总订单数 天
           sum(if(to_date(a.order_time) = '${cur_date}', a.goods_amount, 0))   day_gmv,          --商品销售总金额 天
           0 - sum(if(to_date(a.order_time) = '${cur_date}', a.bonus, 0))          day_bonus,        --发货红包金额 天
           sum(if(to_date(a.order_time) = '${cur_date}', a.shipping_fee, 0)) - sum(f.payment_amount) day_shou_express, --代收快递费 天
           count(distinct (if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM') and to_date(a.order_time) <= '${cur_date}' and d.order_id is null,a.order_id, null)))             mon_order_cnt,    --总订单数 月
           sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}' ,a.goods_amount,0))                                                mon_gmv,          --商品销售总金额 月
           0 - sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',a.bonus,0))                                                     mon_bonus,        --发货红包金额 月
           sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',a.shipping_fee,0))
               - sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',f.payment_amount,0))                       mon_shou_express,  --代收快递费 月

         sum(if(to_date(a.order_time) = '${cur_date}',h.goods_number,0)) /
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM'),h.goods_number,0)) day_before_month_rate,
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM'),h.goods_number,0)) /
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM'),h.goods_number,0)) month_before_month_rate,
         sum(if(to_date(a.order_time) = '${cur_date}',nvl(h.unit_cost,0),0)) union_cost_day,  --商品采购成本 天
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',h.unit_cost,0)) unit_cost_month, --商品采购成本 月
         sum(if(to_date(a.order_time) = '${cur_date}', a.order_amount, 0))   order_amount_day,          --下单维度总收款金额 天
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}' ,a.order_amount,0))    order_amount_month  --下单维度总收款金额 月
    from (select nvl(eoi.goods_amount / usd_currency_conversion_rate, 0.00) as goods_amount,
                 nvl(-1 * eoi.bonus / usd_currency_conversion_rate, 0.00)   as bonus,
                 nvl(eoi.shipping_fee / usd_currency_conversion_rate, 0.00) as shipping_fee,
                 eoi.order_time,
                 eoi.order_id,
                 eoi.party_id,usd_currency_conversion_rate,
                 nvl(eoi.order_amount / usd_currency_conversion_rate, 0.00) as order_amount
          from (
                   select eoi.*,
                          nvl(ucc.currency_conversion_rate, 1.0)                                                     as usd_currency_conversion_rate,
                          row_number()
                                  OVER (PARTITION BY eoi.order_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
                   from ods_fd_ecshop.ods_fd_ecs_order_info eoi
                            left join (SELECT currency_conversion_rate
                                            , to_currency_code
                                            , currency_conversion_date                                     as currency_conversion_shanghai_ts
                                            , to_utc_timestamp(currency_conversion_date, 'Asia/Shanghai') as currency_conversion_utc_ts
                                       FROM ods_fd_romeo.ods_fd_currency_conversion
                                       WHERE from_currency_code = 'USD'
                                         AND currency_conversion_date IS NOT NULL
                                         AND cancellation_flag != 'Y') ucc on eoi.currency = ucc.to_currency_code AND
                                                                              eoi.order_time >= ucc.currency_conversion_shanghai_ts
                   where eoi.order_type_id = 'SALE'
                     and from_unixtime(unix_timestamp(eoi.order_time), 'yyyy-MM') >=
                         from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM')
               ) eoi
          where currency_rn = 1) a
             join ods_fd_romeo.ods_fd_party b on a.party_id = b.party_id and b.name = 'AiryDress'
             left join (select order_id
                        from ods_fd_ecshop.ods_fd_order_attribute
                        where attr_name = 'middle_order_type'
                          and attr_value != ''
                        group by order_id) c on a.order_id = c.order_id --过滤中台订单
             left join (select order_id
                        from ods_fd_ecshop.ods_fd_order_attribute
                        where attr_name = 'Manual_order'
                          and attr_value != ''
                        group by order_id) d on a.order_id = d.order_id --手工单
             left join ods_fd_ecshop.ods_fd_return_apply e on a.order_id = e.sale_order_id
             left join (select updated_at, shipment_id, payment_amount
                        from ods_fd_ecshop.ods_fd_fly_fish_logistics_order fflo
                        ) f
                       on concat('-', e.return_apply_id) = f.shipment_id
    left join (select order_id,sum(unit_cost * g.goods_number) unit_cost,sum(g.goods_number) goods_number from ods_fd_ecshop.ods_fd_ecs_order_goods g
    left join ods_fd_ecshop.ods_fd_ecs_goods h on g.goods_id = h.goods_id
    left join (select product_id, sum(unit_cost) / count(1) unit_cost
from (
         select nvl(t.unit_cost / usd_currency_conversion_rate, 0.00) as unit_cost,
                t.product_id
         from (
                  select i.*,
                         nvl(ucc.currency_conversion_rate, 1.0)                                                            as usd_currency_conversion_rate,
                         row_number()
                                 OVER (PARTITION BY i.inventory_item_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
                  from ods_fd_romeo.ods_fd_inventory_item i
                           left join (SELECT currency_conversion_rate
                                           , to_currency_code
                                           , currency_conversion_date                                    as currency_conversion_shanghai_ts
                                           , to_utc_timestamp(currency_conversion_date, 'Asia/Shanghai') as currency_conversion_utc_ts
                                      FROM ods_fd_romeo.ods_fd_currency_conversion
                                      WHERE from_currency_code = 'USD'
                                        AND currency_conversion_date IS NOT NULL
                                        AND cancellation_flag != 'Y') ucc
                                     on i.currency = ucc.to_currency_code AND
                                        i.last_updated_stamp >= ucc.currency_conversion_shanghai_ts
              ) t
         where currency_rn = 1
     ) t2
group by product_id
) i on h.product_id = i.product_id
group by order_id) h on a.order_id = h.order_id
left join dwd.dwd_fd_ecs_order_info_shipping l on a.order_id = l.ecs_order_id
    where c.order_id is null
;
insert overwrite table tmp.tmp_ad_income_02
    select
sum(if(to_date(a.date) = '${cur_date}', a.cost, 0)) ad_cost_day,
sum(if(to_date(a.date) <= '${cur_date}', a.cost, 0)) ad_cost_month
from ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report a
where a.ads_site_code = 'AD' and from_unixtime(unix_timestamp(a.date), 'yyyy-MM') =
                                 from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')
;


insert overwrite table dwb.dwb_ad_income_order PARTITION (pt = '${cur_date}')
select
'总订单数',tmp1.day_order_cnt,'',tmp1.mon_order_cnt,'',1
from tmp.tmp_ad_income_01 tmp1
union all
select
'退货/退款订单数',cast(round(tmp1.day_order_cnt * ${refund_order_cnt_rate},0) as int),'',cast(round(tmp1.mon_order_cnt * ${refund_order_cnt_rate},0) as int),'',2
from tmp.tmp_ad_income_01 tmp1
union all
select
'净销售订单数',cast(round(tmp1.day_order_cnt * (1- ${refund_order_cnt_rate}),0) as int),'',cast(round(tmp1.mon_order_cnt * (1- ${refund_order_cnt_rate}),0) as int),'',3
from tmp.tmp_ad_income_01 tmp1
union all
select
'商品销售总金额',round(tmp1.day_gmv,2),'100%',round(tmp1.mon_gmv,2),'100%',4
from tmp.tmp_ad_income_01 tmp1
union all
select
'退款金额',round(tmp1.day_gmv * ${refund_order_cnt_rate},2),
concat(round(tmp1.day_gmv * ${refund_order_cnt_rate} / tmp1.day_gmv * 100,2),'%'),
round(tmp1.mon_gmv * ${refund_order_cnt_rate},2),
concat(round(tmp1.mon_gmv * ${refund_order_cnt_rate} / tmp1.mon_gmv * 100,2),'%'),5
from tmp.tmp_ad_income_01 tmp1
union all
select
'商品销售净收入',round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate}),2),'',round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate}),2),'',6
from tmp.tmp_ad_income_01 tmp1
union all
select
'红包消耗总金额',
round(tmp1.day_bonus * (1 - ${refund_amount_rate}),2),
concat(round(abs(tmp1.day_bonus * (1 - ${refund_amount_rate}) / tmp1.day_gmv) * 100,2),'%'),
round(tmp1.mon_bonus * (1 - ${refund_amount_rate}),2),
concat(round(abs(tmp1.mon_bonus * (1 - ${refund_amount_rate}) / tmp1.mon_gmv) * 100,2),'%'),7
from tmp.tmp_ad_income_01 tmp1
union all
selectytui
'发货红包金额',
round(tmp1.day_bonus,2),
concat(round(abs(tmp1.day_bonus / tmp1.day_gmv) * 100,2),'%'),
round(tmp1.mon_bonus,2),
concat(round(abs(tmp1.mon_bonus / tmp1.mon_gmv) * 100,2),'%'),8
from tmp.tmp_ad_income_01 tmp1
union all
select
'退货红包金额',
round(tmp1.day_bonus * -${refund_amount_rate},2),
concat(round(abs(tmp1.day_bonus * ${refund_amount_rate} / tmp1.day_gmv) * 100,2),'%'),
round(tmp1.mon_bonus * -${refund_amount_rate},2),
concat(round(abs(tmp1.mon_bonus * ${refund_amount_rate} / tmp1.mon_gmv) * 100,2),'%'),9
from tmp.tmp_ad_income_01 tmp1
union all
select
'物流费盈亏',
round(tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv,2),
concat(round(abs((tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv) / tmp1.day_gmv) * 100,2),'%'),
round(tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv,2),
concat(round(abs((tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv) / tmp1.mon_gmv) * 100,2),'%'),10
from tmp.tmp_ad_income_01 tmp1
union all
select
'代收快递费',
round(tmp1.day_shou_express,2),
concat(round(abs(tmp1.day_shou_express / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_shou_express,2),
concat(round(abs(tmp1.mon_shou_express / (tmp1.mon_gmv)) * 100,2),'%'),11
from tmp.tmp_ad_income_01 tmp1
union all
select
'代付快递费',
round(0 - ${pay_express_rate} * tmp1.day_gmv,2),
concat(round(abs(${pay_express_rate} * tmp1.day_gmv / (tmp1.day_gmv)) * 100,2),'%'),
round(0 - ${pay_express_rate} * tmp1.mon_gmv,2),
concat(round(abs(${pay_express_rate} * tmp1.mon_gmv / (tmp1.mon_gmv)) * 100,2),'%'),12
from tmp.tmp_ad_income_01 tmp1
union all
select
'商品采购成本',
round(tmp1.union_cost_day,2),
concat(round(abs(tmp1.union_cost_day / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.unit_cost_month,2),
concat(round(abs(tmp1.unit_cost_month / (tmp1.mon_gmv)) * 100,2),'%'),13
from tmp.tmp_ad_income_01 tmp1
union all
select
'广告费',
round(tmp4.ad_cost_day,2),
concat(round(abs(tmp4.ad_cost_day / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp4.ad_cost_month,2),
concat(round(abs(tmp4.ad_cost_month / (tmp1.mon_gmv)) * 100,2),'%'),14
from tmp.tmp_ad_income_02 tmp4
join tmp.tmp_ad_income_01 tmp1 on 1 = 1
union all
select
'支付手续费',
round(tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2},2),
concat(round(abs((tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2}) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2},2),
concat(round(abs((tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2}) / (tmp1.mon_gmv)) * 100,2),'%'),15
from tmp.tmp_ad_income_01 tmp1
union all
select
'毛利',
round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.day_bonus * (1 - ${refund_amount_rate}) + tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv - (tmp1.union_cost_day + tmp4.ad_cost_day + tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2}),2),
'',
round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.mon_bonus * (1 - ${refund_amount_rate}) + tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv - (tmp1.unit_cost_month + tmp4.ad_cost_month + tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2}),2),
'',16
from tmp.tmp_ad_income_01 tmp1
join tmp.tmp_ad_income_02 tmp4 on 1 = 1
union all
select
'毛利率%',
concat(round(abs((tmp1.day_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.day_bonus * (1 - ${refund_amount_rate}) + tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv - (tmp1.union_cost_day + tmp4.ad_cost_day + tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2})) / (tmp1.day_gmv)) * 100,2),'%'),
'',
concat(round(abs((tmp1.mon_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.mon_bonus * (1 - ${refund_amount_rate}) + tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv - (tmp1.unit_cost_month + tmp4.ad_cost_month + tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2})) / (tmp1.mon_gmv)) * 100,2),'%'),
'',17
from tmp.tmp_ad_income_01 tmp1
join tmp.tmp_ad_income_02 tmp4 on 1 = 1
union all
select
'总费用',
round(tmp1.day_gmv * ${employee_money_order} + tmp1.day_gmv * ${stock_order} + ${house_amount} + ${computer_amount},2),
concat(round(abs((tmp1.day_gmv * ${employee_money_order} + tmp1.day_gmv * ${stock_order} + ${house_amount} + ${computer_amount}) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${employee_money_order} + tmp1.mon_gmv * ${stock_order} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount},2),
concat(round(abs((tmp1.mon_gmv * ${employee_money_order} + tmp1.mon_gmv * ${stock_order} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount}) / (tmp1.mon_gmv)) * 100,2),'%'),18
from tmp.tmp_ad_income_01 tmp1
union all
select
'人员工资',
round(tmp1.day_gmv * ${employee_money_order},2),
concat(round(abs(tmp1.day_gmv * ${employee_money_order} / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${employee_money_order},2),
concat(round(abs(tmp1.mon_gmv * ${employee_money_order} / (tmp1.mon_gmv)) * 100,2),'%'),19
from tmp.tmp_ad_income_01 tmp1
union all
select
'仓储费',
round(tmp1.day_gmv * ${stock_order},2),
concat(round(abs(tmp1.day_gmv * ${stock_order} / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${stock_order},2),
concat(round(abs(tmp1.mon_gmv * ${stock_order} / (tmp1.mon_gmv)) * 100,2),'%'),20
from tmp.tmp_ad_income_01 tmp1
union all
select
'房租',
${house_amount},
concat(round(abs(${house_amount} / (tmp1.day_gmv)) * 100,2),'%'),
day('${cur_date}') * ${house_amount},
concat(round(abs(day('${cur_date}') * ${house_amount} / (tmp1.mon_gmv)) * 100,2),'%'),21
from tmp.tmp_ad_income_01 tmp1
union all
select
'服务器费用',
${computer_amount},
concat(round(abs(${computer_amount} / (tmp1.day_gmv)) * 100,2),'%'),
day('${cur_date}') * ${computer_amount},
concat(abs(round(day('${cur_date}') * ${computer_amount} / (tmp1.mon_gmv) * 100,2)),'%'),22
from tmp.tmp_ad_income_01 tmp1
union all
select
'净利润',
round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.day_bonus * (1 - ${refund_amount_rate}) + tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv - (tmp1.union_cost_day + tmp4.ad_cost_day + tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2}) -
(tmp1.day_gmv * ${employee_money_order} + tmp1.day_gmv * ${stock_order} + ${house_amount} + ${computer_amount}),2),
'',
round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.mon_bonus * (1 - ${refund_amount_rate}) + tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv - (tmp1.unit_cost_month + tmp4.ad_cost_month + tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2}) -
(tmp1.mon_gmv * ${employee_money_order} + tmp1.mon_gmv * ${stock_order} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount}),2),
'',23
from tmp.tmp_ad_income_01 tmp1
join tmp.tmp_ad_income_02 tmp4 on 1 = 1
union all
select
'净利率',
concat(round(((tmp1.day_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.day_bonus * (1 - ${refund_amount_rate}) + tmp1.day_shou_express - ${pay_express_rate} * tmp1.day_gmv - (tmp1.union_cost_day + tmp4.ad_cost_day + tmp1.order_amount_day * ${pay_free_rate_1} + tmp1.day_order_cnt * ${pay_free_rate_2})) -
              (tmp1.day_gmv * ${employee_money_order} + tmp1.day_gmv * ${stock_order} + ${house_amount} + ${computer_amount}))  * 100 /
             tmp1.day_gmv,2),'%'),'',
concat(round(((tmp1.mon_gmv * (1 - ${refund_order_cnt_rate}) + tmp1.mon_bonus * (1 - ${refund_amount_rate}) + tmp1.mon_shou_express - ${pay_express_rate} * tmp1.mon_gmv - (tmp1.unit_cost_month + tmp4.ad_cost_month + tmp1.order_amount_month * ${pay_free_rate_1} + tmp1.mon_order_cnt * ${pay_free_rate_2})) -
             (tmp1.mon_gmv * ${employee_money_order} + tmp1.mon_gmv * ${stock_order} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount}))  * 100 /
             tmp1.mon_gmv,2),'%'),'',24
from tmp.tmp_ad_income_01 tmp1
join tmp.tmp_ad_income_02 tmp4 on 1 = 1
;








insert overwrite table tmp.tmp_ad_income_03
    select count(distinct if(to_date(a.order_time) = '${cur_date}', a.order_id,
                             null))                                           day_order_cnt,    --总订单数 天
           sum(if(to_date(a.order_time) = '${cur_date}', a.goods_amount, 0))   day_gmv,          --商品销售总金额 天
           0 - sum(if(to_date(a.order_time) = '${cur_date}', a.bonus, 0))          day_bonus,        --发货红包金额 天
           sum(if(to_date(a.order_time) = '${cur_date}', a.shipping_fee, 0)) - sum(f.payment_amount) day_shou_express, --代收快递费 天
           count(distinct (if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM') and to_date(a.order_time) <= '${cur_date}',a.order_id, null)))             mon_order_cnt,    --总订单数 月
           sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}' ,a.goods_amount,0))                                                mon_gmv,          --商品销售总金额 月
           0 - sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',a.bonus,0))                                                     mon_bonus,        --发货红包金额 月
           sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',a.shipping_fee,0))
               - sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',f.payment_amount,0))                       mon_shou_express,  --代收快递费 月

         sum(if(to_date(a.order_time) = '${cur_date}',h.goods_number,0)) /
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM'),h.goods_number,0)) day_before_month_rate,
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM'),h.goods_number,0)) /
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM'),h.goods_number,0)) month_before_month_rate,
         sum(if(to_date(a.order_time) = '${cur_date}',nvl(h.unit_cost,0),0)) union_cost_day,  --商品采购成本 天
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}',h.unit_cost,0)) unit_cost_month, --商品采购成本 月
         sum(if(to_date(a.order_time) = '${cur_date}', a.order_amount, 0))   order_amount_day,          --下单维度总收款金额 天
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(a.order_time) <= '${cur_date}' ,a.order_amount,0))    order_amount_month,  --下单维度总收款金额 月
         max(i.should_amount_day)   should_express_amount_day,          --代付快递费 天
         max(i.should_amount_month)    should_express_amount_month,  --代付快递费 月
         max(j.order_cnt_day) order_dim_day,max(j.order_cnt_month) order_dim_month,    --计算广告费
         sum(if(to_date(a.order_time) = '${cur_date}',k.ads_cost,0)) ad_cost_day,
         sum(if(from_unixtime(unix_timestamp(a.order_time), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM') and to_date(a.order_time) <= '${cur_date}',k.ads_cost,0)) ad_cost_month
    from (select nvl(eoi.goods_amount / usd_currency_conversion_rate, 0.00) as goods_amount,
                 nvl(-1 * eoi.bonus / usd_currency_conversion_rate, 0.00)   as bonus,
                 nvl(eoi.shipping_fee / usd_currency_conversion_rate, 0.00) as shipping_fee,
                 from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss') order_time,
                 eoi.order_id,
                 eoi.party_id,usd_currency_conversion_rate,
                 nvl(eoi.order_amount / usd_currency_conversion_rate, 0.00) as order_amount
          from (
                   select eoi.*,
                          nvl(ucc.currency_conversion_rate, 1.0)                                                     as usd_currency_conversion_rate,
                          row_number()
                                  OVER (PARTITION BY eoi.order_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
                   from ods_fd_ecshop.ods_fd_ecs_order_info eoi
                            left join (SELECT currency_conversion_rate
                                            , to_currency_code
                                            , currency_conversion_date                                     as currency_conversion_shanghai_ts
                                            , to_utc_timestamp(currency_conversion_date, 'Asia/Shanghai') as currency_conversion_utc_ts
                                       FROM ods_fd_romeo.ods_fd_currency_conversion
                                       WHERE from_currency_code = 'USD'
                                         AND currency_conversion_date IS NOT NULL
                                         AND cancellation_flag != 'Y') ucc on eoi.currency = ucc.to_currency_code AND
                                                                              from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss') >= ucc.currency_conversion_shanghai_ts
                   where eoi.order_type_id = 'SALE' and
                   eoi.shipping_status = 1 --全部出库
                     and from_unixtime(unix_timestamp(from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss')), 'yyyy-MM') >=
                         from_unixtime(unix_timestamp(cast (add_months('${cur_date}',-1) as date)), 'yyyy-MM')
               ) eoi
          where currency_rn = 1) a
             left join (select count(distinct if(to_date(from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss')) = '${cur_date}', eoi.order_id,null)) order_cnt_day,count(1) order_cnt_month
                        from ods_fd_ecshop.ods_fd_ecs_order_info eoi where eoi.order_type_id = 'SALE' and
                 from_unixtime(unix_timestamp(from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss')), 'yyyy-MM') = from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')  and to_date(from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss')) <= '${cur_date}') j on 1 =1
             join ods_fd_romeo.ods_fd_party b on a.party_id = b.party_id and b.name = 'AiryDress'
             left join (select order_id
                        from ods_fd_ecshop.ods_fd_order_attribute
                        where attr_name = 'middle_order_type'
                          and attr_value != ''
                        group by order_id) c on a.order_id = c.order_id --过滤中台订单
             left join ods_fd_ecshop.ods_fd_return_apply e on a.order_id = e.sale_order_id
             left join (select updated_at, shipment_id, payment_amount
                        from ods_fd_ecshop.ods_fd_fly_fish_logistics_order fflo
                        ) f
                       on concat('-', e.return_apply_id) = f.shipment_id
    left join (select order_id,sum(unit_cost * g.goods_number) unit_cost,sum(g.goods_number) goods_number from ods_fd_ecshop.ods_fd_ecs_order_goods g
    left join ods_fd_ecshop.ods_fd_ecs_goods h on g.goods_id = h.goods_id
    left join (select product_id, sum(unit_cost) / count(1) unit_cost
from (
         select nvl(t.unit_cost / usd_currency_conversion_rate, 0.00) as unit_cost,
                t.product_id
         from (
                  select i.*,
                         nvl(ucc.currency_conversion_rate, 1.0)                                                            as usd_currency_conversion_rate,
                         row_number()
                                 OVER (PARTITION BY i.inventory_item_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
                  from ods_fd_romeo.ods_fd_inventory_item i
                           left join (SELECT currency_conversion_rate
                                           , to_currency_code
                                           , currency_conversion_date                                    as currency_conversion_shanghai_ts
                                           , to_utc_timestamp(currency_conversion_date, 'Asia/Shanghai') as currency_conversion_utc_ts
                                      FROM ods_fd_romeo.ods_fd_currency_conversion
                                      WHERE from_currency_code = 'USD'
                                        AND currency_conversion_date IS NOT NULL
                                        AND cancellation_flag != 'Y') ucc
                                     on i.currency = ucc.to_currency_code AND
                                        i.last_updated_stamp >= ucc.currency_conversion_shanghai_ts
              ) t
         where currency_rn = 1
     ) t2
group by product_id
) i on h.product_id = i.product_id
group by order_id) h on a.order_id = h.order_id
left join (select
sum(if(to_date(shipping_date) = '${cur_date}',t.should_amount / usd_currency_conversion_rate,0)) should_amount_day,
sum(t.should_amount / usd_currency_conversion_rate) should_amount_month
from (
select fflo.should_amount,temp.shipping_date,
nvl(ucc.currency_conversion_rate, 1.0)                                                    as usd_currency_conversion_rate,
row_number()
OVER (PARTITION BY fflo.shipment_id,fflo.logistics_tracking_number ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
from ods_fd_ecshop.ods_fd_fly_fish_logistics_order fflo
join (
SELECT
DISTINCT s.shipment_id, s.TRACKING_NUMBER,s.shipping_date
FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
INNER JOIN ods_fd_romeo.ods_fd_order_shipment os ON eoi.order_id = os.order_id
INNER JOIN ods_fd_romeo.ods_fd_shipment s ON os.shipment_id = s.shipment_id
LEFT JOIN ods_fd_ecshop.ods_fd_sync_flyfish_queue sfq ON s.shipment_id = sfq.relevance_key AND sfq.type = 'SP'
WHERE eoi.party_id in ('65592')
AND eoi.order_type_id IN ('SALE', 'TRANSFER')
AND eoi.email not regexp '@tetx.com|@i9i8.com'
AND s.shipment_status >= '155'
AND  from_unixtime(unix_timestamp(s.shipping_date), 'yyyy-MM') =
from_unixtime(unix_timestamp(to_date('${cur_date}')), 'yyyy-MM')
and to_date(shipping_date) <= '${cur_date}'
AND s.facility_id = '383497303'
AND s.shipment_type_id NOT IN (153)
) temp on temp.shipment_id = fflo.shipment_id and temp.tracking_number = fflo.logistics_tracking_number
left join (SELECT currency_conversion_rate
, to_currency_code
, currency_conversion_date                                    as currency_conversion_shanghai_ts
, to_utc_timestamp(currency_conversion_date, 'Asia/Shanghai') as currency_conversion_utc_ts
FROM ods_fd_romeo.ods_fd_currency_conversion
WHERE from_currency_code = 'USD'
AND currency_conversion_date IS NOT NULL
AND cancellation_flag != 'Y') ucc
on fflo.currency = ucc.to_currency_code AND
fflo.updated_at >= ucc.currency_conversion_shanghai_ts
) t
where t.currency_rn = 1) i on 1 = 1
left join dwd.dwd_fd_ecs_order_info_shipping k on a.order_id = k.ecs_order_id
    where c.order_id is null
;


insert overwrite table dwb.dwb_ad_income_out_stock PARTITION (pt = '${cur_date}')
select
'总订单数',tmp1.day_order_cnt,'',tmp1.mon_order_cnt,'',1
from tmp.tmp_ad_income_03 tmp1
union all
select
'退货/退款订单数',cast(round(tmp1.day_order_cnt * ${refund_order_cnt_rate_send},0) as int),'',cast(round(tmp1.mon_order_cnt * ${refund_order_cnt_rate_send},0) as int),'',2
from tmp.tmp_ad_income_03 tmp1
union all
select
'净销售订单数',cast(round(tmp1.day_order_cnt * (1- ${refund_order_cnt_rate_send}),0) as int),'',cast(round(tmp1.mon_order_cnt * (1- ${refund_order_cnt_rate_send}),0) as int),'',3
from tmp.tmp_ad_income_03 tmp1
union all
select
'商品销售总金额',round(tmp1.day_gmv,2),'100%',round(tmp1.mon_gmv,2),'100%',4
from tmp.tmp_ad_income_03 tmp1
union all
select
'退款金额',
round(tmp1.day_gmv * ${refund_order_cnt_rate_send},2),
concat(round(tmp1.day_gmv * ${refund_order_cnt_rate_send} / tmp1.day_gmv * 100,2),'%'),
round(tmp1.mon_gmv * ${refund_order_cnt_rate_send},2),
concat(round(tmp1.mon_gmv * ${refund_order_cnt_rate_send} / tmp1.mon_gmv * 100,2),'%'),5
from tmp.tmp_ad_income_03 tmp1
union all
select
'商品销售净收入',round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate_send}),2),'',round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate_send}),2),'',6
from tmp.tmp_ad_income_03 tmp1
union all
select
'红包消耗总金额',
round(tmp1.day_bonus * (1 - ${refund_amount_rate_send}),2),
concat(round(abs(tmp1.day_bonus * (1 - ${refund_amount_rate_send}) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_bonus * (1 - ${refund_amount_rate_send}),2),
concat(round(abs(tmp1.mon_bonus * (1 - ${refund_amount_rate_send}) / (tmp1.mon_gmv)) * 100,2),'%'),7
from tmp.tmp_ad_income_03 tmp1
union all
select
'发货红包金额',
round(tmp1.day_bonus,2),
concat(round(abs(tmp1.day_bonus / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_bonus,2),
concat(round(abs(tmp1.mon_bonus / (tmp1.mon_gmv)) * 100,2),'%'),8
from tmp.tmp_ad_income_03 tmp1
union all
select
'退货红包金额',
round(tmp1.day_bonus * -${refund_amount_rate_send},2),
concat(round(abs(tmp1.day_bonus * ${refund_amount_rate_send} / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_bonus * -${refund_amount_rate_send},2),
concat(round(abs(tmp1.mon_bonus * ${refund_amount_rate_send} / (tmp1.mon_gmv)) * 100,2),'%'),9
from tmp.tmp_ad_income_03 tmp1
union all
select
'物流费盈亏',
round(tmp1.day_shou_express - tmp1.should_express_amount_day,2),
concat(round(abs((tmp1.day_shou_express - tmp1.should_express_amount_day) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_shou_express -tmp1.should_express_amount_month,2),
concat(round(abs((tmp1.mon_shou_express -tmp1.should_express_amount_month) / (tmp1.mon_gmv)) * 100,2),'%'),10
from tmp.tmp_ad_income_03 tmp1
union all
select
'代收快递费',
round(tmp1.day_shou_express,2),
concat(round(abs(tmp1.day_shou_express / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_shou_express,2),
concat(round(abs(tmp1.mon_shou_express / (tmp1.mon_gmv)) * 100,2),'%'),11
from tmp.tmp_ad_income_03 tmp1
union all
select
'代付快递费',
round(0 - tmp1.should_express_amount_day,2),
concat(round(abs(tmp1.should_express_amount_day / (tmp1.day_gmv)) * 100,2),'%'),
round(0 - tmp1.should_express_amount_month,2),
concat(round(abs(tmp1.should_express_amount_month / (tmp1.mon_gmv)) * 100,2),'%'),12
from tmp.tmp_ad_income_03 tmp1
union all
select
'商品采购成本',
round(tmp1.union_cost_day,2),
concat(round(abs(tmp1.union_cost_day / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.unit_cost_month,2),
concat(round(abs(tmp1.unit_cost_month / (tmp1.mon_gmv)) * 100,2),'%'),13
from tmp.tmp_ad_income_03 tmp1
union all
select
'广告费',
round(tmp1.ad_cost_day,2),
concat(round(abs((tmp1.ad_cost_day) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.ad_cost_month,2),
concat(round(abs((tmp1.ad_cost_month) / (tmp1.mon_gmv)) * 100,2),'%'),14
from tmp.tmp_ad_income_03 tmp1
union all
select
'支付手续费',
round(tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send},2),
concat(round(abs((tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send}) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.order_amount_month * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send},2),
concat(round(abs((tmp1.order_amount_month * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send}) / (tmp1.mon_gmv)) * 100,2),'%'),15
from tmp.tmp_ad_income_03 tmp1
union all
select
'毛利',
round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.day_bonus * (1 - ${refund_amount_rate_send}) + tmp1.day_shou_express - tmp1.should_express_amount_day - (tmp1.union_cost_day + (tmp1.ad_cost_day) + tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send}),2),
'',
round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.mon_bonus * (1 - ${refund_amount_rate_send}) + tmp1.mon_shou_express -tmp1.should_express_amount_month - (tmp1.unit_cost_month + (tmp1.ad_cost_month) + tmp1.order_amount_month * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send}),2),
'',16
from tmp.tmp_ad_income_03 tmp1
union all
select
'毛利率%',
concat(round(abs((tmp1.day_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.day_bonus * (1 - ${refund_amount_rate_send}) + tmp1.day_shou_express - tmp1.should_express_amount_day - (tmp1.union_cost_day + (tmp1.ad_cost_day) + tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send})) / (tmp1.day_gmv)) * 100,2),'%'),
'',
concat(round(abs((tmp1.mon_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.mon_bonus * (1 - ${refund_amount_rate_send}) + tmp1.mon_shou_express -tmp1.should_express_amount_month - (tmp1.unit_cost_month + (tmp1.ad_cost_month) + tmp1.order_amount_month * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send})) / (tmp1.mon_gmv)) * 100,2),'%'),
'',17
from tmp.tmp_ad_income_03 tmp1
union all
select
'总费用',
round(tmp1.day_gmv * ${employee_money_send} + tmp1.day_gmv * ${stock_send} + ${house_amount} + ${computer_amount},2),
concat(round(abs((tmp1.day_gmv * ${employee_money_send} + tmp1.day_gmv * ${stock_send} + ${house_amount} + ${computer_amount}) / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${employee_money_send} + tmp1.mon_gmv * ${stock_send} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount},2),
concat(round(abs((tmp1.mon_gmv * ${employee_money_send} + tmp1.mon_gmv * ${stock_send} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount}) / (tmp1.mon_gmv)) * 100,2),'%'),18
from tmp.tmp_ad_income_03 tmp1
union all
select
'人员工资',
round(tmp1.day_gmv * ${employee_money_send},2),
concat(round(abs(tmp1.day_gmv * ${employee_money_send} / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${employee_money_send},2),
concat(round(abs(tmp1.mon_gmv * ${employee_money_send} / (tmp1.mon_gmv)) * 100,2),'%'),19
from tmp.tmp_ad_income_03 tmp1
union all
select
'仓储费',
round(tmp1.day_gmv * ${stock_send},2),
concat(round(abs(tmp1.day_gmv * ${stock_send} / (tmp1.day_gmv)) * 100,2),'%'),
round(tmp1.mon_gmv * ${stock_send},2),
concat(round(abs(tmp1.mon_gmv * ${stock_send} / (tmp1.mon_gmv)) * 100,2),'%'),20
from tmp.tmp_ad_income_03 tmp1
union all
select
'房租',
${house_amount},
concat(round(abs(${house_amount} / (tmp1.day_gmv)) * 100,2),'%'),
day('${cur_date}') * ${house_amount},
concat(round(abs(day('${cur_date}') * ${house_amount} / (tmp1.mon_gmv)) * 100,2),'%'),21
from tmp.tmp_ad_income_03 tmp1
union all
select
'服务器费用',
${computer_amount},
concat(round(abs(${computer_amount} / (tmp1.day_gmv)) * 100,2),'%'),
day('${cur_date}') * ${computer_amount},
concat(abs(round(day('${cur_date}') * ${computer_amount} / (tmp1.mon_gmv) * 100,2)),'%'),22
from tmp.tmp_ad_income_03 tmp1
union all
select
'净利润',
round(tmp1.day_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.day_bonus * (1 - ${refund_amount_rate_send}) + tmp1.day_shou_express - tmp1.should_express_amount_day - (tmp1.union_cost_day + (tmp1.ad_cost_day) + tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send}) -
(tmp1.day_gmv * ${employee_money_send} + tmp1.day_gmv * ${stock_send} + ${house_amount} + ${computer_amount}),2),
'',
round(tmp1.mon_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.mon_bonus * (1 - ${refund_amount_rate_send}) + tmp1.mon_shou_express -tmp1.should_express_amount_month - (tmp1.unit_cost_month + (tmp1.ad_cost_month) + tmp1.mon_gmv * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send}) -
(tmp1.mon_gmv * ${employee_money_send} + tmp1.mon_gmv * ${stock_send} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount}),2),
'',23
from tmp.tmp_ad_income_03 tmp1
union all
select
'净利率',
concat(round(((tmp1.day_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.day_bonus * (1 - ${refund_amount_rate_send}) + tmp1.day_shou_express - tmp1.should_express_amount_day - (tmp1.union_cost_day + (tmp1.ad_cost_day) + tmp1.order_amount_day * ${pay_free_rate_1_send} + tmp1.day_order_cnt * ${pay_free_rate_2_send}) -
(tmp1.day_gmv * ${employee_money_send} + tmp1.day_gmv * ${stock_send} + ${house_amount} + ${computer_amount})) / (tmp1.day_gmv)) * 100,2),'%'),'',
concat((round((tmp1.mon_gmv * (1 - ${refund_order_cnt_rate_send}) + tmp1.mon_bonus * (1 - ${refund_amount_rate_send}) + tmp1.mon_shou_express -tmp1.should_express_amount_month - (tmp1.unit_cost_month + (tmp1.ad_cost_month) + tmp1.mon_gmv * ${pay_free_rate_1_send} + tmp1.mon_order_cnt * ${pay_free_rate_2_send}) -
(tmp1.mon_gmv * ${employee_money_send} + tmp1.mon_gmv * ${stock_send} + day('${cur_date}') * ${house_amount} + day('${cur_date}') * ${computer_amount})) / (tmp1.mon_gmv) * 100,2)),'%'),'',24
from tmp.tmp_ad_income_03 tmp1
"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--master yarn  \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=ad_money \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.AdMoney s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--pt ${cur_date}  --spark.sparkMaster yarn --rate 0.5

echo '开始发邮件'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--name 'dwb_ad_income_order_email' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select cat_name,day_value,day_rate,month_value,month_rate from dwb.dwb_ad_income_order_1 where pt = '${cur_date}' order by rn"  \
-head "科目,${cur_date},占比（取绝对值）,本月截止昨日累计,占比（取绝对值）"  \
-receiver "juntao@vova.com.hk,cici.liu@i9i8.com,sol.ji@vova.com.hk,qi.zhong@gmail.com,qzhong@i9i8.com,ychen@i9i8.com,yfli@i9i8.com,ytang@i9i8.com,ruth.li@i9i8.com,muqie@i9i8.com,john.wang@i9i8.com,mixian@i9i8.com" \
-title "AD利润报表(订单维度,${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--name 'dwb_ad_income_out_stock_email' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select cat_name,day_value,day_rate,month_value,month_rate from dwb.dwb_ad_income_out_stock_1 where pt = '${cur_date}' order by rn"  \
-head "科目,${cur_date},占比（取绝对值）,本月截止昨日累计,占比（取绝对值）"  \
-receiver "juntao@vova.com.hk,cici.liu@i9i8.com,sol.ji@vova.com.hk,qi.zhong@gmail.com,qzhong@i9i8.com,ychen@i9i8.com,yfli@i9i8.com,ytang@i9i8.com,ruth.li@i9i8.com,muqie@i9i8.com,john.wang@i9i8.com,mixian@i9i8.com" \
-title "AD利润报表(出库维度,${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1


fi


