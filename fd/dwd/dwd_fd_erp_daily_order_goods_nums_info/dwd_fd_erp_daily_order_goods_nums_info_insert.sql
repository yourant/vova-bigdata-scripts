
insert overwrite table dwd.dwd_fd_erp_daily_order_goods_nums_info  partition (pt = '${pt}')

select
     t1.report_date as report_date ,
     t1.deliver_order_num as deliver_order_num,
     t1.deliver_goods_num as deliver_goods_num ,
     t2.reserved_unck_single_order_num as reserved_unck_single_order_num,
     t3.reserved_unck_multi_order_num as reserved_unck_multi_order_num

from (
-- 订单总数和商品总数
 SELECT
       '${pt}' as report_date,
       count(DISTINCT eoi.order_id) as deliver_order_num,
       sum(eog.goods_number) as deliver_goods_num
FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
INNER JOIN ods_fd_romeo.ods_fd_party_config pc on eoi.party_id =pc.party_id
INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog on eog.order_id = eoi.order_id
WHERE
     from_unixtime(shipping_time,'yyyy-MM-dd HH:mm:ss')  >= '${pt}'
AND from_unixtime(shipping_time,'yyyy-MM-dd HH:mm:ss') < date_sub('${pt}',-1)
AND eoi.facility_id ='383497303'
and pc.party_code = '2'
AND lower(eoi.order_type_id) = 'sale' ) t1

left join

-- 单笔订单所有商品数量等于1的订单
(
SELECT
      '${pt}' as report_date,
       count(DISTINCT eoi2.taobao_order_sn) as reserved_unck_single_order_num
FROM ods_fd_romeo.ods_fd_order_inv_reserved oir
INNER JOIN (
-- 所有商品数量为1的订单
           SELECT eog.order_id as order_id
           FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
           INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog on eog.order_id = eoi.order_id
           INNER JOIN ods_fd_romeo.ods_fd_party_config pc on eoi.party_id =pc.party_id
           where eoi.order_status = 1 and eoi.facility_id ='383497303' AND eoi.shipping_status =0 and pc.party_code = '2'
           group by eog.order_id
           having sum(eog.goods_number)= 1) as t1
on oir.order_id=t1.order_id
inner join ods_fd_ecshop.ods_fd_ecs_order_info eoi2
on t1.order_id=eoi2.order_id
WHERE oir.status = 'Y' ) t2

on t1.report_date=t2.report_date

left join

-- 单笔订单所有商品数量大于1的订单
(
  SELECT
        '${pt}' as report_date,
         count(DISTINCT eoi2.taobao_order_sn) as reserved_unck_multi_order_num
  FROM ods_fd_romeo.ods_fd_order_inv_reserved oir
  INNER JOIN (
-- 所有商品数量大于1的订单
              SELECT eog.order_id as order_id
              FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
              INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog on eog.order_id = eoi.order_id
              INNER JOIN ods_fd_romeo.ods_fd_party_config pc on eoi.party_id =pc.party_id
              where eoi.order_status = 1 and eoi.facility_id ='383497303' AND eoi.shipping_status =0 and pc.party_code = '2'
              group by eog.order_id
              having sum(eog.goods_number) > 1) as t1
  on oir.order_id=t1.order_id
inner join ods_fd_ecshop.ods_fd_ecs_order_info eoi2
on t1.order_id=eoi2.order_id
WHERE oir.status = 'Y' ) t3

on t1.report_date=t3.report_date;






