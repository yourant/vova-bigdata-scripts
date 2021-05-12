order_goods_sn  shipping_tracking_number	goal_warehouse	tracking_number	pay_time	real_pay	sub_fee

spark-sql --conf "spark.app.name=ethan.zheng" -e "
select
  dog.order_goods_sn,
  ost.shipping_tracking_number,
  case cog.goal_warehouse
      when '1' then '燕文'
      when '2' then '捷网'
      else ''
  END as goal_warehouse,
  foip.tracking_number,
  dog.pay_time,
  dog.container_transportation_shipping_fee AS real_pay,
  oge2.extension_info AS sub_fee
from dim.dim_vova_order_goods dog
  left join ods_vova_vts.ods_vova_collection_order_goods cog on cog.order_goods_id = dog.order_goods_id
  left join (select ost.order_goods_id ,first(shipping_tracking_number) as shipping_tracking_number from ods_vova_vts.ods_vova_order_shipping_tracking ost group by ost.order_goods_id ) ost on ost.order_goods_id = dog.order_goods_id
  left join (select foip.order_goods_id,first(tracking_number) as tracking_number from ods_vova_vts.ods_vova_fisher_order_ship_product foip group by foip.order_goods_id) foip on foip.order_goods_id = dog.order_goods_id
  left join (select oge2.rec_id,first(oge2.extension_info) as extension_info from ods_vova_vts.ods_vova_order_goods_extension oge2 where oge2.ext_name = 'container_transportation_shipping_fee_discount'  group by oge2.rec_id) oge2 on oge2.rec_id = dog.order_goods_id
where date(dog.pay_time) >= '2021-04-01'
  and date(dog.pay_time) < '2021-04-20'
  and date(dog.confirm_time) >= '2021-04-01'
  and date(dog.confirm_time) < '2021-05-01'
  and dog.collection_plan_id = 2
  and dog.pay_status >= 1
  and cog.combine_type in (2,3 )
  "   > jiewang_210401_210419.csv

spark-sql --conf "spark.app.name=ethan.zheng" -e "
select
  dog.order_goods_sn,
  ost.shipping_tracking_number,
  case cog.goal_warehouse
      when '1' then '燕文'
      when '2' then '捷网'
      else ''
  END as goal_warehouse,
  foip.tracking_number,
  dog.pay_time,
  dog.container_transportation_shipping_fee AS real_pay,
  oge2.extension_info AS sub_fee
from dim.dim_vova_order_goods dog
  left join ods_vova_vts.ods_vova_collection_order_goods cog on cog.order_goods_id = dog.order_goods_id
  left join (select ost.order_goods_id ,first(shipping_tracking_number) as shipping_tracking_number from ods_vova_vts.ods_vova_order_shipping_tracking ost group by ost.order_goods_id ) ost on ost.order_goods_id = dog.order_goods_id
  left join (select foip.order_goods_id,first(tracking_number) as tracking_number from ods_vova_vts.ods_vova_fisher_order_ship_product foip group by foip.order_goods_id) foip on foip.order_goods_id = dog.order_goods_id
  left join (select oge2.rec_id,first(oge2.extension_info) as extension_info from ods_vova_vts.ods_vova_order_goods_extension oge2 where oge2.ext_name = 'container_transportation_shipping_fee_discount'  group by oge2.rec_id) oge2 on oge2.rec_id = dog.order_goods_id
where date(dog.pay_time) >= '2021-04-20'
  and date(dog.pay_time) < '2021-05-01'
  and date(dog.confirm_time) >= '2021-04-01'
  and date(dog.confirm_time) < '2021-05-01'
  and dog.collection_plan_id = 2
  and dog.pay_status >= 1
  and cog.combine_type in (2,3 )
  "   > jiewang_210420_210430.csv