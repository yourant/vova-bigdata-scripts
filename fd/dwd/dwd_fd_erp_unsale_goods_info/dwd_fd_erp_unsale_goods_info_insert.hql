insert overwrite table dwd.dwd_fd_erp_unsale_goods_info  partition (pt = '${pt}')
select
     /*+ REPARTITION(1) */
      t0.goods_id,
      t0.goods_sku,
      t1.14d_avg_sale,
      t2.goods_number_month,
      t3.stock_days,
      t4.reserve_num,
      t4.goods_number
 from
     (
     select
             distinct tab2.goods_id,
     	  	tab1.uniq_sku as goods_sku
     from (
     	select uniq_sku
     	from ods_fd_ecshop.ods_fd_fd_sku_backups
     )tab1
     INNER JOIN (
     	select
     		external_goods_id as goods_id,
     		uniq_sku
     	from ods_fd_ecshop.ods_fd_ecs_goods
     	where external_cat_id != 3002
     	group by external_goods_id,uniq_sku
     ) tab2 on tab1.uniq_sku = tab2.uniq_sku
     ) t0
left join
     (select
     	external_goods_id as goods_id,
     	uniq_sku as goods_sku,
     	sum(14d_sale) as 14d_avg_sale
     from ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail
     group by external_goods_id,uniq_sku ) as t1
 on  t1.goods_id=t0.goods_id and t1.goods_sku=t0.goods_sku
left join
     (select
     	eg.external_goods_id as goods_id,
     	eg.uniq_sku as goods_sku,
     	sum(eog.goods_number) as goods_number_month
     from (
     	select goods_id,order_id,goods_number
     	from ods_fd_ecshop.ods_fd_ecs_order_goods
     	group by goods_id,order_id,goods_number
     )eog
     INNER JOIN (
     	select order_id
     	from ods_fd_ecshop.ods_fd_ecs_order_info
     	where pay_status = 2
     	and order_type_id = 'SALE'
     	and to_date(to_utc_timestamp(order_time, 'PRC')) >= trunc('${pt}','MM')
     	and to_date(to_utc_timestamp(order_time, 'PRC')) <= '${pt}'
     	group by order_id
     )eoi on eoi.order_id = eog.order_id
     INNER JOIN (
     	select external_goods_id,uniq_sku,goods_id
     	from ods_fd_ecshop.ods_fd_ecs_goods
     )eg on eog.goods_id = eg.goods_id
     group by eg.external_goods_id,eg.uniq_sku ) as t2
 on t0.goods_id=t2.goods_id and t0.goods_sku=t2.goods_sku
left join
     (
     select
     	goods_id as goods_id,
     	(2 * produce_days + change_provider_days + 2) as stock_days
     from ods_fd_ecshop.ods_fd_fd_stock_goods_config
     ) t3
 on t0.goods_id=t3.goods_id
left join
   (
   SELECT
   	eg.external_goods_id as goods_id,
   	eg.uniq_sku as goods_sku,
   	SUM(ins.demand_quantity) as goods_number,
   	SUM(ins.available_to_reserved) as reserve_num
   FROM (
   	select
   		product_id,
   		available_to_reserved,
   		demand_quantity
   	from ods_fd_romeo.ods_fd_inventory_summary
   	where STATUS_ID= 'INV_STTS_AVAILABLE' and facility_id = '383497303'
   	group by product_id,available_to_reserved,demand_quantity

   ) ins
   INNER JOIN (
   	select
   		external_goods_id,
   		uniq_sku,
   		product_id
   	from ods_fd_ecshop.ods_fd_ecs_goods
   	group by external_goods_id,uniq_sku,product_id

   ) eg ON ins.product_id = eg.product_id
   GROUP BY eg.external_goods_id,eg.uniq_sku
  ) as t4
 on  t0.goods_id=t4.goods_id and t0.goods_sku=t4.goods_sku;