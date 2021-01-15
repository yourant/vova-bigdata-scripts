insert overwrite table dim.dim_fd_goods
select
  lower(t3.project_name) as project_name,
  t1.goods_id as goods_id,
  t3.virtual_goods_id as virtual_goods_id,
  t1.cp_goods_id as cp_goods_id,
  t1.brand_id as brand_id,
  t1.goods_sn as goods_sn,
  t1.goods_name as goods_name,
  t1.goods_desc as goods_desc,
  t1.keywords as keywords,
  t1.add_time as add_time,
  t1.is_complete as is_complete,
  t1.is_new as is_new,
  t1.cat_id as cat_id,
  t2.cat_name as cat_name,
  t2.first_cat_id as first_cat_id,
  t2.first_cat_name as first_cat_name,
  t2.second_cat_id as second_cat_id,
  t2.second_cat_name as second_cat_name,
  t2.three_cat_id as third_cat_id,
  t2.three_cat_name as third_cat_name,
  t1.shop_price as shop_price,
  t1.goods_weight as goods_weight,
  t4.ext_value
from (
	select
		goods_id,cp_goods_id,brand_id,goods_sn,goods_name,goods_desc,keywords,add_time,
		is_on_sale,
		is_delete,is_display,is_complete,is_new,cat_id,shop_price,goods_weight
	from ods_fd_vb.ods_fd_goods
	) t1
LEFT JOIN dim.dim_fd_category t2 on t1.cat_id = t2.cat_id
LEFT JOIN ods_fd_vb.ods_fd_virtual_goods t3 on t1.goods_id = t3.goods_id
LEFT JOIN ods_fd_vb.ods_fd_goods_project_extension t4 on t3.goods_id = t4.goods_id and lower(t3.project_name) = lower(t4.project_name) and t4.ext_name = 'goods_selector'
;
