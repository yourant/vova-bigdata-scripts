
insert overwrite table dwd.dwd_fd_goods_event_detail partition (pt = '${pt}')
-- 商品的打点信息

-- 商品的点击，曝光
select
t2.goods_id,
t1.goods_event_struct.virtual_goods_id,
t1.project ,
t1.country ,
t2.cat_name,
t2.cat_id,
t1.platform ,
t1.dvce_type ,
t1.session_id,
t1.event_name ,  --点击，曝光
t1.page_code ,
t1.mkt_source,
t1.goods_event_struct.list_type,
case
   when t1.dvce_type ='Computer' or t1.dvce_type ='Tablet'  then 'PC'
   when t1.dvce_type ='Mobile' and (t1.platform_type='android_app'  or t1.platform_type='ios_app' )  then 'APP'
   when t1.dvce_type ='Mobile' and t1.platform_type='mobile_web'  then 'H5'
   else 'others'
end as source_type
from
    ods_fd_snowplow.ods_fd_snowplow_goods_event t1
left join
    dim.dim_fd_goods t2
on t1.goods_event_struct.virtual_goods_id= t2.virtual_goods_id
where t1.pt='${pt}'

union all

-- 商品的加车，check out等事件
select
t3.goods_id,
t1.ecommerce_product.id as virtual_goods_id,
t1.project ,
t1.country,
t3.cat_name,
t3.cat_id,
t1.platform ,
t1.dvce_type ,
t1.session_id,
t1.event_name,
t1.page_code ,
t1.mkt_source ,
'' as list_type,
case
   when t1.dvce_type ='Computer' or t1.dvce_type ='Tablet'  then 'PC'
   when t1.dvce_type ='Mobile' and (t1.platform_type='android_app'  or t1.platform_type='ios_app' )  then 'APP'
   when  t1.dvce_type ='Mobile' and t1.platform_type='mobile_web'  then 'H5'
   else 'others'
end as source_type
from  ods_fd_snowplow.ods_fd_snowplow_ecommerce_event t1
left join dim.dim_fd_category t2
on t1.ecommerce_product.category=cast(t2.cat_id as string)
left join  dim.dim_fd_goods t3
on t1.ecommerce_product.id=cast(t3.virtual_goods_id as string)
 where t1.pt='${pt}'



 union all


 -- 商品的浏览等事件
 select
 t2.goods_id,
 t1.url_virtual_goods_id ,
 t1.project,
 t1.country,
 t2.cat_name,
 t2.cat_id,
 t1.platform,
 t1.dvce_type,
 t1.session_id,
 t1.event_name,
 t1.page_code,
 t1.mkt_source,
 '' as list_type,
 case
   when t1.dvce_type ='Computer' or t1.dvce_type ='Tablet'  then 'PC'
   when t1.dvce_type ='Mobile' and (t1.platform_type='android_app'  or t1.platform_type='ios_app' )  then 'APP'
   when  t1.dvce_type ='Mobile' and t1.platform_type='mobile_web'  then 'H5'
   else 'others'
end as source_type
 from
 ods_fd_snowplow.ods_fd_snowplow_view_event t1
left join  dim.dim_fd_goods t2
on t1.url_virtual_goods_id=cast(t2.virtual_goods_id as string)
where t1.pt='${pt}'
 ;


