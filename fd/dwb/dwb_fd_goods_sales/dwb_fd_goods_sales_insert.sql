insert overwrite table dwb.dwb_fd_goods_sales partition (pt='${pt}')

select
project_name,
cat_name,
country_code,
source_type ,--转换为H5,APP,PC,others
virtual_goods_id ,
--日销售额
sum(goods_number* shop_price),
--日销量
sum(goods_number)
from
 (
 select
project_name,
cat_name,
country_code,
 case
      when platform_type ='pc_web' or platform_type ='tablet_web'  then 'PC'
      when platform_type ='ios_app' or platform_type ='android_app'   then 'APP'
      when platform_type='mobile_web'  then 'H5'
      else 'others'
    end as source_type, --是否转换为H5,APP,PC,others ???
virtual_goods_id ,
--销量
goods_number,
--价格
shop_price
 from
  dwd.dwd_fd_order_goods
where pay_status=2
-- and date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}' --UTC时间
-- 北京时间如下
and date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}'
 and goods_id is not null
  and email not like '%@tetx.com%'
  and email not like '%@i9i8.com%'
  and email not like '%@qq.com%'
  and email not like '%@163.com%'
  and email not like '%@jjshouse.com%'
  and email not like '%@jenjenhouse.com%'
 ) t
group by
project_name,
cat_name,
country_code,
source_type ,
virtual_goods_id ;