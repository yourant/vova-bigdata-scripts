
insert overwrite table dwb.dwb_fd_goods_detail partition (pt='${pt}')
select
t1.project_name,
t1.country_code,
t1.source_type,
t3.goods_id,
t1.virtual_goods_id,
t3.cat_name,
t2.goods_impression_session,
t2.goods_click_session,
t2.goods_add_session,
t2.cy_po_impression,
t2.cy_po_click,
t2.detail_add,
t2.detail_view,
t1.order_paid_number,
t1.goods_amount
from
--商品订单的相关信息,销量，价格
(select
    project_name,
    country_code,
    virtual_goods_id,
    count(distinct order_id) order_paid_number, --支付订单数
    sum(goods_number*shop_price*100) goods_amount ,--商品销售额
    source_type
from
-- 先进行一步过滤，对国家进行过滤筛选
-- 订单和平台挂钩的。。
 (
  select
    project_name,
    if(country_code in ('DE','FR','PL','SE','GB','MX','BR','IT','CZ','US','NL','ES','CO','CL','CH','AT','AU','NO','DK','RU') ,country_code ,'others') as country_code,
    virtual_goods_id,
    order_id,
    goods_number,
    shop_price,
    case
      when platform_type ='pc_web' or platform_type ='tablet_web'  then 'PC'
      when platform_type ='ios_app' or platform_type ='android_app'   then 'APP'
      when platform_type='mobile_web'  then 'H5'
      else 'others'
    end as source_type
    from  dwd.dwd_fd_order_goods
    where pay_status=2 and date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
 ) t
group by virtual_goods_id,country_code,project_name,source_type
) t1
left join
-- 商品打点事件相关信息，
(
select
   virtual_goods_id,
   project_name,
   country_code,
   source_type,  --平台类型（H5，APP,PC）
   count(distinct if(event_name='goods_impression',session_id,null)) as goods_impression_session,--商品曝光量
   count(distinct if(event_name='goods_click',session_id,null)) as goods_click_session,--商品点击量
   count(distinct if(event_name='add',session_id,null)) as goods_add_session,--商品加车会话量
   count(distinct if(event_name='goods_impression' and list_type in('list-category','list-pre-order'),session_id,null))
                        as cy_po_impression,--品类，预售列表会话曝光量
   count(distinct if(event_name='goods_click' and list_type in('list-category','list-pre-order'),session_id,null))
                        as cy_po_click,--品类，预售列表会话点击量
   count(distinct if(event_name = 'add' and page_code = 'product',session_id,null)) as detail_add,--商品详情页加车会话量
   count(distinct if(event_name in ('page_view', 'screen_view') and page_code = 'product',session_id,null)) as detail_view --商品详情页浏览会话量
from
-- 先进行一步过滤，对国家进行筛选
(
select
   virtual_goods_id,
   project_name,
   if(country_code in ('DE','FR','PL','SE','GB','MX','BR','IT','CZ','US','NL','ES','CO','CL','CH','AT','AU','NO','DK','RU') ,country_code ,'others') as country_code,
   source_type,  --平台类型（H5，APP,PC）
   event_name,
   session_id,
   list_type,
   page_code
from
dwd.dwd_fd_goods_event_detail
where pt= '${pt}'
) t
group by virtual_goods_id,project_name,country_code,source_type
 )
 t2
on t1.virtual_goods_id=t2.virtual_goods_id and t1.project_name=t2.project_name
   and t1.country_code=t2.country_code and t1.source_type=t2.source_type
--关联dim_fd_goods 获取goods_id,cat_name
left join
    dim.dim_fd_goods t3
on t1.virtual_goods_id= t3.virtual_goods_id

;