#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_homepage_total_efficiency_v2_req7445_chenkai_${cur_date}"

###逻辑sql
sql="
-- 获取最近8个app_version 和 'all'
create table if not EXISTS tmp.tmp_app_version_req7445_${table_suffix} as
select 'all' as app_version
union all
select
  app_version
from
(
  select
    distinct regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1) app_version
  from
    dwd.dwd_vova_log_screen_view
  where pt = '${cur_date}'
    and datasource = 'vova'
) order by app_version desc limit 9
;

-- 首页入口曝光(theme_activity)
create table if not EXISTS tmp.tmp_entry_impre_list_type_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all')       region_code,
  nvl(platform, 'all')          platform,
  nvl(app_version, 'all')       app_version,
  nvl(is_new, 'all')            is_new,
  nvl(module_name, 'all')       module_name,
  nvl(element_position, 'all')  element_position,
  nvl(element_name, 'all')      element_name,
  count(distinct device_id)     entry_impre_uv,
  count(device_id)              entry_impre_pv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), geo_country, 'others') region_code,
    nvl(os_type, null) platform,
    nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(fli.pt,dd.activate_time)<=0 THEN 'new'
      WHEN datediff(fli.pt,dd.activate_time)>=1 and datediff(fli.pt,dd.activate_time)<6 THEN '2-7'
      WHEN datediff(fli.pt,dd.activate_time)>=7 and datediff(fli.pt,dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    case when list_type = '/hp_topNavigation' then 'topNavigation'
      when list_type = '/banner' then 'banner'
      when list_type = '/hpMultiEntrance' then 'hpMultiEntrance'
      when list_type = '/hpTopEntrance' then 'hpTopEntrance'
      when list_type = '/hpmultiGEntrance' then 'hpmultiGEntrance'
      end module_name,
    nvl(element_position, null) element_position,
    nvl(element_name, null) element_name,
    fli.device_id
  from
    dwd.dwd_vova_log_impressions fli
  left join
    dim.dim_vova_devices dd
  on fli.datasource = dd.datasource
    and fli.device_id = dd.device_id
  where fli.pt= '${cur_date}'
    and fli.datasource = 'vova'
    and fli.page_code = 'homepage'
    and fli.list_type in (
        '/hp_topNavigation',
        '/banner',
        '/hpMultiEntrance',
        '/hpTopEntrance',
        '/hpmultiGEntrance'
    )
    and fli.os_type in ('ios','android')
    and dd.device_id is not null
) t
group by cube(region_code
  ,platform
  ,app_version
  ,is_new
  ,module_name
  ,element_position
  ,element_name)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 入口曝光(非theme_activity)
create table if not EXISTS tmp.tmp_entry_impre_element_name_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all')       region_code,
  nvl(platform, 'all')          platform,
  nvl(app_version, 'all')       app_version,
  nvl(is_new, 'all')            is_new,
  nvl(element_name, 'all')      module_name,
  'all'                         element_position,
  'all'                         element_name,
  count(distinct device_id)     entry_impre_uv,
  count(device_id)              entry_impre_pv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), geo_country, 'others') region_code,
    nvl(os_type, null) platform,
    nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(fli.pt, dd.activate_time)<=0 THEN 'new'
      WHEN datediff(fli.pt, dd.activate_time)>=1 and datediff(fli.pt, dd.activate_time)<6 THEN '2-7'
      WHEN datediff(fli.pt, dd.activate_time)>=7 and datediff(fli.pt, dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    element_name,
    fli.device_id
  from
    dwd.dwd_vova_log_impressions fli
  left join
    dim.dim_vova_devices dd
  on fli.datasource = dd.datasource
    and fli.device_id = dd.device_id
  where fli.pt= '${cur_date}'
    and fli.datasource = 'vova'
    and fli.element_name in (
      'searchtab',
      'NewCommer',
      'homepageFlashSaleEntrance',
      'hpRecentlyViewEntrance'
    )
    and fli.os_type in ('ios','android')
    and dd.device_id is not null
)
group by cube(region_code
  ,platform
  ,app_version
  ,is_new
  ,element_name)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 入口点击(theme_activity)
create table if not EXISTS tmp.tmp_entry_click_list_type_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all')       region_code,
  nvl(platform, 'all')           platform,
  nvl(app_version, 'all')       app_version,
  nvl(is_new, 'all')            is_new,
  nvl(module_name, 'all')       module_name,
  nvl(element_position, 'all')  element_position,
  nvl(element_name, 'all')      element_name,
  count(distinct device_id)     entry_clk_uv,
  count(device_id)              entry_clk_pv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), geo_country, 'others') region_code,
    nvl(os_type, null) platform,
    nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(flca.pt, dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flca.pt, dd.activate_time)>=1 and datediff(flca.pt, dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flca.pt, dd.activate_time)>=7 and datediff(flca.pt, dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    case when list_type = '/hp_topNavigation' then 'topNavigation'
      when list_type = '/banner' then 'banner'
      when list_type = '/hpMultiEntrance' then 'hpMultiEntrance'
      when list_type = '/hpTopEntrance' then 'hpTopEntrance'
      when list_type = '/hpmultiGEntrance' then 'hpmultiGEntrance'
      end module_name,
    nvl(element_position, null) element_position,
    nvl(element_name, null) element_name,
    flca.device_id
  from
    dwd.dwd_vova_log_click_arc flca
  left join
    dim.dim_vova_devices dd
  on flca.device_id = dd.device_id and flca.datasource = dd.datasource
  where flca.pt = '${cur_date}'
    and flca.datasource = 'vova'
    and flca.page_code = 'homepage'
    and flca.list_type in (
      '/hp_topNavigation',
      '/banner',
      '/hpMultiEntrance',
      '/hpTopEntrance',
      '/hpmultiGEntrance'
    )
    and flca.event_type = 'normal'
    and flca.os_type in ('ios','android')
    and dd.device_id is not null
) t
group by cube(region_code
  ,platform
  ,app_version
  ,is_new
  ,module_name
  ,element_position
  ,element_name)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 入口点击(非theme_activity)
create table if not EXISTS tmp.tmp_entry_click_element_name_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all')       region_code,
  nvl(platform, 'all')           platform,
  nvl(app_version, 'all')       app_version,
  nvl(is_new, 'all')            is_new,
  nvl(element_name, 'all')      module_name,
  'all'                         element_position,
  'all'                         element_name,
  count(distinct device_id)     entry_clk_uv,
  count(device_id)              entry_clk_pv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), geo_country, 'others') region_code,
    nvl(os_type, null) platform,
    nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(flca.pt, dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flca.pt, dd.activate_time)>=1 and datediff(flca.pt, dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flca.pt, dd.activate_time)>=7 and datediff(flca.pt, dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    case when element_name = 'searchtab' then 'searchtab'
      when element_name = 'NewCommerButton' or list_type = '/new_user_7day' then 'NewCommer'
      when element_name = 'homepageFlashSaleEntrance' or element_name = 'homepageFlashSaleViewAll' or list_type = '/flash_sale_hp_entrance' then 'homepageFlashSaleEntrance'
      when element_name = 'hpRecentlyViewEntranceViewAll' or list_type = '/recentlyViewed_hp_entrance' then 'hpRecentlyViewEntrance'
      end element_name,
    flca.device_id
  from
    dwd.dwd_vova_log_click_arc flca
  left join
    dim.dim_vova_devices dd
  on flca.device_id = dd.device_id and flca.datasource = dd.datasource
  where flca.pt = '${cur_date}'
    and flca.datasource = 'vova'
    and flca.page_code = 'homepage'
    and (
      (
        flca.element_name in (
        'searchtab',
        'NewCommerButton',
        'homepageFlashSaleEntrance',
        'homepageFlashSaleViewAll',
        'hpRecentlyViewEntranceViewAll'
        )
        and
        flca.event_type = 'normal'
      )
      or
      (
        flca.list_type in (
          '/new_user_7day',
          '/flash_sale_hp_entrance',
          '/recentlyViewed_hp_entrance'
        ) and
        flca.event_type = 'goods'
      )
    )
    and flca.os_type in ('ios','android')
    and dd.device_id is not null
) t
group by cube(region_code
  ,platform
  ,app_version
  ,is_new
  ,element_name)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 首页点击pv
create table if not EXISTS tmp.tmp_homepage_clk_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(app_version, 'all') app_version,
  nvl(is_new, 'all') is_new,
  count(device_id) homepage_clk_pv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), geo_country, 'others') region_code,
    nvl(os_type, null) platform,
    nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(flca.pt, dd.activate_time)<=0 THEN 'new'
      WHEN datediff(flca.pt, dd.activate_time)>=1 and datediff(flca.pt, dd.activate_time)<6 THEN '2-7'
      WHEN datediff(flca.pt, dd.activate_time)>=7 and datediff(flca.pt, dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    flca.device_id device_id
  from
    dwd.dwd_vova_log_click_arc flca
  left join
    dim.dim_vova_devices dd
  on flca.device_id = dd.device_id and flca.datasource = dd.datasource
  where flca.pt = '${cur_date}'
    and flca.datasource = 'vova'
    and flca.page_code = 'homepage'
    and flca.event_type = 'normal'
    and flca.os_type in ('ios','android')
    and dd.device_id is not null
)
group by cube(
  region_code,
  platform,
  app_version,
  is_new)
having app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- list_type: theme_activity 订单归因
create table if not EXISTS tmp.tmp_theme_activity_order_cause_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code, 'all')             region_code,
  nvl(platform, 'all')                platform,
  nvl(app_version, 'all')             app_version,
  nvl(is_new, 'all')                  is_new,
  nvl(module_name, 'all')             module_name,
  nvl(element_position, 'all')        element_position,
  nvl(element_name, 'all')            element_name,
  sum(goods_number)                   activity_goods_number,
  sum(gmv)                            activity_gmv,
  count(distinct pay_device_id)       activity_pay_uv,
  count(distinct first_pay_device_id) activity_first_pay_uv
from
(
  select
  /*+ REPARTITION(100) */
    nvl(if(fp.region_code in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), fp.region_code, 'others'), 'unknown') region_code,
    nvl(fp.platform, null)    platform,
    nvl(t1.app_version, null) app_version,
    CASE WHEN datediff(foc2.pt, dd.activate_time)<=0 THEN 'new'
        WHEN datediff(foc2.pt, dd.activate_time)>=1 and datediff(foc2.pt, dd.activate_time)<6 THEN '2-7'
        WHEN datediff(foc2.pt, dd.activate_time)>=7 and datediff(foc2.pt, dd.activate_time)<29 THEN '8-30'
        else '30+' END is_new,
    case
      -- when t2.list_type = '/hp_topNavigation' then 'topNavigation'
      when t2.list_type = '/banner' then 'banner'
      when t2.list_type = '/hpMultiEntrance' then 'hpMultiEntrance'
      when t2.list_type = '/hpTopEntrance' then 'hpTopEntrance'
      when t2.list_type = '/hpmultiGEntrance' then 'hpmultiGEntrance'
      end module_name,
    nvl(t2.element_position, null) element_position,
    nvl(t2.element_name, null)  element_name,
    fp.goods_number goods_number,
    fp.shipping_fee + fp.shop_price * fp.goods_number gmv,
    foc2.device_id pay_device_id, -- 单频道支付成功用户
    if(to_date(dd.first_pay_time) = to_date(fp.pay_time) and dd.first_order_id = fp.order_id, foc2.device_id, null) first_pay_device_id -- 单频道首次支付成功用户
  from
  (
    select
      distinct
      nvl(regexp_extract(app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), 'unknown') app_version,
      page_code page_code,
      list_type list_type,
      element_type element_type
    from
      dwd.dwd_vova_log_goods_impression
    where pt = '${cur_date}'
      and device_id is not null
      and datasource = 'vova'
      and referrer = 'homepage'
      and page_code = 'theme_activity'
      and element_type != ''
      and element_type is not null
  ) t1
  left join
    dwd.dwd_vova_fact_order_cause_v2 foc2
  on t1.page_code = foc2.pre_page_code
    and t1.list_type = foc2.pre_list_type
    and t1.element_type = foc2.pre_element_type
    -- and t1.app_version = foc2.pre_app_version
  left join
    dwd.dwd_vova_fact_pay fp
  on foc2.order_goods_id = fp.order_goods_id
    and foc2.datasource = fp.datasource
  left join
    dim.dim_vova_devices dd
  on foc2.device_id = dd.device_id
    and foc2.datasource = dd.datasource
  left join
  (
    select
    distinct
      list_type,
      nvl(element_position, null) element_position,
      nvl(element_name, null) element_name,
      split(element_name,'_')[size(split(element_name,'_'))-1] element_type
    from
    dwd.dwd_vova_log_impressions
    where pt = '${cur_date}' and page_code = 'homepage'
      and datasource = 'vova'
      and list_type in (
        -- '/hp_topNavigation',
        '/banner',
        '/hpMultiEntrance',
        '/hpTopEntrance',
        '/hpmultiGEntrance'
        )
  ) t2
  on t1.element_type = t2.element_type
  where foc2.pt <= '${cur_date}' and foc2.pt >= date_sub('${cur_date}', 2)
    and foc2.datasource = 'vova'
    and fp.platform in ('android', 'ios')
    and to_date(fp.pay_time) = '${cur_date}'
    and t2.element_name is not null
    -- and fp.region_code is not null
) t
group by cube(
  region_code,
  platform,
  app_version,
  is_new,
  module_name,
  element_position,
  element_name
)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- element_name: 非theme_activity 归因
create table if not EXISTS tmp.tmp_no_theme_activity_order_cause_req7445_${table_suffix} as
select
/*+ REPARTITION(2) */
  nvl(region_code,'all')        region_code,
  nvl(platform,'all')           platform,
  nvl(app_version,'all')        app_version,
  nvl(is_new,'all')             is_new,
  nvl(element_name,'all')       module_name,
  'all'                         element_position,
  'all'                         element_name,
  sum(goods_number)             activity_goods_number,
  sum(gmv)                      activity_gmv,
  count(distinct pay_device_id) activity_pay_uv,
  count(distinct first_pay_device_id) activity_first_pay_uv
from
(
select
  nvl(if(fp.region_code in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), fp.region_code, 'others'), 'unknown') region_code,
  nvl(fp.platform, null) platform,
  nvl(regexp_extract(foc2.pre_app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), 'unknown') app_version,
  CASE WHEN datediff(foc2.pt, dd.activate_time)<=0 THEN 'new'
      WHEN datediff(foc2.pt, dd.activate_time)>=1 and datediff(foc2.pt, dd.activate_time)<6 THEN '2-7'
      WHEN datediff(foc2.pt, dd.activate_time)>=7 and datediff(foc2.pt, dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
  case when foc2.pre_page_code = 'theme_activity' and foc2.pre_list_type like '/newuser_%' then 'NewCommer'
    when foc2.pre_page_code = 'flashsale' and foc2.pre_list_type in ('/onsale','/upcoming') then 'homepageFlashSaleEntrance'
    when foc2.pre_page_code = 'homepage' and foc2.pre_list_type = '/recentlyViewed_hp_entrance' then 'hpRecentlyViewEntrance'
    when foc2.pre_page_code = 'search_result' and foc2.pre_list_type like '/search_result%' then 'searchtab'
    when foc2.pre_page_code = 'homepage' and foc2.pre_list_type like '/product_list%' then 'topNavigation'
    end element_name,
  fp.goods_number,
  fp.shipping_fee + fp.shop_price * fp.goods_number gmv,
  foc2.device_id pay_device_id, -- 单频道支付成功用户
  if(to_date(dd.first_pay_time) = to_date(fp.pay_time) and dd.first_order_id = fp.order_id, foc2.device_id, null) first_pay_device_id -- 单频道首次支付成功用户
from
  dwd.dwd_vova_fact_order_cause_v2 foc2
left join
  dwd.dwd_vova_fact_pay fp
on foc2.order_goods_id = fp.order_goods_id
  and foc2.datasource = fp.datasource
left join
  dim.dim_vova_devices dd
on foc2.device_id = dd.device_id
  and foc2.datasource = dd.datasource
where foc2.pt <= '${cur_date}' and foc2.pt >= date_sub('${cur_date}', 2)
  and foc2.datasource = 'vova'
  and fp.platform in ('android', 'ios')
  and to_date(fp.pay_time) = '${cur_date}'
  and dd.device_id is not null
  -- and fp.region_code is not null
  -- and regexp_extract(foc2.pre_app_version,'(\\d+\\.\\d+)\\.(\\d+)',1) is not null
  and (
   (foc2.pre_page_code = 'theme_activity' and foc2.pre_list_type in ('/newuser_plist','/newuser_flashsale','/newuser_feeds'))
   or
   (foc2.pre_page_code = 'flashsale' and foc2.pre_list_type in ('/onsale','/upcoming'))
   or
   (foc2.pre_page_code = 'recently_view' and foc2.pre_list_type = '/recently_view')
   or
   (foc2.pre_page_code = 'search_result' and foc2.pre_list_type like '/search_result%')
   or
   (foc2.pre_page_code = 'homepage' and foc2.pre_list_type like '/product_list%')
  )
)
group by cube(
  region_code,
  platform,
  app_version,
  is_new,
  element_name
)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 非theme_activity 会场 pv,uv screen_view
create table if not exists tmp.tmp_no_theme_activity_pv_req7445_${table_suffix} as
select
/*+ REPARTITION(1) */
  nvl(region_code,'all') region_code,
  nvl(platform,'all')    platform,
  nvl(app_version,'all') app_version,
  nvl(is_new,'all')      is_new,
  nvl(element_name,'all') module_name,
  'all'                   element_position,
  'all'                   element_name,
  count(device_id) activity_pv,
  count(distinct device_id) activity_uv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(flsv.geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), flsv.geo_country, 'others') region_code,
    nvl(flsv.platform, null) platform,
    nvl(regexp_extract(flsv.app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(flsv.pt, dd.activate_time)<=0 THEN 'new'
        WHEN datediff(flsv.pt, dd.activate_time)>=1 and datediff(flsv.pt, dd.activate_time)<6 THEN '2-7'
        WHEN datediff(flsv.pt, dd.activate_time)>=7 and datediff(flsv.pt, dd.activate_time)<29 THEN '8-30'
        else '30+' END is_new,
    case when flsv.page_code = 'flashsale' then 'homepageFlashSaleEntrance'
      when flsv.page_code = 'search_result' then 'searchtab'
      when flsv.page_code = 'recently_view' then 'hpRecentlyViewEntrance'
      -- when flsv.page_code = 'product_list' then 'topNavigation'
      end element_name,
    flsv.device_id
  from
    dwd.dwd_vova_log_screen_view flsv
  left join
    dim.dim_vova_devices dd
  on flsv.device_id = dd.device_id
    and flsv.datasource = dd.datasource
  where flsv.pt = '${cur_date}'
    and flsv.datasource = 'vova'
    and dd.device_id is not null
    and flsv.page_code in (
      'flashsale'
      ,'search_result'
      ,'recently_view'
      -- ,'product_list'
    )
)
group by cube(
  region_code,
  platform,
  app_version,
  is_new,
  element_name
)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 非theme_activity 会场 dv page_view
create table if not exists tmp.tmp_no_theme_activity_dv_req7445_${table_suffix} as
select
/*+ REPARTITION(1) */
  nvl(region_code,'all')  region_code,
  nvl(platform,'all')     platform,
  nvl(app_version,'all')  app_version,
  nvl(is_new,'all')       is_new,
  nvl(element_name,'all') module_name,
  'all'                   element_position,
  'all'                   element_name,
  count(device_id)        activity_dv
from
(
  select
  /*+ REPARTITION(100) */
    if(nvl(flsv.geo_country, 'others') in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), flsv.geo_country, 'others') region_code,
    nvl(flsv.platform, null) platform,
    nvl(regexp_extract(flsv.app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), null) app_version,
    CASE WHEN datediff(flsv.pt, dd.activate_time)<=0 THEN 'new'
        WHEN datediff(flsv.pt, dd.activate_time)>=1 and datediff(flsv.pt, dd.activate_time)<6 THEN '2-7'
        WHEN datediff(flsv.pt, dd.activate_time)>=7 and datediff(flsv.pt, dd.activate_time)<29 THEN '8-30'
        else '30+' END is_new,
    case when flsv.referrer like '%flashsale%' then 'homepageFlashSaleEntrance'
      when flsv.referrer like '%search_result%' then 'searchtab'
      when flsv.referrer like '%recently_view%' then 'hpRecentlyViewEntrance'
      -- when flsv.referrer like '%product_list%' then 'topNavigation'
      end element_name,
    flsv.device_id
  from
    dwd.dwd_vova_log_screen_view flsv
  left join
    dim.dim_vova_devices dd
  on flsv.device_id = dd.device_id
    and flsv.datasource = dd.datasource
  where flsv.pt = '${cur_date}'
    and flsv.datasource = 'vova'
    and dd.device_id is not null
    and flsv.page_code = 'product_detail'
    and (flsv.referrer like '%flashsale%'
      or flsv.referrer like '%search_result%'
      or flsv.referrer like '%recently_view%'
      -- or flsv.referrer like '%product_list%'
    )
)
group by cube(
  region_code,
  platform,
  app_version,
  is_new,
  element_name
)
having module_name != 'all'
  and app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 全站销售情况
create table if not exists tmp.tmp_vova_order_req7445_${table_suffix} as
select
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all')    platform,
  nvl(app_version, 'all') app_version,
  nvl(is_new, 'all')      is_new,
  sum(goods_number) all_station_goods_number,
  sum(gmv) all_station_gmv
from
(
  select
    nvl(if(fp.region_code in ('FR','IT','DE','ES','GB','US','TW','CH','CZ','PL','SK','AT','BR','AU','SE','NO'), fp.region_code, 'others'), 'unknown') region_code,
    nvl(fp.platform, null) platform,
    nvl(regexp_extract(foc2.pre_app_version,'(\\\\d+\\\\.\\\\d+)\\\\.(\\\\d+)',1), 'unknown') app_version,
    CASE WHEN datediff(to_date(fp.pay_time), dd.activate_time)<=0 THEN 'new'
      WHEN datediff(to_date(fp.pay_time), dd.activate_time)>=1 and datediff(to_date(fp.pay_time), dd.activate_time)<6 THEN '2-7'
      WHEN datediff(to_date(fp.pay_time), dd.activate_time)>=7 and datediff(to_date(fp.pay_time), dd.activate_time)<29 THEN '8-30'
      else '30+' END is_new,
    fp.goods_number,
    fp.shipping_fee + fp.shop_price * fp.goods_number gmv,
    fp.device_id pay_device_id
  from
    dwd.dwd_vova_fact_pay fp
  left join
    dwd.dwd_vova_fact_order_cause_v2 foc2
  on fp.datasource = foc2.datasource
    and fp.order_goods_id = foc2.order_goods_id
  left join
    dim.dim_vova_devices dd
  on fp.datasource = dd.datasource
    and fp.device_id = dd.device_id
  where to_date(fp.pay_time) = '${cur_date}'
    and fp.datasource = 'vova'
    and fp.platform in ('android', 'ios')
    -- and regexp_extract(foc2.pre_app_version,'(\\d+\\.\\d+)\\.(\\d+)',1) is not null
    -- and fp.region_code is not null
)
group by cube(
  region_code,
  platform,
  app_version,
  is_new
)
having app_version in (select app_version from tmp.tmp_app_version_req7445_${table_suffix})
;

-- 聚合
insert OVERWRITE TABLE dwb.dwb_vova_homepage_total_efficiency_v2 PARTITION (pt='${cur_date}')
select
/*+ REPARTITION(2) */
  tmp_impre.region_code,
  tmp_impre.platform,
  tmp_impre.app_version,
  tmp_impre.is_new,
  tmp_impre.module_name,
  tmp_impre.element_position,
  tmp_impre.element_name,
  entry_impre_uv,
  entry_impre_pv,
  entry_clk_uv,
  entry_clk_pv,
  homepage_clk_pv,
  activity_gmv,
  all_station_gmv,
  activity_goods_number,
  all_station_goods_number,
  activity_pv,
  activity_uv,
  activity_dv,
  activity_first_pay_uv,
  activity_pay_uv
from
(
  select
    region_code,
    platform,
    app_version,
    is_new,
    module_name,
    element_position,
    element_name,
    entry_impre_uv,
    entry_impre_pv
  from
  tmp.tmp_entry_impre_list_type_req7445_${table_suffix}
  union all
  select
  *
  from
  tmp.tmp_entry_impre_element_name_req7445_${table_suffix}
) tmp_impre
left join
(
  select
    region_code,
    platform,
    app_version,
    is_new,
    module_name,
    element_position,
    element_name,
    activity_goods_number,
    activity_gmv,
    activity_pay_uv,
    activity_first_pay_uv
  from
  tmp.tmp_no_theme_activity_order_cause_req7445_${table_suffix}
  union all
  select
  *
  from
  tmp.tmp_theme_activity_order_cause_req7445_${table_suffix}
) tmp_order
on tmp_impre.region_code = tmp_order.region_code
  and tmp_impre.platform = tmp_order.platform
  and tmp_impre.app_version = tmp_order.app_version
  and tmp_impre.is_new = tmp_order.is_new
  and tmp_impre.module_name = tmp_order.module_name
  and tmp_impre.element_position = tmp_order.element_position
  and tmp_impre.element_name = tmp_order.element_name
left join
(
  select
    region_code,
    platform,
    app_version,
    is_new,
    module_name,
    element_position,
    element_name,
    entry_clk_uv,
    entry_clk_pv
  from
  tmp.tmp_entry_click_list_type_req7445_${table_suffix}
  union all
  select
  *
  from
  tmp.tmp_entry_click_element_name_req7445_${table_suffix}
) tmp_click
on tmp_impre.region_code = tmp_click.region_code
  and tmp_impre.platform = tmp_click.platform
  and tmp_impre.app_version = tmp_click.app_version
  and tmp_impre.is_new = tmp_click.is_new
  and tmp_impre.module_name = tmp_click.module_name
  and tmp_impre.element_position = tmp_click.element_position
  and tmp_impre.element_name = tmp_click.element_name

left join
tmp.tmp_no_theme_activity_pv_req7445_${table_suffix} tmp_no_pv
on tmp_impre.region_code = tmp_no_pv.region_code
  and tmp_impre.platform = tmp_no_pv.platform
  and tmp_impre.app_version = tmp_no_pv.app_version
  and tmp_impre.is_new = tmp_no_pv.is_new
  and tmp_impre.module_name = tmp_no_pv.module_name
  and tmp_impre.element_position = tmp_no_pv.element_position
  and tmp_impre.element_name = tmp_no_pv.element_name
left join
tmp.tmp_no_theme_activity_dv_req7445_${table_suffix} tmp_no_dv
on tmp_impre.region_code = tmp_no_dv.region_code
  and tmp_impre.platform = tmp_no_dv.platform
  and tmp_impre.app_version = tmp_no_dv.app_version
  and tmp_impre.is_new = tmp_no_dv.is_new
  and tmp_impre.module_name = tmp_no_dv.module_name
  and tmp_impre.element_position = tmp_no_dv.element_position
  and tmp_impre.element_name = tmp_no_dv.element_name

left join
tmp.tmp_homepage_clk_req7445_${table_suffix} tmp_homepage_clk
on tmp_impre.region_code = tmp_homepage_clk.region_code
  and tmp_impre.platform = tmp_homepage_clk.platform
  and tmp_impre.app_version = tmp_homepage_clk.app_version
  and tmp_impre.is_new = tmp_homepage_clk.is_new
left join
tmp.tmp_vova_order_req7445_${table_suffix} tmp_vova_order
on tmp_impre.region_code = tmp_vova_order.region_code
  and tmp_impre.platform = tmp_vova_order.platform
  and tmp_impre.app_version = tmp_vova_order.app_version
  and tmp_impre.is_new = tmp_vova_order.is_new
;

drop table if exists tmp.tmp_app_version_req7445_${table_suffix};
drop table if exists tmp.tmp_entry_impre_list_type_req7445_${table_suffix};
drop table if exists tmp.tmp_entry_impre_element_name_req7445_${table_suffix};
drop table if exists tmp.tmp_entry_click_list_type_req7445_${table_suffix};
drop table if exists tmp.tmp_entry_click_element_name_req7445_${table_suffix};
drop table if exists tmp.tmp_no_theme_activity_pv_req7445_${table_suffix};
drop table if exists tmp.tmp_no_theme_activity_dv_req7445_${table_suffix};
drop table if exists tmp.tmp_no_theme_activity_order_cause_req7445_${table_suffix};
drop table if exists tmp.tmp_theme_activity_order_cause_req7445_${table_suffix};
drop table if exists tmp.tmp_homepage_clk_req7445_${table_suffix};
drop table if exists tmp.tmp_vova_order_req7445_${table_suffix};
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 12G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`