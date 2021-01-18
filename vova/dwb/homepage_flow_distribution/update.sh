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

job_name="dwb_vova_homepage_flow_distribution_req5371_chenkai_${cur_date}"

###逻辑sql
sql="
with t1 as(
 select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(element_position, 'all') element_position,
  count(distinct(searchtab_impr_device)) searchtab_impr_uv,
  count(distinct(top_navigation_impr_device)) top_navigation_impr_uv,
  count(distinct(banner_impr_device)) banner_impr_uv,
  count(distinct(hp_multi_entrance_impr_device)) hp_multi_entrance_impr_uv,
  count(distinct(hp_activity_entrance_impr_device)) hp_activity_entrance_impr_uv,
  count(distinct(hpmulti_gentrance_impr_device)) hpmulti_gentrance_impr_uv,
  count(distinct(text_content_popup_impr_device)) text_content_popup_impr_uv,
  count(distinct(pic_content_popup_impr_device)) pic_content_popup_impr_uv
 from (
  select
  fli.pt pt,
  nvl(fli.geo_country, 'NA') region_code,
  fli.os_type platform,
  nvl(dd.main_channel, 'NA') main_channel,
  CASE WHEN datediff(fli.pt, dd.activate_time)=0 THEN 'new'
   WHEN datediff(fli.pt, dd.activate_time)>=1 and datediff(fli.pt,dd.activate_time)<6 THEN '2-7'
   WHEN datediff(fli.pt, dd.activate_time)>=7 and datediff(fli.pt,dd.activate_time)<29 THEN '8-30'
   else '30+' END is_new,
  if(list_type in ('/hp_topNavigation'
                  ,'/banner'
                  ,'/hpMultiEntrance'
                  -- ,'/new_user_7day'
                  ,'/hpActivityEntrance'
                  -- ,'/flash_sale_hp_entrance'
                  ,'/hpmultiGEntrance'
                  -- ,'/recentlyViewed_hp_entrance'
                  ) and fli.element_position is not null, fli.element_position, 'NA') element_position,

  CASE WHEN element_name='searchtab' THEN fli.device_id END searchtab_impr_device,
  CASE WHEN list_type='/hp_topNavigation' THEN fli.device_id END top_navigation_impr_device,
  CASE WHEN list_type='/banner' THEN fli.device_id END banner_impr_device,
  CASE WHEN list_type='/hpMultiEntrance' THEN fli.device_id END hp_multi_entrance_impr_device,
  CASE WHEN list_type='/hpActivityEntrance' THEN fli.device_id END hp_activity_entrance_impr_device,
  CASE WHEN list_type='/hpmultiGEntrance' THEN fli.device_id END hpmulti_gentrance_impr_device,
  CASE WHEN element_name='textContentPopup' THEN fli.device_id END text_content_popup_impr_device,
  CASE WHEN element_name='picContentPopup' THEN fli.device_id END pic_content_popup_impr_device
  from dwd.dwd_vova_log_impressions fli
  LEFT JOIN
  dim.dim_vova_devices dd
  on fli.datasource = dd.datasource and fli.device_id = dd.device_id
  where fli.pt= '${cur_date}' and fli.os_type in ('android','ios') and fli.datasource ='vova'
    and fli.page_code = 'homepage'
 ) tmp1
 group by cube(pt,region_code,platform,main_channel,is_new,element_position)
 HAVING pt != 'all' and element_position != 'NA'
),
t2 as(
 select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(element_position, 'all') element_position,
  count(distinct(searchtab_click_device)) searchtab_click_uv,
  count(distinct(top_navigation_click_device)) top_navigation_click_uv,
  count(distinct(banner_click_device)) banner_click_uv,
  count(distinct(hp_multi_entrance_click_device)) hp_multi_entrance_click_uv,
  count(distinct(hp_activity_entrance_click_device)) hp_activity_entrance_click_uv,
  count(distinct(hpmulti_gentrance_click_device)) hpmulti_gentrance_click_uv,
  count(distinct(positive_button_click_device)) positive_button_click_uv,
  count(distinct(pic_content_popup_but_click_device)) pic_content_popup_but_click_uv,
  count(distinct(hp_coupon_shop_now_click_device)) hp_coupon_shop_now_click_uv

 from (
  select
   flcc.pt pt,
   nvl(flcc.geo_country, 'NA') region_code,
   flcc.os_type platform,
   nvl(dd.main_channel, 'NA') main_channel,
   CASE WHEN datediff(flcc.pt, dd.activate_time)=0 THEN 'new'
    WHEN datediff(flcc.pt, dd.activate_time)>=1 and datediff(flcc.pt,dd.activate_time)<6 THEN '2-7'
    WHEN datediff(flcc.pt, dd.activate_time)>=7 and datediff(flcc.pt,dd.activate_time)<29 THEN '8-30'
    else '30+' END is_new,
   if(flcc.list_type in ('/hp_topNavigation'
                   ,'/banner'
                   ,'/hpMultiEntrance'
                   -- ,'/new_user_7day'
                   ,'/hpActivityEntrance'
                   -- ,'/flash_sale_hp_entrance'
                   ,'/hpmultiGEntrance'
                   -- ,'/recentlyViewed_hp_entrance'
                   ) and flcc.element_position is not null, flcc.element_position, 'NA') element_position,

   CASE WHEN element_name='searchtab' THEN flcc.device_id END searchtab_click_device,
   CASE WHEN list_type='/hp_topNavigation' THEN flcc.device_id END top_navigation_click_device,
   CASE WHEN list_type='/banner' THEN flcc.device_id END banner_click_device,
   CASE WHEN list_type='/hpMultiEntrance' THEN flcc.device_id END hp_multi_entrance_click_device,
   CASE WHEN list_type='/hpActivityEntrance' THEN flcc.device_id END hp_activity_entrance_click_device,
   CASE WHEN list_type='/hpmultiGEntrance' THEN flcc.device_id END hpmulti_gentrance_click_device,
   CASE WHEN element_name='positive_button' THEN flcc.device_id END positive_button_click_device,
   CASE WHEN element_name='picContentPopupBut' THEN flcc.device_id END pic_content_popup_but_click_device,
   CASE WHEN element_name='hpCouponShopNow' THEN flcc.device_id END hp_coupon_shop_now_click_device

  from
   dwd.dwd_vova_log_click_arc flcc
  LEFT JOIN
   dim.dim_vova_devices dd
  on flcc.datasource = dd.datasource and flcc.device_id = dd.device_id
  where flcc.pt = '${cur_date}' and flcc.os_type in ('android','ios') and flcc.datasource ='vova' and flcc.event_type ='normal'
   and (flcc.page_code = 'homepage' or (flcc.page_code = 'homepage_couponPopup' and flcc.element_name='hpCouponShopNow'))
 ) tmp2
 group by cube(pt,region_code,platform,main_channel,is_new,element_position)
 HAVING pt != 'all' and element_position != 'NA'
),
t3 as(
 select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(element_position, 'all') element_position,
  count(distinct(new_user_7day_impr_device)) new_user_7day_impr_uv,
  count(distinct(flash_sale_hp_entrance_impr_device)) flash_sale_hp_entrance_impr_uv,
  count(distinct(recently_viewed_hp_entrance_impr_device)) recently_viewed_hp_entrance_impr_uv
 from (
  select
   flgi.pt pt,
   nvl(flgi.geo_country, 'NA') region_code,
   flgi.os_type platform,
   nvl(dd.main_channel, 'NA') main_channel,
   CASE WHEN datediff(flgi.pt, dd.activate_time)=0 THEN 'new'
    WHEN datediff(flgi.pt, dd.activate_time)>=1 and datediff(flgi.pt,dd.activate_time)<6 THEN '2-7'
    WHEN datediff(flgi.pt, dd.activate_time)>=7 and datediff(flgi.pt,dd.activate_time)<29 THEN '8-30'
    else '30+' END is_new,
   if(flgi.list_type in (
                   '/new_user_7day'
                   ,'/flash_sale_hp_entrance'
                   ,'/recentlyViewed_hp_entrance'
                   ) and flgi.absolute_position is not null, flgi.absolute_position, 'NA') element_position,

   CASE WHEN list_type='/new_user_7day' THEN flgi.device_id END new_user_7day_impr_device,
   CASE WHEN list_type='/flash_sale_hp_entrance' THEN flgi.device_id END flash_sale_hp_entrance_impr_device,
   CASE WHEN list_type='/recentlyViewed_hp_entrance' THEN flgi.device_id END recently_viewed_hp_entrance_impr_device
  from
   dwd.dwd_vova_log_goods_impression flgi
  LEFT JOIN
   dim.dim_vova_devices dd
  on flgi.datasource = dd.datasource and flgi.device_id = dd.device_id
  where flgi.pt = '${cur_date}' and flgi.os_type in ('android','ios') and flgi.datasource ='vova'
   and flgi.page_code='homepage'
   and flgi.list_type in ('/new_user_7day','/flash_sale_hp_entrance','/recentlyViewed_hp_entrance')
 ) tmp3
 group by cube(pt,region_code,platform,main_channel,is_new,element_position)
 HAVING pt != 'all' and element_position != 'NA'
),
t4 as(
 select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  nvl(element_position, 'all') element_position,
  count(distinct(new_user_7day_click_device)) new_user_7day_click_uv,
  count(distinct(flash_sale_hp_entrance_click_device)) flash_sale_hp_entrance_click_uv,
  count(distinct(recently_viewed_hp_entrance_click_device)) recently_viewed_hp_entrance_click_uv
 from (
  select
   flgc.pt pt,
   nvl(flgc.geo_country, 'NA') region_code,
   flgc.os_type platform,
   nvl(dd.main_channel, 'NA') main_channel,
   CASE WHEN datediff(flgc.pt, dd.activate_time)=0 THEN 'new'
    WHEN datediff(flgc.pt, dd.activate_time)>=1 and datediff(flgc.pt,dd.activate_time)<6 THEN '2-7'
    WHEN datediff(flgc.pt, dd.activate_time)>=7 and datediff(flgc.pt,dd.activate_time)<29 THEN '8-30'
    else '30+' END is_new,
   if(flgc.list_type in (
                   '/new_user_7day'
                   ,'/flash_sale_hp_entrance'
                   ,'/recentlyViewed_hp_entrance'
                   ) and flgc.absolute_position is not null, flgc.absolute_position, 'NA') element_position,
   CASE WHEN list_type='/new_user_7day' THEN flgc.device_id END new_user_7day_click_device,
   CASE WHEN list_type='/flash_sale_hp_entrance' THEN flgc.device_id END flash_sale_hp_entrance_click_device,
   CASE WHEN list_type='/recentlyViewed_hp_entrance' THEN flgc.device_id END recently_viewed_hp_entrance_click_device
  from
  dwd.dwd_vova_log_goods_click flgc
  LEFT JOIN
   dim.dim_vova_devices dd
  on flgc.datasource = dd.datasource and flgc.device_id = dd.device_id
  where flgc.pt = '${cur_date}' and flgc.os_type in ('android','ios') and flgc.datasource ='vova'
   and flgc.page_code='homepage'
   and flgc.list_type in ('/new_user_7day','/flash_sale_hp_entrance','/recentlyViewed_hp_entrance')
 ) tmp4
 group by cube(pt,region_code,platform,main_channel,is_new,element_position)
 HAVING pt != 'all' and element_position != 'NA'
),
t5 as(
 select
  nvl(pt, 'all') pt,
  nvl(region_code, 'all') region_code,
  nvl(platform, 'all') platform,
  nvl(main_channel, 'all') main_channel,
  nvl(is_new, 'all') is_new,
  'all' element_position,
  count(distinct(homepage_coupon_popup_device)) homepage_coupon_popup_uv,
  count(distinct(homepage_device)) homepage_uv
 from (
  select
   flsv.pt pt,
   nvl(flsv.geo_country, 'NA') region_code,
   flsv.os_type platform,
   nvl(dd.main_channel, 'NA') main_channel,
   CASE WHEN datediff(flsv.pt, dd.activate_time)=0 THEN 'new'
    WHEN datediff(flsv.pt, dd.activate_time)>=1 and datediff(flsv.pt,dd.activate_time)<6 THEN '2-7'
    WHEN datediff(flsv.pt, dd.activate_time)>=7 and datediff(flsv.pt,dd.activate_time)<29 THEN '8-30'
    else '30+' END is_new,

   if(flsv.page_code='homepage_couponPopup',flsv.device_id, null) homepage_coupon_popup_device,
   if(flsv.page_code='homepage',flsv.device_id, null) homepage_device
  from
   dwd.dwd_vova_log_screen_view flsv
  LEFT JOIN
   dim.dim_vova_devices dd
  on flsv.datasource = flsv.datasource and flsv.device_id = dd.device_id
  where flsv.pt = '${cur_date}' and flsv.os_type in ('android','ios') and flsv.datasource ='vova'
  and flsv.page_code in ('homepage_couponPopup','homepage')
 ) tmp5
 group by cube(pt,region_code,platform,main_channel,is_new,element_position)
 HAVING pt != 'all' and element_position != 'NA'
)

INSERT OVERWRITE TABLE dwb.dwb_vova_homepage_flow_distribution PARTITION (pt='${cur_date}')
 select
 /*+ REPARTITION(1) */
 'vova',
 t1.region_code,
 t1.platform,
 t1.main_channel,
 t1.is_new,
 t1.element_position,

 if(t1.element_position = 'all', searchtab_impr_uv, 'NA'),
 nvl(top_navigation_impr_uv, 0) ,
 nvl(banner_impr_uv, 0) ,
 nvl(hp_multi_entrance_impr_uv, 0) ,
 nvl(hp_activity_entrance_impr_uv, 0) ,
 nvl(hpmulti_gentrance_impr_uv, 0) ,
 if(t1.element_position = 'all', text_content_popup_impr_uv, 'NA'),
 if(t1.element_position = 'all', pic_content_popup_impr_uv, 'NA'),

 if(t1.element_position = 'all', searchtab_click_uv, 'NA'),
 nvl(top_navigation_click_uv, 0),
 nvl(banner_click_uv, 0),
 nvl(hp_multi_entrance_click_uv, 0),
 nvl(hp_activity_entrance_click_uv, 0),
 nvl(hpmulti_gentrance_click_uv, 0),
 if(t1.element_position = 'all', positive_button_click_uv, 'NA'),
 if(t1.element_position = 'all', pic_content_popup_but_click_uv, 'NA'),
 if(t1.element_position = 'all', hp_coupon_shop_now_click_uv, 'NA'),

 nvl(new_user_7day_impr_uv, 0),
 nvl(flash_sale_hp_entrance_impr_uv, 0),
 nvl(recently_viewed_hp_entrance_impr_uv, 0),
 nvl(new_user_7day_click_uv, 0),
 nvl(flash_sale_hp_entrance_click_uv, 0),
 nvl(recently_viewed_hp_entrance_click_uv, 0),
 if(t1.element_position = 'all', t5.homepage_coupon_popup_uv, 'NA'),
 nvl(t5.homepage_uv, 0)
 from
 t1
 LEFT JOIN
 t2
 on t1.pt = t2.pt and t1.region_code = t2.region_code and t1.platform = t2.platform
  and t1.main_channel = t2.main_channel and t1.is_new = t2.is_new and t1.element_position = t2.element_position
 left JOIN
 t3
 on t1.pt = t3.pt and t1.region_code = t3.region_code and t1.platform = t3.platform
  and t1.main_channel = t3.main_channel and t1.is_new = t3.is_new and t1.element_position = t3.element_position
 LEFT JOIN
 t4
 on t1.pt = t4.pt and t1.region_code = t4.region_code and t1.platform = t4.platform
  and t1.main_channel = t4.main_channel and t1.is_new = t4.is_new and t1.element_position = t4.element_position
 LEFT JOIN
 t5
 on t1.pt = t5.pt and t1.region_code = t5.region_code and t1.platform = t5.platform
  and t1.main_channel = t5.main_channel and t1.is_new = t5.is_new
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

