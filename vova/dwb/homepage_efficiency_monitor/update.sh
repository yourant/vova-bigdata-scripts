#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table tmp.tmp_homepage_expre_uv_pv_cc
 select /*+ REPARTITION(1) */ device_id from dwd.dwd_vova_log_common_click where pt = '${cur_date}' group by device_id;
insert overwrite table  tmp.tmp_homepage_expre_uv_pv_gc
select /*+ REPARTITION(1) */ device_id from dwd.dwd_vova_log_goods_click where pt = '${cur_date}' group by device_id;

insert overwrite table  tmp.tmp_homepage_expre_pv
--首页曝光pv
select
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
count(tmp.device_id) homepage_expre_pv --首页曝光pv
from (
select
nvl(a.geo_country,'NA') country, --国家
nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
b.device_id device_id,a.page_code
from dwd.dwd_vova_log_impressions a
join (select a.device_id,a.activate_time,a.current_app_version,cm.channel_en from dim.dim_vova_devices a left join dim.dim_vova_channel_map cm on a.child_channel = cm.channel_name) b
on a.device_id = b.device_id
where a.page_code in ('homepage')
and a.pt = '${cur_date}'
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user);

insert overwrite table  tmp.tmp_homepage_expre_uv_tmp
select /*+ REPARTITION(1) */ geo_country,platform,app_version,page_code,device_id,os_type from dwd.dwd_vova_log_impressions where page_code in ('homepage','launch_gender_select','start_ads','app_start') and pt = '${cur_date}'
group by geo_country,platform,app_version,page_code,device_id,os_type;

insert overwrite table  tmp.tmp_homepage_expre_uv
--首页曝光uv
select
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
count(distinct if(tmp.page_code = 'homepage',tmp.device_id,null)) homepage_expre_uv, --首页曝光uv
count(distinct if(tmp.page_code in ('homepage','launch_gender_select','start_ads','app_start') and clk_device_id is null and e_clk_device_id is null,tmp.device_id,null)) homepage_leave_uv, --跳失人数
count(distinct device_id) dau --dau
from (
select
nvl(a.geo_country,'NA') country, --国家
nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
b.device_id device_id,a.page_code,
d.device_id clk_device_id,
e.device_id e_clk_device_id
from tmp.tmp_homepage_expre_uv_tmp a
join (select a.device_id,a.activate_time,a.current_app_version,cm.channel_en from dim.dim_vova_devices a left join dim.dim_vova_channel_map cm on a.child_channel = cm.channel_name) b
on a.device_id = b.device_id
left join tmp.tmp_homepage_expre_uv_pv_cc d
on a.device_id = d.device_id
left join tmp.tmp_homepage_expre_uv_pv_gc e
on a.device_id = e.device_id
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user);


--新老埋点数据汇总
insert overwrite table  tmp.tmp_homepage_clk_cnt
select /*+ REPARTITION(10) */
distinct geo_country,
platform,
os_type,
os_version,
device_id,
collector_ts collector_tstamp,
buyer_id,
session_id,
element_name,
page_code,
ip
from
dwd.dwd_vova_log_common_click a
where page_code = 'homepage'
and pt = '${cur_date}'
;



insert overwrite table  tmp.tmp_homepage_clk_uv_pv
--首页点击uv,pv
select /*+ REPARTITION(1) */
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
nvl(tmp.element_name,'all') element_name, --活动名称
count(*) homepage_clk_pv, --首页点击pv
count(distinct tmp.device_id) homepage_clk_uv --首页点击uv
from (
select
nvl(a.geo_country,'NA') country, --国家
nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
nvl(a.element_name,'NA') element_name, --活动名称
a.device_id
from tmp.tmp_homepage_clk_cnt a
join (select a.device_id,a.activate_time,a.current_app_version,cm.channel_en from dim.dim_vova_devices a left join dim.dim_vova_channel_map cm on a.child_channel = cm.channel_name) b
on a.device_id = b.device_id
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user,tmp.element_name);


insert overwrite table  tmp.tmp_homepage_income
--首页营收
select
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
sum(goods_number) goods_number, --销售件数
sum(price) gmv, --首页营收
sum(direct_price) direct_gmv --直接营收
from (
select
nvl(c.geo_country,'NA') country, --国家
nvl(c.os_type,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
goods_number,
shop_price * goods_number + shipping_fee price,
if(d.page_code = 'homepage' and d.list_type = '/popular',shop_price * goods_number + shipping_fee,0) direct_price
from dwd.dwd_vova_fact_pay a
join (select
        device_id,
        geo_country,
        os_type,
        page_code
        from (select
        device_id,
        geo_country,
        os_type,
        page_code,
        row_number() over(partition by device_id order by collector_tstamp asc) rn
        from dwd.dwd_vova_log_screen_view
        where pt = '${cur_date}' and page_code != 'app_start'
        ) tmp where tmp.rn = 1
    ) c
on a.device_id = c.device_id
left join (select
        device_id,
        geo_country,
        os_type,
        page_code,list_type
        from (select
        device_id,
        geo_country,
        os_type,
        page_code,list_type,
        row_number() over(partition by device_id order by collector_ts asc) rn
        from dwd.dwd_vova_log_goods_click
        where pt = '${cur_date}'
        ) tmp where tmp.rn = 1
    ) d
on a.device_id = d.device_id
join (select a.device_id,a.activate_time,a.current_app_version,cm.channel_en from dim.dim_vova_devices a left join dim.dim_vova_channel_map cm on a.child_channel = cm.channel_name) b
on a.device_id = b.device_id
where to_date(a.pay_time) = '${cur_date}' and c.page_code = 'homepage'
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user);


insert overwrite table dwb.dwb_vova_homepage_total_index   PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}' cur_date, --日期
a.country, --国家
a.platform, --平台
a.app_version, --版本号
a.channel_en, --渠道
a.is_activate_user, --是否新用户
d.homepage_expre_uv, --首页曝光uv
a.homepage_expre_pv, --首页曝光pv
b.homepage_clk_uv, --首页点击uv
b.homepage_clk_pv, --首页点击pv
round(b.homepage_clk_pv / d.homepage_expre_uv,2) avg_clk,
round(c.gmv,2), --首页营收
round(c.direct_gmv,2), --首页直接营收
concat(round(c.gmv * 100 / b.homepage_clk_pv,2),'%') clk_value,
concat(round(d.homepage_leave_uv * 100 / d.dau,2),'%') leave_rate
from tmp.tmp_homepage_expre_pv a
join tmp.tmp_homepage_expre_uv d
on a.country = d.country
and a.platform = d.platform
and a.app_version = d.app_version
and a.channel_en = d.channel_en
and a.is_activate_user = d.is_activate_user
left join (select * from tmp.tmp_homepage_clk_uv_pv where element_name = 'all') b
on a.country = b.country
and a.platform = b.platform
and a.app_version = b.app_version
and a.channel_en = b.channel_en
and a.is_activate_user = b.is_activate_user
left join tmp.tmp_homepage_income c
on a.country = c.country
and a.platform = c.platform
and a.app_version = c.app_version
and a.channel_en = c.channel_en
and a.is_activate_user = c.is_activate_user
;


insert overwrite table  tmp.tmp_homepage_active_expre_uv_pv
--活动入口曝光uv,pv
select /*+ REPARTITION(2) */
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
nvl(tmp.element_name,'all') element_name, --活动名称
count(tmp.device_id) active_expre_pv, --活动入口曝光pv
count(distinct tmp.device_id) active_expre_uv --活动入口曝光uv
from (
select
nvl(a.geo_country,'NA') country, --国家
nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
nvl(a.element_name,'NA') element_name, --活动名称
a.device_id device_id,page_code
from dwd.dwd_vova_log_impressions a
join (select b.device_id,b.activate_time,b.current_app_version,cm.channel_en from dim.dim_vova_devices b left join dim.dim_vova_channel_map cm on b.child_channel = cm.channel_name) b
on a.device_id = b.device_id
where a.page_code = 'homepage'
and (a.list_type in ('hp_topNavigation','/banner','/hpMultiEntrance','/hpActivityEntrance','/hpmultiGEntrance') or a.element_name like '%click_hp_mainBanner_%' or a.element_name like '%click_hp_multiEntrance_%' or a.element_name like '%click_hp_multiCategory_%'
 or a.element_name like '%click_hp_topNavigation_name&route_sn%' or a.element_name like '%click_hp_activityEntrance_%')
and a.pt = '${cur_date}'
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user,tmp.element_name);


--新老埋点数据汇总
insert overwrite table  tmp.tmp_homepage_clk_cnt_2
select /*+ REPARTITION(10) */
distinct geo_country,
platform,
os_type,
os_version,
device_id,
collector_ts collector_tstamp,
buyer_id,
session_id,
element_name,
page_code,
ip
from
dwd.dwd_vova_log_common_click a
where page_code = 'homepage'
and pt = '${cur_date}'
and (a.element_name like '%click_hp_mainBanner_%' or a.element_name like '%click_hp_multiEntrance_%' or a.element_name like '%click_hp_multiCategory_%'
 or a.element_name like '%click_hp_topNavigation_name&route_sn%' or a.element_name like '%click_hp_activityEntrance_%')
;

insert overwrite table  tmp.tmp_homepage_active_clk_uv_pv
--活动入口点击uv,pv
select
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
nvl(tmp.element_name,'all') element_name, --活动名称
count(*) homepage_clk_pv, --活动入口点击pv
count(distinct tmp.device_id) homepage_clk_uv --活动入口点击uv
from (
select
nvl(a.geo_country,'NA') country, --国家
nvl(case
           when a.platform = 'pc' then 'pc'
           when a.platform = 'web' then 'mob'
           when a.platform = 'mob' and a.os_type = 'android' then 'android'
           when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
           else ''
           end,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
nvl(a.element_name,'NA') element_name, --活动名称
a.device_id
from tmp.tmp_homepage_clk_cnt_2 a
join (select b.device_id,b.activate_time,b.current_app_version,cm.channel_en from dim.dim_vova_devices b left join dim.dim_vova_channel_map cm on b.child_channel = cm.channel_name) b
on a.device_id = b.device_id
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user,tmp.element_name);

insert overwrite table  tmp.tmp_homepage_active_income_bef
select
*
from (select
a.*,b.element_name,
row_number() over(partition by a.datasource,a.device_id,a.order_goods_id,a.goods_id,a.platform,a.pre_page_code,a.pre_list_type order by b.collector_tstamp desc) rn
from tmp.tmp_homepage_clk_cnt_2 b
join dwd.dwd_vova_fact_order_cause_v2 a
on b.device_id = a.device_id where a.pt = '${cur_date}') tmp
where tmp.rn = 1;



insert overwrite table  tmp.tmp_homepage_active_income
--活动入口营收   重新写一个归因带element_name
select
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
nvl(tmp.element_name,'all') element_name, --活动名称
count(distinct device_id) cnt_id,
sum(goods_number) goods_number, --销售件数
sum(price) gmv --首页营收
from (
select
nvl(c.region_code,'NA') country, --国家
nvl(c.datasource,'NA') platform, --平台
nvl(b.current_app_version,'NA') app_version, --版本号
nvl(b.channel_en,'NA') channel_en, --渠道
if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
nvl(a.element_name,'NA') element_name, --活动名称
a.device_id,
goods_number,
shop_price * goods_number + shipping_fee price
from tmp.tmp_homepage_active_income_bef a
join dwd.dwd_vova_fact_pay c
on a.order_goods_id = c.order_goods_id
join (select b.device_id,b.activate_time,b.current_app_version,cm.channel_en from dim.dim_vova_devices b left join dim.dim_vova_channel_map cm on b.child_channel = cm.channel_name) b
on a.device_id = b.device_id
where to_date(c.pay_time) = '${cur_date}'
) tmp
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user,tmp.element_name);


--某入口产生的商详页访问量
insert overwrite table  tmp.tmp_list_section_tmp1
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') country,
nvl(t1.os_type,'NA') platform,
nvl(t1.event_name,'NA') event_name,
from_unixtime(unix_timestamp(cast(t1.collector_tstamp/1000 as timestamp)),'yyyy-MM-dd HH:mm:ss') collector_tstamp,
t1.device_id
from
(
select datasource,event_name,geo_country,os_type,device_id,collector_ts collector_tstamp from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null and page_code='product_detail' and view_type='show'
union
select datasource,event_name,geo_country,os_type,device_id,collector_ts collector_tstamp from dwd.dwd_vova_log_common_click where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null and page_code='product_detail' and view_type='show'
union
select datasource,event_name,geo_country,os_type,device_id,collector_ts collector_tstamp from dwd.dwd_vova_log_order_process where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null and page_code='product_detail' and view_type='show'
) t1;



insert overwrite table  tmp.tmp_list_section_tmp2
select /*+ REPARTITION(100) */
	a.country, --国家
	a.device_id,
	a.element_name,
	a.collector_ts collector_tstamp
from dwd.dwd_vova_log_impressions a
where
a.pt = '${cur_date}'
and a.page_code = 'homepage'
and a.list_type in ('hp_topNavigation','/banner','/hpMultiEntrance','/hpActivityEntrance','/hpmultiGEntrance') or a.element_name like '%click_hp_mainBanner_%' or a.element_name like '%click_hp_multiEntrance_%' or a.element_name like '%click_hp_multiCategory_%'
 or a.element_name like '%click_hp_topNavigation_name&route_sn%' or a.element_name like '%click_hp_activityEntrance_%'
;

insert overwrite table  tmp.tmp_list_section_tmp4
select /*+ REPARTITION(5) */
nvl(tmp.country,'all') country, --国家
nvl(tmp.platform,'all') platform, --平台
nvl(tmp.app_version,'all') app_version, --版本号
nvl(tmp.channel_en,'all') channel_en, --渠道
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新用户
nvl(tmp.element_name,'all') element_name, --活动名称
count(*) channel_pv,
count(distinct b_device_id) channel_uv
from (select
nvl(a.country,'NA') country, --国家
nvl(a.platform,'NA') platform, --平台
nvl(c.current_app_version,'NA') app_version, --版本号
nvl(c.channel_en,'NA') channel_en, --渠道
if(to_date(c.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新用户
nvl(b.element_name,'NA') element_name, --活动名称
b.device_id b_device_id,
row_number() over(partition by a.device_id,a.country,a.datasource,a.platform,a.collector_tstamp order by b.collector_tstamp desc) rn
from tmp.tmp_list_section_tmp1 a --商详页浏览表
join tmp.tmp_list_section_tmp2 b --首页活动入口
on a.device_id = b.device_id
and a.country = b.country
join (select c.device_id,c.activate_time,c.current_app_version,cm.channel_en from dim.dim_vova_devices c left join dim.dim_vova_channel_map cm on c.child_channel = cm.channel_name) c
on a.device_id = c.device_id
where a.collector_tstamp > b.collector_tstamp) tmp
where tmp.rn = 1
group by cube(tmp.country,tmp.platform,tmp.app_version,tmp.channel_en,tmp.is_activate_user,tmp.element_name);



insert overwrite table dwb.dwb_vova_homepage_total_efficiency   PARTITION (pt = '${cur_date}')
select * from (select
/*+ REPARTITION(1) */
'${cur_date}' cur_date, --日期
a.country, --国家
a.platform, --平台
a.app_version, --版本号
a.channel_en, --渠道
a.is_activate_user, --是否新用户
a.element_name, --活动名称
'', --page-code
nvl(b.active_expre_uv,0), --活动入口曝光uv
nvl(b.active_expre_pv,0), --活动入口曝光pv
nvl(a.homepage_clk_uv,0), --活动入口点击uv
nvl(a.homepage_clk_pv,0), --活动入口点击pv
concat(nvl(round(a.homepage_clk_pv * 100 / b.active_expre_pv,2),0),'%') ctr,
nvl(round(c.gmv / a.homepage_clk_uv,3),0), --单活动点击价值uv
nvl(round(c.gmv / a.homepage_clk_pv,3),0), --单活动点击价值pv
concat(nvl(round(a.homepage_clk_pv * 100 / e.homepage_clk_pv,2),0),'%') click_mix,
concat(nvl(round(c.gmv * 100 / f.gmv,2),0),'%') gmv_mix,
concat(nvl(round(c.goods_number * 100 / f.goods_number,2),0),'%') unit_mix,
nvl(round(c.gmv / f.gmv  / (a.homepage_clk_pv / e.homepage_clk_pv),2),0) gmv_change_rate,
nvl(round(c.goods_number / f.goods_number  / (a.homepage_clk_pv / e.homepage_clk_pv),2),0) unit_change_rate,
nvl(c.gmv,0),
concat(nvl(round(d.channel_pv * 100 / g.pv,2),0),'%') dv_mix,
nvl(round(b.active_expre_pv / b.active_expre_uv,2),0) avg_pv,
nvl(round(d.channel_pv  / b.active_expre_uv,3),0) avg_dv,
nvl(c.cnt_id,0)
from tmp.tmp_homepage_active_clk_uv_pv a
left join tmp.tmp_homepage_active_expre_uv_pv b
on a.country = b.country
and a.platform = b.platform
and a.app_version = b.app_version
and a.channel_en = b.channel_en
and a.is_activate_user = b.is_activate_user
and a.element_name = b.element_name
left join tmp.tmp_homepage_active_income c
on a.country = c.country
and a.platform = c.platform
and a.app_version = c.app_version
and a.channel_en = c.channel_en
and a.is_activate_user = c.is_activate_user
and a.element_name = c.element_name
left join tmp.tmp_list_section_tmp4 d
on a.country = d.country
and a.platform = d.platform
and a.app_version = d.app_version
and a.channel_en = d.channel_en
and a.is_activate_user = d.is_activate_user
and a.element_name = d.element_name
left join (select * from tmp.tmp_homepage_clk_uv_pv where element_name = 'all') e
on a.country = e.country
and a.platform = e.platform
and a.app_version = e.app_version
and a.channel_en = e.channel_en
and a.is_activate_user = e.is_activate_user
join (select sum(goods_number) goods_number,sum(shop_price * goods_number + shipping_fee) gmv  from dwd.dwd_vova_fact_pay where to_date(pay_time) = '${cur_date}') f
on 1 = 1
join (select count(*) pv from tmp.tmp_list_section_tmp1) g
on 1 = 1) where element_name != 'all';
"


#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_homepage_efficiency_monitor" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

