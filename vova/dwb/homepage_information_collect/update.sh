#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_homepage_information" \
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
-e "

drop table if exists tmp.fact_cart_cause_v2_glk_cause_homepage;
create table tmp.fact_cart_cause_v2_glk_cause_homepage as
select /*+ REPARTITION(5) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                datasource,
                event_name,
                device_id,
                virtual_goods_id,
                referrer,
                dvce_created_tstamp,
                buyer_id,
                platform,
                country
         from (
                  select datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         page_code,
                         list_type,
                         list_uri,
                         referrer,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         element_type,
                         app_version
                  from dwd.dwd_vova_log_goods_click
                  where pt = '$cur_date'
                  and os_type in('ios','android')
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         event_name,
                         cast(element_id as bigint) virtual_goods_id,
                         device_id,
                         null                       page_code,
                         null                       list_type,
                         null                       list_uri,
                         referrer,
                         buyer_id,
                         os_type as                 platform,
                         geo_country as             country,
                         null                       element_type,
                         null                       app_version
                  from dwd.dwd_vova_log_common_click
                  where pt = '$cur_date'
                    and element_name in ('pdAddToCartClick')
                    and os_type in('ios','android')
              ) t1) t2
where t2.event_name = 'common_click';

drop table if exists tmp.fact_cart_cause_v2_expre_cause_homepage;
create table tmp.fact_cart_cause_v2_expre_cause_homepage as
select /*+ REPARTITION(10) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version
from (
         select COALESCE(page_code, last_value(page_code, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_page_code,
                COALESCE(list_type, last_value(list_type, true)
                                               OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) pre_list_type,
                COALESCE(list_uri, last_value(list_uri, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_list_uri,
                COALESCE(element_type, last_value(element_type, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_element_type,
                COALESCE(app_version, last_value(app_version, true)
                                              OVER (PARTITION BY device_id,virtual_goods_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))  pre_app_version,
                datasource,
                event_name,
                device_id,
                virtual_goods_id,
                referrer,
                buyer_id,
                platform,
                country,
                dvce_created_tstamp
         from (
                  select datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         platform,
                         country,
                         referrer,
                         null page_code,
                         null list_type,
                         null list_uri,
                         null element_type,
                         null app_version
                  from tmp.fact_cart_cause_v2_glk_cause_homepage
                  where pre_page_code is null
                  union all
                  select datasource,
                         dvce_created_tstamp,
                         event_name,
                         virtual_goods_id,
                         device_id,
                         buyer_id,
                         os_type as platform,
                         geo_country as country,
                         referrer,
                         page_code,
                         list_type,
                         list_uri,
                         element_type,
                         app_version
                  from dwd.dwd_vova_log_goods_impression
                  where pt = '$cur_date'
                  and os_type in('ios','android')
              ) t1
     ) t2
where t2.event_name = 'common_click';

drop table if exists tmp.fact_cart_cause_v2_homepage;
create table tmp.fact_cart_cause_v2_homepage as
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version
from tmp.fact_cart_cause_v2_glk_cause_homepage
where pre_page_code is not null
union all
select /*+ REPARTITION(2) */
       datasource,
       event_name,
       virtual_goods_id,
       device_id,
       buyer_id,
       platform,
       country,
       referrer,
       dvce_created_tstamp,
       pre_page_code,
       pre_list_type,
       pre_list_uri,
       pre_element_type,
       pre_app_version
from tmp.fact_cart_cause_v2_glk_cause_homepage;





DROP TABLE IF EXISTS tmp.tmp_homepage_mesg_expre;
CREATE TABLE IF NOT EXISTS tmp.tmp_homepage_mesg_expre as
select
'${cur_date}' cur_date,
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user,
nvl(tmp.element_type,'all') element_type,
'all' is_brand,
'all' first_cat_name,
'all' second_cat_name,
count(distinct entrance_pv) entrance_uv, --首页清单入口曝光数
count(distinct entrance_user) entrance_user, --首页清单入口曝光用户数
count(distinct device_id) uv --首页uv
from (
    select
    nvl(country,'NA') country, --国家
    if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
    if(a.element_type in ('best-sellers','hottest','top-rated') and a.list_type = '/shopping_guide_list' and a.element_name = 'feeds_ad','榜单','trending') element_type, --清单类型
    a.device_id,
    if(a.element_type in ('best-sellers','hottest','top-rated','trending') and a.list_type = '/shopping_guide_list' and a.element_name = 'feeds_ad',concat(a.device_id,a.element_type),null) entrance_pv,
    if(a.element_type in ('best-sellers','hottest','top-rated','trending') and a.list_type = '/shopping_guide_list' and a.element_name = 'feeds_ad',a.device_id,null) entrance_user
    from dwd.dwd_vova_log_impressions a
    left join (select device_id,activate_time from dim.dim_vova_devices) b
    on a.device_id = b.device_id
    where a.page_code = 'homepage'
      and a.pt = '${cur_date}'
    ) tmp
group by cube (country,is_activate_user,element_type);


DROP TABLE IF EXISTS tmp.tmp_homepage_mesg_clk;
CREATE TABLE IF NOT EXISTS tmp.tmp_homepage_mesg_clk as
select
'${cur_date}' cur_date,
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user,
nvl(tmp.element_type,'all') element_type,
'all' is_brand,
'all' first_cat_name,
'all' second_cat_name,
count(distinct pv) clk_cnt --首页清单入口点击
from (
    select
    nvl(country,'NA') country, --国家
    if(to_date(b.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
    if(a.element_type in ('best-sellers','hottest','top-rated'),'榜单','trending') element_type, --清单类型
    concat(a.device_id,a.element_type) pv
    from dwd.dwd_vova_log_common_click a
    left join (select device_id,activate_time from dim.dim_vova_devices) b
    on a.device_id = b.device_id
    where a.element_name = 'feeds_ad'
      and a.element_type in ('best-sellers','hottest','top-rated','trending')
      and a.page_code = 'homepage'
      and a.pt = '${cur_date}'
    ) tmp
group by cube (country,is_activate_user,element_type);


DROP TABLE IF EXISTS tmp.tmp_list_page_tmp2;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_page_tmp2 as
--清单列表曝光
select
/*+ REPARTITION(2) */
nvl(country,'NA') country, --国家
a.virtual_goods_id,
a.device_id,
from_unixtime(unix_timestamp(cast(a.collector_tstamp/1000 as timestamp)),'yyyy-MM-dd HH:mm:ss') collector_tstamp
from dwd.dwd_vova_log_goods_impression a
where a.pt = '${cur_date}'
and a.page_code in ('vovalist_trendinglist','vovalist_goodpage')
;


DROP TABLE IF EXISTS tmp.tmp_list_entrance_tmp2;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_entrance_tmp2 as
--首页清单入口曝光
    select
	/*+ REPARTITION(2) */
		a.country, --国家
		if(a.element_type in ('best-sellers','hottest','top-rated'),'榜单','trending') element_type, --清单类型
		a.device_id,
		a.collector_tstamp
    from dwd.dwd_vova_log_impressions a
    where a.element_name = 'feeds_ad'
      and a.element_type in ('best-sellers','hottest','top-rated','trending')
      and a.page_code = 'homepage'
      and a.pt = '${cur_date}';





--清单页面商品曝光数
DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp4;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp4 as
select
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新激活
nvl(tmp.element_type,'all') element_type, --清单类型
nvl(tmp.is_brand,'all') is_brand,
nvl(tmp.first_cat_name,'all') first_cat_name,
nvl(tmp.second_cat_name,'all') second_cat_name,
count(distinct concat(tmp.virtual_goods_id,tmp.device_id)) list_detail_expre, --清单页面商品曝光数
count(distinct tmp.device_id) list_detail_uv --清单页面uv
from (select
nvl(a.country,'NA') country,
if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
if(c.brand_id >0 ,'Y', 'N') is_brand,
nvl(c.first_cat_name,'NA') first_cat_name,
nvl(c.second_cat_name,'NA') second_cat_name,
nvl(b.element_type,'NA') element_type,
a.device_id,
a.virtual_goods_id,
row_number() over(partition by a.device_id,a.country,a.virtual_goods_id,a.collector_tstamp order by b.collector_tstamp desc) rn
from tmp.tmp_list_page_tmp2 a --清单列表
left join tmp.tmp_list_entrance_tmp2 b --首页清单入口
on a.device_id = b.device_id
and a.country = b.country
join dim.dim_vova_goods c
on a.virtual_goods_id = c.virtual_goods_id
left join (select device_id,activate_time from dim.dim_vova_devices) d
on a.device_id = d.device_id
where a.collector_tstamp > b.collector_tstamp) tmp
where tmp.rn = 1
group by cube(tmp.country,tmp.is_activate_user,tmp.element_type,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name);


DROP TABLE IF EXISTS tmp.tmp_list_page_tmp;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_page_tmp as
--清单列表点击
    select
	/*+ REPARTITION(1) */
		nvl(country,'NA') country, --国家
		a.virtual_goods_id,
		a.device_id list_detail_pv,
		a.collector_tstamp,
		a.device_id
    from dwd.dwd_vova_log_goods_click a
    where a.pt = '${cur_date}'
	and a.page_code in ('vovalist_trendinglist','vovalist_goodpage');

DROP TABLE IF EXISTS tmp.tmp_list_entrance_tmp;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_entrance_tmp as
--首页清单入口点击
    select
	/*+ REPARTITION(1) */
		a.country, --国家
		if(a.element_type in ('best-sellers','hottest','top-rated'),'榜单','trending') element_type, --清单类型
		a.device_id,
		a.collector_tstamp
    from dwd.dwd_vova_log_common_click a
    where a.element_name = 'feeds_ad'
      and a.element_type in ('best-sellers','hottest','top-rated','trending')
      and a.page_code = 'homepage'
      and a.pt = '${cur_date}';


--获取清单列表的清单类型
--清单引导商详页uv
DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp1;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp1 as
select
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新激活
nvl(tmp.element_type,'all') element_type, --清单类型
nvl(tmp.is_brand,'all') is_brand,
nvl(tmp.first_cat_name,'all') first_cat_name,
nvl(tmp.second_cat_name,'all') second_cat_name,
count(distinct tmp.list_detail_pv) list_detail_uv, --清单引导商详页uv
count(tmp.list_detail_pv) list_detail_clk, --清单页面商品点击数
count(tmp.list_detail_pv) list_detail_view --清单引导商详页浏览数
from (select
nvl(a.country,'NA') country,
if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
if(c.brand_id >0 ,'Y', 'N') is_brand,
nvl(c.first_cat_name,'NA') first_cat_name,
nvl(c.second_cat_name,'NA') second_cat_name,
nvl(b.element_type,'NA') element_type,
a.device_id,
a.list_detail_pv,
row_number() over(partition by a.device_id,a.country,a.virtual_goods_id,a.collector_tstamp order by b.collector_tstamp desc) rn
from tmp.tmp_list_page_tmp a --清单列表
left join tmp.tmp_list_entrance_tmp b --首页清单入口
on a.device_id = b.device_id
join dim.dim_vova_goods c
on a.virtual_goods_id = c.virtual_goods_id
left join (select device_id,activate_time from dim.dim_vova_devices) d
on a.device_id = d.device_id
where a.collector_tstamp > b.collector_tstamp) tmp
where tmp.rn = 1
group by cube(tmp.country,tmp.is_activate_user,tmp.element_type,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name);


DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp2_2_tmp;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp2_2_tmp as
select * from tmp.fact_cart_cause_v2_homepage a where a.pre_page_code in ('vovalist_trendinglist','vovalist_goodpage');

DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp2_2;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp2_2 as
--清单引导加车用户数
select
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新激活
nvl(tmp.element_type,'all') element_type, --清单类型
nvl(tmp.is_brand,'all') is_brand,
nvl(tmp.first_cat_name,'all') first_cat_name,
nvl(tmp.second_cat_name,'all') second_cat_name,
count(distinct tmp.device_id) detail_order_uv --清单引导加购成功用户数
from (select
nvl(d.region_code,'NA') country,
if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
if(a.pre_page_code = 'vovalist_trendinglist','trending','榜单') element_type,--a.清单类型,
if(c.brand_id >0 ,'Y', 'N') is_brand,
nvl(c.first_cat_name,'NA') first_cat_name,
nvl(c.second_cat_name,'NA') second_cat_name,
a.device_id
from tmp.tmp_list_detail_tmp2_2_tmp a
join dim.dim_vova_goods c
on a.virtual_goods_id = c.virtual_goods_id
left join (select device_id,region_code,activate_time from dim.dim_vova_devices) d
on a.device_id = d.device_id
) tmp
group by cube(tmp.country,tmp.is_activate_user,tmp.element_type,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name);

DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp2;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp2 as
--清单引导加车成功用户数
select
nvl(tmp.country,'all') country,
nvl(tmp.is_activate_user,'all') is_activate_user, --是否新激活
nvl(tmp.element_type,'all') element_type, --清单类型
nvl(tmp.is_brand,'all') is_brand,
nvl(tmp.first_cat_name,'all') first_cat_name,
nvl(tmp.second_cat_name,'all') second_cat_name,
count(distinct tmp.device_id) detail_order_uv --清单引导加购成功用户数
from (select
nvl(d.region_code,'NA') country,
if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
if(a.pre_page_code = 'vovalist_trendinglist','trending','榜单') element_type,--a.清单类型,
if(c.brand_id >0 ,'Y', 'N') is_brand,
nvl(c.first_cat_name,'NA') first_cat_name,
nvl(c.second_cat_name,'NA') second_cat_name,
a.device_id
from dwd.dwd_vova_fact_cart_cause_v2 a
join dim.dim_vova_goods c
on a.virtual_goods_id = c.virtual_goods_id
left join (select device_id,region_code,activate_time from dim.dim_vova_devices) d
on a.device_id = d.device_id
where a.pre_page_code in ('vovalist_trendinglist','vovalist_goodpage') and a.pt = '${cur_date}') tmp
group by cube(tmp.country,tmp.is_activate_user,tmp.element_type,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name);

--商品维度的clk,expre
DROP TABLE IF EXISTS tmp.tmp_detail_goods_clk_tmp;
CREATE TABLE IF NOT EXISTS tmp.tmp_detail_goods_clk_tmp as
select
	page_code,
	virtual_goods_id,
	count(clk_pv) clk_pv,
	count(distinct clk_pv) clk_uv,
	count(expre_pv) expre_pv
from (
	select
	a.page_code,
	a.virtual_goods_id,
	a.device_id clk_pv,
	null expre_pv
	from dwd.dwd_vova_log_goods_click a
	where a.pt = '${cur_date}' and a.page_code in ('vovalist_trendinglist','vovalist_goodpage')
	union all
    select
		a.page_code,
		a.virtual_goods_id,
		null clk_pv,
		a.device_id expre_pv
    from dwd.dwd_vova_log_goods_impression a
    where a.pt = '${cur_date}' and a.page_code in ('vovalist_trendinglist','vovalist_goodpage')
    ) tmp
group by tmp.page_code,tmp.virtual_goods_id;



DROP TABLE IF EXISTS tmp.tmp_list_detail_tmp3;
CREATE TABLE IF NOT EXISTS tmp.tmp_list_detail_tmp3 as
--清单引导下单用户数
select
	nvl(tmp.country,'all') country,
	nvl(tmp.is_activate_user,'all') is_activate_user, --是否新激活
	nvl(tmp.element_type,'all') element_type, --清单类型
	nvl(tmp.is_brand,'all') is_brand,
	nvl(tmp.first_cat_name,'all') first_cat_name,
	nvl(tmp.second_cat_name,'all') second_cat_name,
	count(distinct tmp.device_id) detail_order_uv, --清单引导下单用户数
	count(distinct tmp.pay_id) detail_pay_uv, --清单引导支付用户数
	sum(gmv) detail_gmv, --清单引导gmv
	sum(gmv) / sum(clk_uv) * sum(clk_pv) / sum(g_cnt) * 10000 gcr --清单引导gcr
from (select
		nvl(d.region_code,'NA') country,
		if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
		if(a.pre_page_code = 'vovalist_trendinglist','trending','榜单') element_type,--a.清单类型,
		if(c.brand_id >0 ,'Y', 'N') is_brand,
		nvl(c.first_cat_name,'NA') first_cat_name,
		nvl(c.second_cat_name,'NA') second_cat_name,
		a.device_id,
		if(e.device_id is not null,a.device_id,null) pay_id,
		if(e.device_id is not null,e.shop_price * e.goods_number + e.shipping_fee,0) gmv,
		c.clk_uv,c.clk_pv,
		c.expre_pv g_cnt
	from dwd.dwd_vova_fact_order_cause_v2 a
	join
	(
	select c.*,f.clk_pv,f.clk_uv,f.expre_pv from
	dim.dim_vova_goods c
	left join tmp.tmp_detail_goods_clk_tmp f
	on c.virtual_goods_id = f.virtual_goods_id
	) c on a.goods_id = c.goods_id
	left join (select device_id,region_code,activate_time from dim.dim_vova_devices) d
		on a.device_id = d.device_id
	left join dwd.dwd_vova_fact_pay e
		on a.order_goods_id = e.order_goods_id
	where a.pre_page_code in ('vovalist_trendinglist','vovalist_goodpage') and a.pt = '${cur_date}') tmp
group by cube(tmp.country,tmp.is_activate_user,tmp.element_type,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name);


insert overwrite table dwb.dwb_vova_homepage_information_collect   PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
	'${cur_date}' cur_date,
	a.country,
	a.is_activate_user,
	a.element_type,
	a.is_brand,
	a.first_cat_name,
	a.second_cat_name,
	b.entrance_uv, --首页清单入口曝光数
	c.clk_cnt, --首页清单入口点击数
	concat(round(c.clk_cnt * 100 / b.entrance_uv,2),'%'), --首页清单入口点击率
	d.list_detail_expre, --清单页面商品曝光数
	e.list_detail_clk, --清单页面商品点击数
	concat(round(e.list_detail_clk * 100 / d.list_detail_expre,2),'%'), --清单页面商品点击率
	e.list_detail_view, --清单引导商详页浏览数
	f.detail_order_uv, --清单引导商详页加车数
	concat(round(f.detail_order_uv * 100 / e.list_detail_view,2),'%'), --清单引导加车率
	b.uv, --首页uv
	b.entrance_user, --首页清单入口曝光用户数
	d.list_detail_uv, --清单页面uv
	e.list_detail_uv, --清单引导商详页uv
	g.detail_order_uv, --清单引导加车成功UV
	a.detail_order_uv, --清单引导下单用户数
	a.detail_pay_uv, --清单引导支付用户数
	round(a.detail_gmv,2), --清单引导gmv
	round(a.gcr,2), --清单引导gcr
	concat(round(b.entrance_user * 100 / b.uv,2),'%'), --首页清单入口曝光率
	concat(round(d.list_detail_uv * 100 / b.entrance_user,2),'%'), -- 首页清单入口用户转化率
	concat(round(g.detail_order_uv * 100 / e.list_detail_uv,2),'%'), --用户加车率
	concat(round(a.detail_pay_uv * 100 / a.detail_order_uv,2),'%'), --下单-支付转化率
	concat(round(a.detail_pay_uv * 100 / e.list_detail_uv,2),'%') --商详-支付转化率
from tmp.tmp_list_detail_tmp3 a
left join tmp.tmp_homepage_mesg_expre b
on a.country = b.country
and a.is_activate_user = b.is_activate_user
and a.element_type = b.element_type
and a.is_brand = b.is_brand
and a.first_cat_name = b.first_cat_name
and a.second_cat_name = b.second_cat_name
left join tmp.tmp_homepage_mesg_clk c
on a.country = c.country
and a.is_activate_user = c.is_activate_user
and a.element_type = c.element_type
and a.is_brand = c.is_brand
and a.first_cat_name = c.first_cat_name
and a.second_cat_name = c.second_cat_name
left join tmp.tmp_list_detail_tmp4 d
on a.country = d.country
and a.is_activate_user = d.is_activate_user
and a.element_type = d.element_type
and a.is_brand = d.is_brand
and a.first_cat_name = d.first_cat_name
and a.second_cat_name = d.second_cat_name
left join tmp.tmp_list_detail_tmp1 e
on a.country = e.country
and a.is_activate_user = e.is_activate_user
and a.element_type = e.element_type
and a.is_brand = e.is_brand
and a.first_cat_name = e.first_cat_name
and a.second_cat_name = e.second_cat_name
left join tmp.tmp_list_detail_tmp2 f
on a.country = f.country
and a.is_activate_user = f.is_activate_user
and a.element_type = f.element_type
and a.is_brand = f.is_brand
and a.first_cat_name = f.first_cat_name
and a.second_cat_name = f.second_cat_name
left join tmp.tmp_list_detail_tmp2_2 g
on a.country = g.country
and a.is_activate_user = g.is_activate_user
and a.element_type = g.element_type
and a.is_brand = g.is_brand
and a.first_cat_name = g.first_cat_name
and a.second_cat_name = g.second_cat_name
;


insert overwrite table dwb.dwb_vova_homepage_information_goods   PARTITION (pt = '${cur_date}')
--清单引导下单商品
select
/*+ REPARTITION(1) */
'${cur_date}' cur_date,
'all' country,
tmp.is_activate_user, --是否新激活
tmp.element_type, --清单类型
tmp.is_brand,
tmp.first_cat_name,
tmp.second_cat_name,
tmp.goods_id,
max(tmp.virtual_goods_id) virtual_goods_id,
round(max(tmp.shop_price),2) shop_price,
round(max(tmp.shipping_fee),2) shipping_fee,
round(max(tmp.price),2) price,
count(*) goods_order_cnt, --创建订单数
count(distinct tmp.device_id) detail_order_uv, --下单用户数
count(distinct tmp.pay_order_id) pay_order_uv, --支付订单数
count(distinct tmp.pay_id) detail_pay_uv, --支付用户数
round(sum(gmv),2) detail_gmv, --gmv
round(nvl((sum(gmv) / sum(clk_uv) * sum(clk_pv)) * 10000 / sum(g_cnt),0),2) gcr, --gcr
round(nvl(sum(gmv) / count(distinct tmp.pay_id),0),2) avg_price,
max(tmp.goods_name) goods_name
from (select
if(to_date(d.activate_time) = '${cur_date}','Y','N') is_activate_user, --是否新激活
if(a.pre_page_code = 'vovalist_trendinglist','trending','榜单') element_type,--a.清单类型,
if(c.brand_id >0 ,'Y', 'N') is_brand,
nvl(c.first_cat_name,'NA') first_cat_name,
nvl(c.second_cat_name,'NA') second_cat_name,
c.virtual_goods_id,
c.shop_price,
c.shipping_fee,
c.shop_price + c.shipping_fee price,
c.goods_name,
a.goods_id,
a.device_id,
if(e.device_id is not null,e.order_goods_id,null) pay_order_id,
if(e.device_id is not null,a.device_id,null) pay_id,
if(e.device_id is not null,e.shop_price * e.goods_number + e.shipping_fee,0) gmv,
c.clk_pv clk_pv,
c.clk_uv clk_uv,
c.expre_pv g_cnt
from dwd.dwd_vova_fact_order_cause_v2 a
left join dim.dim_vova_devices d on a.device_id = d.device_id and a.datasource = d.datasource
left join dwd.dwd_vova_fact_pay e on a.order_goods_id = e.order_goods_id
join
(
select c.*,f.clk_pv,f.clk_uv,f.expre_pv from
dim.dim_vova_goods c
left join tmp.tmp_detail_goods_clk_tmp f
on c.virtual_goods_id = f.virtual_goods_id
) c on a.goods_id = c.goods_id
where a.pre_page_code in ('vovalist_trendinglist','vovalist_homepage') and a.pt = '${cur_date}'
) tmp
group by tmp.is_activate_user,tmp.element_type,tmp.goods_id,tmp.is_brand,tmp.first_cat_name,tmp.second_cat_name
;

"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi









