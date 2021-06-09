#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
now_date=`date -d "-1 days ago ${cur_date}" +%Y-%m-%d`


spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.blacklist.enabled=true" \
--conf "spark.app.name=mlb_vova_user_behavior_link" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 500" \
--conf "spark.sql.shuffle.partitions=500" \
--conf "spark.dynamicAllocation.maxExecutors=250" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e"


insert overwrite table tmp.tmp_user_behavior_link_add_clk_tmp
SELECT event_fingerprint,
        device_id,
        session_id,
        buyer_id,
        gender,
        b.languages_id                      language,
        c.country_id                        geo_country,
        os_type,
        device_model,
        cast(element_id AS BIGINT)          virtual_goods_id,
        page_code,
        list_type,
        collector_tstamp,
        d.goods_id,
        cast(dvce_created_tstamp as bigint) dvce_created_tstamp
 FROM dwd.dwd_vova_log_click_arc a
          left join (select languages_id, languages_code
                     from dim.dim_vova_languages
                     group by languages_id, languages_code) b
                    on a.language = b.languages_code
          left join (select country_id, country_code from dim.dim_vova_region group by country_id, country_code) c
                    on a.geo_country = c.country_code
          left join dim.dim_vova_goods d on cast(a.element_id AS BIGINT) = d.virtual_goods_id
 WHERE pt <= '$cur_date' and pt >= date_sub('$cur_date',1)
   AND event_type = 'goods'
   AND platform = 'mob'
;


insert overwrite table tmp.tmp_vova_user_clk_behave_link_d_distinct partition(pt='$cur_date')
select /*+ REPARTITION(200) */
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
os_type       ,
device_model  ,
goods_id      ,
first_cat_id  ,
second_cat_id ,
cat_id        ,
mct_id        ,
brand_id      ,
shop_price    ,
shipping_fee  ,
click_time    ,
page_code     ,
list_type     ,
clk_from      ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
goods_name    ,
expre_time    ,
is_click      ,
is_order
from tmp.tmp_vova_user_clk_behave_link_d
where pt = '$cur_date'
group by session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
os_type       ,
device_model  ,
goods_id      ,
first_cat_id  ,
second_cat_id ,
cat_id        ,
mct_id        ,
brand_id      ,
shop_price    ,
shipping_fee  ,
click_time    ,
page_code     ,
list_type     ,
clk_from      ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
goods_name    ,
expre_time    ,
is_click      ,
is_order;


insert overwrite table tmp.tmp_user_behavior_link_add_clks_1
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 0
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_2
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 1
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_3
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 2
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;


insert overwrite table tmp.tmp_user_behavior_link_add_clks_4
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 3
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_5
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 4
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_6
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 5
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_7
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 6
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_8
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 7
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_9
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 8
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_10
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from) clk_from,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(b.collector_tstamp as string),14,'0'),cast(b.goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on a.buyer_id = b.buyer_id and a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and abs(hash(nvl(a.session_id,0)) % 10) = 9
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    if(a.page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(a.clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),a.clk_from)      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    lower(trim(regexp_replace(regexp_replace(a.goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order
;

insert overwrite table tmp.tmp_user_behavior_link_add_clks_final
select * from tmp.tmp_user_behavior_link_add_clks_1 union all
select * from tmp.tmp_user_behavior_link_add_clks_2 union all
select * from tmp.tmp_user_behavior_link_add_clks_3 union all
select * from tmp.tmp_user_behavior_link_add_clks_4 union all
select * from tmp.tmp_user_behavior_link_add_clks_5 union all
select * from tmp.tmp_user_behavior_link_add_clks_6 union all
select * from tmp.tmp_user_behavior_link_add_clks_7 union all
select * from tmp.tmp_user_behavior_link_add_clks_8 union all
select * from tmp.tmp_user_behavior_link_add_clks_9 union all
select * from tmp.tmp_user_behavior_link_add_clks_10
;


insert overwrite table tmp.tmp_user_behavior_link_add_cart_1
select  /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,

      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(c.collector_tstamp as string),14,'0'),cast(c.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_cart_list
from tmp.tmp_user_behavior_link_add_clks_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',1)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToCartSuccess')
    ) c on a.buyer_id = c.buyer_id and a.device_id = c.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > c.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 0
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,a.goods_clk_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_cart_2
select   /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,

      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(c.collector_tstamp as string),14,'0'),cast(c.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_cart_list
from tmp.tmp_user_behavior_link_add_clks_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToCartSuccess')
    ) c on a.buyer_id = c.buyer_id and a.device_id = c.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > c.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 1
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,a.goods_clk_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_cart_3
select   /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,

      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(c.collector_tstamp as string),14,'0'),cast(c.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_cart_list
from tmp.tmp_user_behavior_link_add_clks_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToCartSuccess')
    ) c on a.buyer_id = c.buyer_id and a.device_id = c.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > c.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 2
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,a.goods_clk_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_cart_final
select * from tmp.tmp_user_behavior_link_add_cart_1 union all
select * from tmp.tmp_user_behavior_link_add_cart_2 union all
select * from tmp.tmp_user_behavior_link_add_cart_3
;


insert overwrite table tmp.tmp_user_behavior_link_add_wish_1
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(d.collector_tstamp as string),14,'0'),cast(d.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_wish_list
from tmp.tmp_user_behavior_link_add_cart_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToWishlistClick')
    ) d on a.buyer_id = d.buyer_id and a.device_id = d.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > d.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 0
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_wish_2
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(d.collector_tstamp as string),14,'0'),cast(d.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_wish_list
from tmp.tmp_user_behavior_link_add_cart_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToWishlistClick')
    ) d on a.buyer_id = d.buyer_id and a.device_id = d.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > d.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 1
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_wish_3
select /*+ REPARTITION(30) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(d.collector_tstamp as string),14,'0'),cast(d.goods_id as string))
                 )
               )
             ),
        '\\\d+:','') AS goods_wish_list
from tmp.tmp_user_behavior_link_add_cart_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',2)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToWishlistClick')
    ) d on a.buyer_id = d.buyer_id and a.device_id = d.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > d.collector_tstamp
where abs(hash(nvl(a.session_id,0)) % 3) = 2
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list
;

insert overwrite table tmp.tmp_user_behavior_link_add_wish_final
select * from tmp.tmp_user_behavior_link_add_wish_1 union all
select * from tmp.tmp_user_behavior_link_add_wish_2 union all
select * from tmp.tmp_user_behavior_link_add_wish_3
;



set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE mlb.mlb_vova_user_behave_link_d partition(pt='$cur_date',pagecode)
select  /*+ REPARTITION(20) */
    a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
    a.goods_wish_list,
      regexp_replace(
             concat_ws(',',
               sort_array(
                 collect_list(
                   concat_ws(':',lpad(cast(e.collector_tstamp as string),14,'0'),cast(e.search_words as string))
                 )
               )
             ),
        '\\\d+:','') AS search_words_list,page_code pagecode
from tmp.tmp_user_behavior_link_add_wish_final a
left join (
    select device_id,buyer_id,collector_tstamp,
           lower(trim(regexp_replace(regexp_replace(element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) search_words
           from dwd.dwd_vova_log_common_click where pt = '$cur_date'
           and element_name = 'search_confirm'
    ) e on a.buyer_id = e.buyer_id and a.device_id = e.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > e.collector_tstamp
group by     a.session_id    ,
    a.buyer_id      ,
    a.gender        ,
    a.language_id   ,
    a.country_id    ,
    a.os_type       ,
    a.device_model  ,
    a.goods_id      ,
    a.first_cat_id  ,
    a.second_cat_id ,
    a.cat_id        ,
    a.mct_id        ,
    a.brand_id      ,
    a.shop_price    ,
    a.shipping_fee  ,
    a.click_time    ,
    a.page_code     ,
    a.list_type     ,
    a.clk_from      ,
    a.enter_ts      ,
    a.leave_ts      ,
    a.stay_time     ,
    a.is_add_cart   ,
    a.is_collect    ,
    a.device_id     ,
    a.goods_name    ,
    a.expre_time    ,
    a.is_click      ,
    a.is_order,
    a.goods_clk_list,
    a.goods_cart_list,
    a.goods_wish_list
;

"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



