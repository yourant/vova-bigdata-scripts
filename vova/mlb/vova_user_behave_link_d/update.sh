#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
now_date=`date -d "-1 days ago ${cur_date}" +%Y-%m-%d`



spark-sql \
--driver-memory 8G \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=250" \
--conf "spark.app.name=mlb_vova_user_behavior_link" \
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
is_order,
geo_city,
geo_latitude,
geo_longitude,
geo_region,
absolute_position,
imsi
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
is_order,
geo_city,
geo_latitude,
geo_longitude,
geo_region,
absolute_position,
imsi;



insert overwrite table tmp.tmp_user_behavior_link_add_clks_final
select /*+ REPARTITION(200) */
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
first(os_type)      ,
first(device_model)  ,
goods_id      ,
first(first_cat_id)  ,
first(second_cat_id) ,
first(cat_id)        ,
first(mct_id)        ,
first(brand_id)      ,
first(shop_price)    ,
first(shipping_fee)  ,
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
first(goods_name)   ,
expre_time    ,
is_click      ,
is_order,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(collector_tstamp as string),14,'0'),cast(goods_id_b as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_clk_list,
first(geo_city),
first(geo_latitude),
first(geo_longitude),
first(geo_region),
first(absolute_position),
first(imsi)
    from
(select
       t1.*,
       row_number() over (partition by buyer_id,goods_id,expre_time order by (unix_timestamp(expre_time,'yyyy-MM-dd HH:mm:ss')-collector_tstamp) desc) rk
       from
(select
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
    b.collector_tstamp,
    b.goods_id as goods_id_b,
a.geo_city,
a.geo_latitude,
a.geo_longitude,
a.geo_region,
a.absolute_position,
a.imsi
from tmp.tmp_vova_user_clk_behave_link_d_distinct a
left join tmp.tmp_user_behavior_link_add_clk_tmp b on  a.device_id = b.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > b.collector_tstamp
where a.pt = '$cur_date' and  a.device_id is not null)t1)
where rk <=30
group by
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
goods_id      ,
click_time    ,
page_code     ,
list_type     ,
clk_from     ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
expre_time    ,
is_click      ,
is_order
;


insert overwrite table tmp.tmp_user_behavior_link_add_cart_final
select /*+ REPARTITION(200) */
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
first(os_type)       ,
first(device_model)  ,
goods_id      ,
first(first_cat_id)  ,
first(second_cat_id) ,
first(cat_id)        ,
first(mct_id)        ,
first(brand_id)      ,
first(shop_price)    ,
first(shipping_fee)  ,
click_time    ,
page_code     ,
list_type     ,
clk_from     ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
first(goods_name)   ,
expre_time    ,
is_click      ,
is_order,
goods_clk_list,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(c_collector_tstamp as string),14,'0'),cast(c_goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_cart_list,
first(geo_city),
first(geo_latitude),
first(geo_longitude),
first(geo_region),
first(absolute_position),
first(imsi)
    from
(select
       t1.*,
       row_number() over (partition by buyer_id,goods_id,expre_time order by (unix_timestamp(expre_time,'yyyy-MM-dd HH:mm:ss')-c_collector_tstamp) desc) rk
       from
(select
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
    c.collector_tstamp c_collector_tstamp,c.goods_id c_goods_id,
a.geo_city,
a.geo_latitude,
a.geo_longitude,
a.geo_region,
a.absolute_position,
a.imsi
from tmp.tmp_user_behavior_link_add_clks_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',1)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToCartSuccess')
    ) c on a.buyer_id = c.buyer_id and a.device_id = c.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > c.collector_tstamp
)t1)
where rk <=30
group by
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
goods_id      ,
click_time    ,
page_code     ,
list_type     ,
clk_from     ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
expre_time    ,
is_click      ,
is_order,
goods_clk_list
;


insert overwrite table tmp.tmp_user_behavior_link_add_wish_final
select /*+ REPARTITION(200) */
session_id    ,
buyer_id      ,
gender        ,
language_id   ,
country_id    ,
first(os_type)       ,
first(device_model)  ,
goods_id      ,
first(first_cat_id)  ,
first(second_cat_id) ,
first(cat_id)        ,
first(mct_id)        ,
first(brand_id)      ,
first(shop_price)    ,
first(shipping_fee)  ,
click_time    ,
page_code     ,
list_type     ,
clk_from     ,
enter_ts      ,
leave_ts      ,
stay_time     ,
is_add_cart   ,
is_collect    ,
device_id     ,
first(goods_name)   ,
expre_time    ,
is_click      ,
is_order,
goods_clk_list,
goods_cart_list,
       regexp_replace(
                     concat_ws(',',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(d_collector_tstamp as string),14,'0'),cast(d_goods_id as string))
                         )
                       )
                     ),
        '\\\d+:','') AS goods_cart_list,
first(geo_city),
first(geo_latitude),
first(geo_longitude),
first(geo_region),
first(absolute_position),
first(imsi)
    from
(select
       t1.*,
       row_number() over (partition by buyer_id,goods_id,expre_time order by (unix_timestamp(expre_time,'yyyy-MM-dd HH:mm:ss')-d_collector_tstamp) desc) rk
       from
(select
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
    d.collector_tstamp d_collector_tstamp,
    d.goods_id d_goods_id,
a.geo_city,
a.geo_latitude,
a.geo_longitude,
a.geo_region,
a.absolute_position,
a.imsi
from tmp.tmp_user_behavior_link_add_cart_final a
left join (select a.device_id,a.buyer_id,a.collector_tstamp,b.goods_id from dwd.dwd_vova_log_common_click a
            left join dim.dim_vova_goods b on cast(a.element_id AS BIGINT) = b.virtual_goods_id
            where a.pt <= '$cur_date' and a.pt >= date_sub('$cur_date',1)
           and a.platform = 'mob' and a.page_code = 'product_detail'
           and a.element_name in ('pdAddToWishlistClick')
    ) d on a.buyer_id = d.buyer_id and a.device_id = d.device_id and unix_timestamp(a.expre_time,'yyyy-MM-dd HH:mm:ss') * 1000 > d.collector_tstamp
)t1) t
where rk <=30
group by     session_id    ,
    buyer_id      ,
    gender        ,
    language_id   ,
    country_id    ,
    goods_id      ,
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
    expre_time    ,
    is_click      ,
    is_order,
    goods_clk_list,
    goods_cart_list
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
    first(a.os_type)       ,
    first(a.device_model)  ,
    a.goods_id      ,
    first(a.first_cat_id)  ,
    first(a.second_cat_id) ,
    first(a.cat_id)        ,
    first(a.mct_id)        ,
    first(a.brand_id)      ,
    first(a.shop_price)    ,
    first(a.shipping_fee)  ,
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
    first(a.goods_name)    ,
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
        '\\\d+:','') AS search_words_list,
        first(geo_city),
first(geo_latitude),
first(geo_longitude),
first(geo_region),
first(absolute_position),
first(imsi),
        page_code pagecode
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
    a.goods_id      ,
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



