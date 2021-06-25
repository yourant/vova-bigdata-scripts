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
--conf "spark.app.name=mlb_vova_user_behavior_link_before" \
-e "
insert overwrite table tmp.tmp_vova_product_detail_with_clk_from
select /*+ REPARTITION(40) */ * from (
                  select t1.event_fingerprint,
                         t1.buyer_id,
                         t1.device_id,
                         t1.virtual_goods_id,
                         t1.page_code,
                         t1.list_type,
                         t1.collector_tstamp,
                         t1.dvce_created_tstamp,
                         t1.session_id,
                         t1.gender,
                         t1.language,
                         t1.geo_country,
                         t1.os_type,
                         t1.device_model,
                         row_number()
                                 over (partition by t1.buyer_id,t1.device_id,t1.virtual_goods_id,t1.collector_tstamp order by (t1.dvce_created_tstamp - t2.dvce_created_tstamp) ) as rn,
                         t2.enter_ts,
                         t2.leave_ts
                  from (
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
                                  collector_ts                        collector_tstamp,
                                  cast(dvce_created_tstamp as bigint) dvce_created_tstamp
                           FROM dwd.dwd_vova_log_click_arc a
                                    left join (select languages_id, languages_code
                                               from dim.dim_vova_languages
                                               group by languages_id, languages_code) b
                                              on a.language = b.languages_code
                                    left join (select country_id, country_code
                                               from dim.dim_vova_region
                                               group by country_id, country_code) c
                                              on a.geo_country = c.country_code
                           WHERE pt = '${cur_date}'
                             AND event_type = 'goods' and a.platform = 'mob'
                       ) t1
                           left join (
                      SELECT buyer_id,
                             device_id,
                             virtual_goods_id,
                             enter_ts,
                             leave_ts,
                             cast(dvce_created_tstamp as bigint) dvce_created_tstamp,
                             page_code
                      FROM dwd.dwd_vova_log_page_view_arc
                      WHERE pt = '${cur_date}'
                        AND page_code = 'product_detail'
                        AND view_type = 'hide' and platform = 'mob'
                  ) t2
                                     on t1.buyer_id = t2.buyer_id and t1.device_id = t2.device_id
                                         and t1.virtual_goods_id = t2.virtual_goods_id
                                         and t1.dvce_created_tstamp <= t2.dvce_created_tstamp
              ) t where t.rn = 1

;


insert overwrite table tmp.tmp_vova_expre_with_clk_from
select /*+ REPARTITION(200) */ * from (
                  select t1.buyer_id,
                         t1.device_id,
                         t1.event_fingerprint,
                         cast(t1.element_id AS BIGINT)                                                                                                virtual_goods_id,
                         t1.page_code,
                         t1.list_type,
                         t1.collector_ts                                                                                                              collector_tstamp,
                         t1.session_id,
                         t1.gender,
                         t1.language,
                         t1.geo_country,
                         t1.os_type,
                         t1.platform,
                         t1.device_model,
                         t2.element_id,
                         t1.dvce_created_tstamp,
                         row_number()
                                 over (partition by t1.event_fingerprint,t1.element_id order by (t1.dvce_created_tstamp - t2.dvce_created_tstamp)) as rn,
                         t1.geo_city,
                         t1.geo_latitude,
                         t1.geo_longitude,
                         t1.geo_region,
                         t1.absolute_position,
                         t1.imsi,
                         t2.collector_tstamp search_words_or_goods_clk_ts
                  from (select a.buyer_id,
                               a.event_fingerprint,
                               a.device_id,
                               a.element_id,
                               a.page_code,
                               a.list_type,
                               a.collector_ts,
                               a.session_id,
                               a.gender,
                               b.languages_id     language,
                               c.country_id       geo_country,
                               a.os_type,
                               a.platform,
                               a.device_model,
                               a.dvce_created_tstamp,
                               a.geo_city,
                               a.geo_latitude,
                               a.geo_longitude,
                               a.geo_region,
                               a.element_position absolute_position,
                               a.imsi
                        from dwd.dwd_vova_log_impressions_arc a
                                 left join (select languages_id, languages_code
                                            from dim.dim_vova_languages
                                            group by languages_id, languages_code) b
                                           on a.language = b.languages_code
                                 left join (select country_id, country_code
                                            from dim.dim_vova_region
                                            group by country_id, country_code) c
                                           on a.geo_country = c.country_code
                        WHERE a.pt = '${cur_date}'
                          AND a.event_type = 'goods' and a.platform = 'mob'
                       ) t1
                           left join (select event_fingerprint,
                                             buyer_id,
                                             device_id,
                                             collector_ts                        collector_tstamp,
                                             'search_result'                     page_code,
                                             cast(dvce_created_tstamp as bigint) dvce_created_tstamp,
                                             lower(trim(element_id))             element_id
                                      from dwd.dwd_vova_log_common_click
                                      where pt = '${cur_date}'
                                        and element_name = 'search_confirm'
                                      union all
                                      select event_fingerprint,
                                             buyer_id,
                                             device_id,
                                             collector_ts                        collector_tstamp,
                                             'product_detail'                    page_code,
                                             cast(dvce_created_tstamp as bigint) dvce_created_tstamp,
                                             virtual_goods_id                    element_id
                                      from dwd.dwd_vova_log_goods_click
                                      where pt = '${cur_date}') t2
                                     on t1.buyer_id = t2.buyer_id and t1.page_code = t2.page_code and
                                        t1.device_id = t2.device_id and t1.event_fingerprint != t2.event_fingerprint
                                         and t1.dvce_created_tstamp >= t2.dvce_created_tstamp
              ) t where t.rn = 1


;

insert overwrite table tmp.tmp_vova_result_with_expre_from
select /*+ REPARTITION(200) */* from (
                  select t1.buyer_id,
                         t1.device_id,
                         t1.virtual_goods_id,
                         t1.page_code,
                         t1.list_type,
                         t1.collector_tstamp                                                                                                                            expre_time,
                         t1.collector_tstamp,
                         t1.session_id,
                         t1.gender,
                         t1.language,
                         t1.geo_country,
                         t1.os_type,
                         t1.platform,
                         t1.device_model,
                         t2.enter_ts,
                         t2.leave_ts,
                         t1.element_id,
                         t2.event_fingerprint,
                         t2.collector_tstamp                                                                                                                            clk_time,
                         row_number()
                                 over (partition by t1.buyer_id,t1.device_id,t1.virtual_goods_id,t1.collector_tstamp order by (t1.dvce_created_tstamp - t2.dvce_created_tstamp)) as time_gap_2,
                         t1.geo_city,
                         t1.geo_latitude,
                         t1.geo_longitude,
                         t1.geo_region,
                         t1.absolute_position,
                         t1.imsi,
                         t1.search_words_or_goods_clk_ts
                  from tmp.tmp_vova_expre_with_clk_from t1
                           left join tmp.tmp_vova_product_detail_with_clk_from t2
                                     on t1.buyer_id = t2.buyer_id and t1.page_code = t2.page_code and
                                        t1.device_id = t2.device_id and t1.virtual_goods_id = t2.virtual_goods_id
                                         and t1.dvce_created_tstamp <= t2.dvce_created_tstamp
              ) t where t.time_gap_2 = 1

;



INSERT OVERWRITE TABLE tmp.tmp_vova_user_clk_behave_link_d partition (pt = '${cur_date}')
select /*+ REPARTITION(200) */ tmp.session_id,
                               tmp.buyer_id,
                               tmp.gender,
                               tmp.language                                                      language_id,
                               tmp.geo_country                                                   country_id,
                               tmp.os_type,
                               tmp.device_model,
                               dg.goods_id,
                               dg.first_cat_id,
                               dg.second_cat_id,
                               dg.cat_id,
                               dg.mct_id,
                               dg.brand_id,
                               dg.shop_price,
                               dg.shipping_fee,
                               tmp.clk_time,
                               tmp.page_code,
                               tmp.list_type,
                               if(tmp.page_code != 'product_detail', tmp.element_id, d.goods_id) clk_from,
                               from_unixtime(enter_ts / 1000) as                                 enter_ts,
                               from_unixtime(leave_ts / 1000) as                                 leave_ts,
                               (leave_ts - enter_ts) / 1000   as                                 stay_time,
                               if(cw.element_name = 'pdAddToCartSuccess', 1, 0)                  is_add_cart,
                               if(cw.element_name = 'pdAddToWishlistClick', 1, 0)                is_collect,
                               tmp.device_id,
                               dg.goods_name,
                               tmp.expre_time,
                               if(tmp.event_fingerprint is null, 0, 1)                           is_click,
                               if(e.device_id is null, 0, 1)              is_order,
                               tmp.geo_city,
                               tmp.geo_latitude,
                               tmp.geo_longitude,
                               tmp.geo_region,
                               tmp.absolute_position,
                               tmp.imsi,
                               tmp.search_words_or_goods_clk_ts
from (select *,if(element_id is null ,-rand() * 100000,element_id) as element_id_2 from tmp.tmp_vova_result_with_expre_from) tmp
         left join dim.dim_vova_goods dg
                   on tmp.virtual_goods_id = dg.virtual_goods_id
         left join (select
                           device_id,
                           max(collector_tstamp)         collector_tstamp,
                           element_name,
                           cast(element_id as bigint) as virtual_goods_id
                    from dwd.dwd_vova_log_common_click
                    where pt = '${cur_date}'
                      and device_id is not null
                      and page_code = 'product_detail'
                      and platform = 'mob'
                      and element_name in ('pdAddToCartSuccess', 'pdAddToWishlistClick')
                    group by
                             device_id,
                             element_name,
                             cast(element_id as bigint)
) cw
                   on  tmp.device_id = cw.device_id
                       and tmp.virtual_goods_id = cw.virtual_goods_id and
                      cast((cw.collector_tstamp - 1000 * 60 * 30) / 1000 as bigint) >
                      unix_timestamp(tmp.collector_tstamp)
         left join dim.dim_vova_goods d
                   on element_id_2 = d.virtual_goods_id
         left join (select  a.device_id, b.virtual_goods_id, max(a.pay_time) pay_time
                    from dwd.dwd_vova_fact_pay a
                             left join dim.dim_vova_goods b on a.goods_id = b.goods_id
                    where unix_timestamp(a.pay_time) <= unix_timestamp('$now_date 01', 'yyyy-MM-dd HH')
                      and to_date(a.pay_time) >= '${cur_date}'
                    group by a.device_id, b.virtual_goods_id
) e
                   on tmp.device_id = e.device_id
                       and tmp.virtual_goods_id = e.virtual_goods_id
                       and tmp.expre_time < e.pay_time
"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
