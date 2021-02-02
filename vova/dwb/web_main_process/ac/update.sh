#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

##dependence
#dwd_vova_log_data
#dwd_vova_log_page_view
#dim_vova_web_domain_userid
#dim_vova_order_goods
#dim_vova_buyers

sql="
INSERT OVERWRITE TABLE dwb.dwb_ac_web_main_process PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
'${cur_date}' AS action_date,
view_data.region_code,
view_data.is_activate,
view_data.is_new_user,
view_data.medium,
view_data.source,
view_data.dau,
view_data.homepage_uv,
view_data.product_detail_uv,
view_data.product_detail_pv,
view_data.add_to_cart_uv,
view_data.add_to_cart_success_uv,
view_data.add_to_cart_success_pv,
view_data.cart_uv,
view_data.checkout_common_click_uv,
view_data.checkout_apply_success_uv,
view_data.checkout_page_view_uv,
view_data.place_order_uv,
view_data.place_order_apply_success_uv,
view_data.wishlist_uv,
view_data.wishlist_goods_impression_uv,
view_data.wishlist_goods_click_uv,
extra_log.bag_uv,
extra_log.bag_viewchart_uv,
extra_log.bag_checkout_uv,
order_final.order_uv,
order_final.pay_uv,
order_final.cur_pay_uv,
order_final.pay_order_cnt,
order_final.sale_goods_cnt,
order_final.gmv,
view_data.is_new_reg_time,
view_data.is_new_register_success_time,
view_data.datasource
FROM (
         SELECT nvl(datasource, 'all')                       AS datasource,
                nvl(region_code, 'all')                      AS region_code,
                nvl(is_activate, 'all')                      AS is_activate,
                nvl(is_new_user, 'all')                      AS is_new_user,
                nvl(is_new_reg_time, 'all')                      AS is_new_reg_time,
                nvl(is_new_register_success_time, 'all')                      AS is_new_register_success_time,
                nvl(medium, 'all')                           AS medium,
                nvl(source, 'all')                           AS source,
                count(DISTINCT dau)                          AS dau,
                count(DISTINCT homepage_uv)                  AS homepage_uv,
                count(DISTINCT product_detail_uv)            AS product_detail_uv,
                count(product_detail_uv)                     AS product_detail_pv,
                count(DISTINCT add_to_cart_uv)               AS add_to_cart_uv,
                count(DISTINCT add_to_cart_success_uv)       AS add_to_cart_success_uv,
                count(add_to_cart_success_uv)                AS add_to_cart_success_pv,
                count(DISTINCT cart_uv)                      AS cart_uv,
                count(DISTINCT checkout_common_click_uv)     AS checkout_common_click_uv,
                count(DISTINCT checkout_apply_success_uv)    AS checkout_apply_success_uv,
                count(DISTINCT checkout_page_view_uv)        AS checkout_page_view_uv,
                count(DISTINCT place_order_uv)               AS place_order_uv,
                count(DISTINCT place_order_apply_success_uv) AS place_order_apply_success_uv,
                count(DISTINCT wishlist_uv)                  AS wishlist_uv,
                count(DISTINCT wishlist_goods_impression_uv) AS wishlist_goods_impression_uv,
                count(DISTINCT wishlist_goods_click_uv)      AS wishlist_goods_click_uv
         FROM (
                select
                nvl(temp.datasource, 'NALL') AS datasource,
                nvl(temp.geo_country, 'NALL') AS region_code,
                if(date(dwdu.activate_time) = '${cur_date}','Y','N') AS is_activate,
                if(dwdu.first_order_id is null OR to_date(dwdu.first_pay_time) = '${cur_date}' ,'Y','N') AS is_new_user,
                if(dwdu.reg_time is not null and to_date(dwdu.reg_time) = '${cur_date}' ,'Y','N') AS is_new_reg_time,
                if(dwdu.register_success_time is not null and to_date(dwdu.register_success_time) = '${cur_date}' ,'Y','N') AS is_new_register_success_time,
                nvl(dwdu.medium,'NA') AS medium,
                nvl(dwdu.source,'NA') AS source,
                if(event_name = 'page_view' and page_code = 'product_detail',temp.domain_userid, null) as product_detail_uv,
                if(event_name = 'page_view' and page_code = 'homepage',temp.domain_userid, null) as homepage_uv,
                if(event_name = 'page_view',temp.domain_userid, null) as dau,
                if(event_name = 'click' and page_code = 'product_detail' and element_name = 'AddToCart' and event_type = 'normal',temp.domain_userid, null) as add_to_cart_uv,
                if(event_name = 'data' and page_code = 'product_detail' and element_name = 'AddToCartSuccess',temp.domain_userid, null) as add_to_cart_success_uv,
                if(event_name = 'page_view' and page_code = 'cart',temp.domain_userid, null) as cart_uv,
                if(event_name = 'click' and page_code = 'cart' and element_name = 'check_out' and event_type = 'normal',temp.domain_userid, null) as checkout_common_click_uv,
                if(event_name = 'data' and page_code = 'cart' and element_name = 'checkout_apply_success',temp.domain_userid, null) as checkout_apply_success_uv,
                if(event_name = 'page_view' and page_code = 'checkout',temp.domain_userid, null) as checkout_page_view_uv,
                if(event_name = 'click' and page_code = 'checkout' and element_name = 'place_order' and event_type = 'normal',temp.domain_userid, null) as place_order_uv,
                if(event_name = 'data' and page_code = 'checkout' and element_name = 'place_order_apply_success',temp.domain_userid, null) as place_order_apply_success_uv,
                if(event_name = 'page_view' and page_code = 'wishlist',temp.domain_userid, null) as wishlist_uv,
                if(event_name = 'impressions' and page_code = 'wishlist' and element_name = 'wishlist' and list_type='/wishlist' and event_type = 'goods',temp.domain_userid, null) as wishlist_goods_impression_uv,
                if(event_name = 'click' and page_code = 'wishlist' and element_name = 'wishlist' and list_type='/wishlist' and event_type = 'goods',temp.domain_userid, null) as wishlist_goods_click_uv
                  FROM (
select pt,datasource,event_name,geo_country,os_type,buyer_id,page_code,device_id,NULL event_type,NULL element_name,domain_userid,app_uri,NULL list_uri,NULL list_type from dwd.dwd_vova_log_page_view_arc where pt='${cur_date}' and datasource in ('airyclub') and platform in ('web','pc')
union
select pt,datasource,event_name,geo_country,os_type,buyer_id,page_code,device_id,NULL event_type,element_name,domain_userid,app_uri,NULL list_uri,NULL list_type from dwd.dwd_vova_log_data where pt='${cur_date}' and datasource in ('airyclub') and platform in ('web','pc')
union
select pt,datasource,event_name,geo_country,os_type,buyer_id,page_code,device_id,event_type,element_name,domain_userid,app_uri,NULL list_uri,list_type from dwd.dwd_vova_log_impressions_arc where pt='${cur_date}' and datasource in ('airyclub') and platform in ('web','pc')
union
select pt,datasource,event_name,geo_country,os_type,buyer_id,page_code,device_id,event_type,element_name,domain_userid,app_uri,NULL list_uri,list_type from dwd.dwd_vova_log_click_arc where pt='${cur_date}' and datasource in ('airyclub') and platform in ('web','pc')
) temp
                           LEFT JOIN dim.dim_vova_web_domain_userid dwdu ON dwdu.domain_userid = temp.domain_userid AND dwdu.datasource = temp.datasource
              ) final
         GROUP BY CUBE (final.datasource, final.region_code, final.is_activate, final.is_new_user, final.medium, final.source, final.is_new_reg_time, final.is_new_register_success_time)
     ) view_data
         LEFT JOIN (
    SELECT nvl(datasource, 'all')         AS datasource,
           nvl(region_code, 'all')        AS region_code,
           nvl(is_activate, 'all')        AS is_activate,
           nvl(is_new_user, 'all')        AS is_new_user,
           nvl(medium, 'all')             AS medium,
           nvl(source, 'all')             AS source,
           nvl(is_new_reg_time, 'all')             AS is_new_reg_time,
           nvl(is_new_register_success_time, 'all')             AS is_new_register_success_time,
           count(DISTINCT bag_uv)           AS bag_uv,
           count(DISTINCT bag_viewchart_uv) AS bag_viewchart_uv,
           count(DISTINCT bag_checkout_uv)     AS bag_checkout_uv
    FROM (
             SELECT nvl(tmp_flow.geo_country, 'NALL')                            AS region_code,
                    nvl(tmp_flow.datasource, 'NALL')                             AS datasource,
                    if(date(dwdu.activate_time) = '${cur_date}', 'Y', 'N')   AS is_activate,
                    if(dwdu.first_order_id is null OR to_date(dwdu.first_pay_time) = '${cur_date}' ,'Y','N') AS is_new_user,
                    if(dwdu.reg_time is not null and to_date(dwdu.reg_time) = '${cur_date}' ,'Y','N') AS is_new_reg_time,
                    if(dwdu.register_success_time is not null and to_date(dwdu.register_success_time) = '${cur_date}' ,'Y','N') AS is_new_register_success_time,
                    nvl(dwdu.medium, 'NA')                                   AS medium,
                    nvl(dwdu.source, 'NA')                                   AS source,
                    if(event_name = 'impressions' AND element_name = 'bag' AND lead_event_name = 'click' AND lead_element_name = 'AddToCart', tmp_flow.domain_userid, NULL) AS bag_uv,
                    if(event_name = 'click' AND element_name = 'bag_viewchart' AND lead_event_name = 'impressions' AND lead_element_name = 'bag' AND lead2_event_name = 'click' AND lead2_element_name = 'AddToCart',tmp_flow.domain_userid, NULL)                                  AS bag_viewchart_uv,
                    if(event_name = 'click' AND element_name = 'bag_checkout' AND lead_event_name = 'impressions' AND lead_element_name = 'bag' AND lead2_event_name = 'click' AND lead2_element_name = 'AddToCart', tmp_flow.domain_userid, NULL)                                  AS bag_checkout_uv
             FROM (
                      SELECT pt,
                             datasource,
                             domain_userid,
                             page_code,
                             element_name,
                             event_name,
                             geo_country,
                             lead(event_name, 1, 'NA')
                                  OVER (PARTITION BY domain_userid,datasource ORDER BY dvce_created_tstamp) lead_event_name,
                             lead(element_name, 1, 'NA')
                                  OVER (PARTITION BY domain_userid,datasource ORDER BY dvce_created_tstamp) lead_element_name,
                             lead(event_name, 2, 'NA')
                                  OVER (PARTITION BY domain_userid,datasource ORDER BY dvce_created_tstamp) lead2_event_name,
                             lead(element_name, 2, 'NA')
                                  OVER (PARTITION BY domain_userid,datasource ORDER BY dvce_created_tstamp) lead2_element_name
                      FROM (
                               SELECT pt,
                                      datasource,
                                      domain_userid,
                                      page_code,
                                      element_name,
                                      dvce_created_tstamp,
                                      event_name,
                                      geo_country
                               FROM dwd.dwd_vova_log_impressions_arc
                               WHERE pt = '${cur_date}'
                                 AND datasource in ('airyclub')
                                 AND platform in ('web','pc')
                                 AND event_type = 'normal'
                               UNION
                               SELECT pt,
                                      datasource,
                                      domain_userid,
                                      page_code,
                                      element_name,
                                      dvce_created_tstamp,
                                      event_name,
                                      geo_country
                               FROM dwd.dwd_vova_log_click_arc
                               WHERE pt = '${cur_date}'
                                 AND datasource in ('airyclub')
                                 AND platform in ('web','pc')
                                 AND event_type = 'normal'
                           ) temp
                  ) tmp_flow
                      LEFT JOIN dim.dim_vova_web_domain_userid dwdu ON dwdu.domain_userid = tmp_flow.domain_userid AND dwdu.datasource = tmp_flow.datasource
         ) extra_log
    GROUP BY CUBE (extra_log.datasource, extra_log.region_code, extra_log.is_activate, extra_log.is_new_user, extra_log.medium,
                   extra_log.source,extra_log.source ,extra_log.is_new_reg_time, extra_log.is_new_register_success_time)
) extra_log
                   ON view_data.region_code = extra_log.region_code
                       AND view_data.is_activate = extra_log.is_activate
                       AND view_data.is_new_user = extra_log.is_new_user
                       AND view_data.medium = extra_log.medium
                       AND view_data.source = extra_log.source
                       AND view_data.is_new_reg_time = extra_log.is_new_reg_time
                       AND view_data.is_new_register_success_time = extra_log.is_new_register_success_time
                       AND view_data.datasource = extra_log.datasource
         LEFT JOIN (
    SELECT nvl(datasource, 'all')         AS datasource,
           nvl(region_code, 'all')        AS region_code,
           nvl(is_activate, 'all')        AS is_activate,
           nvl(is_new_user, 'all')        AS is_new_user,
           nvl(medium, 'all')             AS medium,
           nvl(source, 'all')             AS source,
           nvl(is_new_reg_time, 'all')             AS is_new_reg_time,
           nvl(is_new_register_success_time, 'all')             AS is_new_register_success_time,
           count(DISTINCT order_buyer_id) AS order_uv,
           count(DISTINCT paid_buyer_id)  AS pay_uv,
           count(DISTINCT cur_paid_buyer_id)  AS cur_pay_uv,
           count(DISTINCT paid_order_id)  AS pay_order_cnt,
           sum(sale_goods_cnt)  AS sale_goods_cnt,
           sum(gmv)  AS gmv
    FROM (
SELECT nvl(ddog.region_code, 'NALL')                                                                      region_code,
       nvl(ddog.datasource, 'NALL')                                                                       datasource,
       if(date(page_view_log.activate_time) = '${cur_date}', 'Y', 'N')                                  AS is_activate,
       if(db.first_order_id is null OR to_date(db.first_pay_time) = '${cur_date}' ,'Y','N') AS is_new_user,
       if(page_view_log.reg_time is not null and to_date(page_view_log.reg_time) = '${cur_date}' ,'Y','N') AS is_new_reg_time,
       if(page_view_log.register_success_time is not null and to_date(page_view_log.register_success_time) = '${cur_date}' ,'Y','N') AS is_new_register_success_time,
       nvl(page_view_log.medium, 'NA')                                                                 AS medium,
       nvl(page_view_log.source, 'NA')                                                                 AS source,
       if(to_date(ddog.order_time) = '${cur_date}', ddog.buyer_id, NULL)                                AS order_buyer_id,
       if(to_date(ddog.pay_time) = '${cur_date}', ddog.buyer_id, NULL)                                  AS paid_buyer_id,
       if(to_date(ddog.pay_time) = '${cur_date}' and to_date(ddog.order_time) = '${cur_date}', ddog.buyer_id, NULL) AS cur_paid_buyer_id,
       if(to_date(ddog.pay_time) = '${cur_date}', ddog.order_id, NULL)                                  AS paid_order_id,
       if(to_date(ddog.pay_time) = '${cur_date}', ddog.goods_number, 0)                                   AS sale_goods_cnt,
       if(to_date(ddog.pay_time) = '${cur_date}', ddog.shop_price * ddog.goods_number + ddog.shipping_fee, 0) AS gmv
FROM dim.dim_vova_order_goods ddog
         LEFT JOIN (
    SELECT log.buyer_id,
           log.datasource,
           first_value(activate_time) AS activate_time,
           first_value(reg_time) AS reg_time,
           first_value(register_success_time) AS register_success_time,
           first_value(medium) AS medium,
           first_value(source) AS source
    FROM dwd.dwd_vova_log_page_view log
             INNER JOIN dim.dim_vova_web_domain_userid dwdu ON log.domain_userid = dwdu.domain_userid AND log.domain_userid = dwdu.domain_userid
    WHERE log.pt = '${cur_date}'
      AND log.datasource in ('airyclub')
      AND log.platform in ('web','pc')
      AND log.buyer_id > 0
    GROUP BY log.buyer_id, log.datasource
) page_view_log ON page_view_log.buyer_id = ddog.buyer_id AND page_view_log.datasource = ddog.datasource
         LEFT JOIN dim.dim_vova_buyers db ON ddog.buyer_id = db.buyer_id
WHERE (to_date(ddog.order_time) = '${cur_date}' OR to_date(ddog.pay_time) = '${cur_date}')
  AND ddog.from_domain NOT LIKE '%api%'
  AND ddog.datasource in ('airyclub')
         ) order_final
    GROUP BY CUBE (order_final.datasource, order_final.region_code, order_final.is_activate, order_final.is_new_user, order_final.medium,
                   order_final.source, order_final.is_new_reg_time, order_final.is_new_register_success_time)
) order_final
                   ON view_data.region_code = order_final.region_code
                       AND view_data.is_activate = order_final.is_activate
                       AND view_data.is_new_user = order_final.is_new_user
                       AND view_data.medium = order_final.medium
                       AND view_data.source = order_final.source
                       AND view_data.is_new_reg_time = order_final.is_new_reg_time
                       AND view_data.is_new_register_success_time = order_final.is_new_register_success_time
                       AND view_data.datasource = order_final.datasource
where view_data.datasource = 'airyclub'
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_ac_web_main_process" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

