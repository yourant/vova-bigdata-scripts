#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_board_rank_v2 PARTITION (pt = '${cur_date}')
SELECT '${cur_date}' AS event_date,
       m_dau.platform,
       m_dau.region_code,
       bod_dau,
       mkt_dau,
       mkt_gmv,
       mkt_pay_user,
       mkt_pay_num,
       bod_gmv,
       bod_order_gmv,
       bod_order_user,
       bod_pay_user,
       bod_order_num,
       bod_pay_num,
       bod_order_user_old,
       bod_pay_user_old,
       bod_order_user - bod_order_user_old as bod_order_user_new,
       bod_pay_user - bod_pay_user_old     as bod_pay_user_new,
       bod_pay_again,
       bod_dau_b1,
       bod_dau_cht_b1,
       bod_hmpg_uv,
       bod_hmpg_pv,
       bod_goods_uv,
       bod_goods_pv,
       bod_hmpg_list_1_3_uv,
       bod_hmpg_list_4_9_uv,
       bod_hmpg_list_10_15_uv,
       bod_hmpg_list_16_21_uv,
       bod_hmpg_list_22_27_uv,
       bod_hmpg_list_28_33_uv,
       bod_hmpg_list_34_39_uv,
       bod_hmpg_goods_click_rcmd_uv,
       bod_hmpg_list_1_5_uv,
       bod_hmpg_list_1_5_pv,
       bod_hmpg_goods_rcmd_uv,
       bod_hmpg_goods_rcmd_pv,
       bod_goodspage_list_uv,
       bod_goodspage_list_pv,
       bod_goodspalace_rcmd_uv,
       bod_goodspalace_rcmd_pv,
       bod_goods_rcmd_uv,
       bod_goods_rcmd_pv,
       bod_goods_dtl_uv,
       bod_goods_dtl_pv,
       bod_no_brand_gmv,
       total_goods_num
FROM
    -- 大盘dau
    (
        SELECT nvl(region_code, 'all')   AS region_code,
               nvl(platform, 'all')      AS platform,
               count(DISTINCT device_id) AS mkt_dau
        FROM (
                 SELECT nvl(su.region_code, 'NALL') AS region_code,
                        nvl(su.platform, 'NA')      AS platform,
                        su.device_id
                 FROM dwd.dwd_vova_fact_start_up su
                 WHERE su.pt = '${cur_date}'
             ) tmp
        GROUP BY CUBE (tmp.region_code, tmp.platform)
    ) AS m_dau
        -- 榜单dau,留存
        LEFT JOIN (SELECT nvl(region_code, 'all')          AS region_code,
                          nvl(platform, 'all')             AS platform,
                          count(DISTINCT device_id)        AS bod_dau,
                          count(DISTINCT pgv_b1_device_id) AS bod_dau_cht_b1
                   FROM (SELECT nvl(pgv.geo_country, 'NALL') AS region_code,
                                nvl(pgv.os_type, 'NA')       AS platform,
                                pgv.device_id,
                                pgv_b1.device_id             AS pgv_b1_device_id
                         FROM (
                                  SELECT geo_country, os_type, device_id
                                  FROM dwd.dwd_vova_log_page_view log1
                                  WHERE pt = '${cur_date}'
                                    AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                  GROUP BY geo_country, os_type, device_id
                                  UNION
                                  SELECT geo_country, os_type, device_id
                                  FROM dwd.dwd_vova_log_screen_view log2
                                  WHERE pt = '${cur_date}'
                                    AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                  GROUP BY geo_country, os_type, device_id
                              ) pgv
                                  LEFT JOIN (
                             SELECT device_id
                             FROM (SELECT device_id FROM
                            dwd.dwd_vova_log_page_view log1
                            WHERE pt = date_sub('${cur_date}', 1)
                              AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                            UNION
                            SELECT device_id FROM
                            dwd.dwd_vova_log_screen_view log2
                            WHERE pt = date_sub('${cur_date}', 1)
                              AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                      ) temp
                             GROUP BY temp.device_id
                         ) AS pgv_b1
                                            ON pgv_b1.device_id = pgv.device_id
                        ) tmp
                   GROUP BY CUBE (tmp.region_code, tmp.platform)
    ) AS tmp1 ON m_dau.region_code = tmp1.region_code AND m_dau.platform = tmp1.platform
        -- 1天前dau
        LEFT JOIN (SELECT nvl(region_code, 'all')   AS region_code,
                          nvl(platform, 'all')      AS platform,
                          count(DISTINCT device_id) AS bod_dau_b1
                   FROM (
                            SELECT nvl(pgv.geo_country, 'NALL') AS region_code,
                                   nvl(pgv.os_type, 'NA')       AS platform,
                                   pgv.device_id
                            FROM (
                                     SELECT geo_country, os_type, device_id
                                     FROM dwd.dwd_vova_log_page_view log1
                                     WHERE pt = date_sub('${cur_date}', 1)
                                       AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                     UNION
                                     SELECT geo_country, os_type, device_id
                                     FROM dwd.dwd_vova_log_screen_view log2
                                     WHERE pt = date_sub('${cur_date}', 1)
                                       AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                 ) pgv
                        ) tmp
                   GROUP BY CUBE (tmp.region_code, tmp.platform)
    ) AS tmp2 ON m_dau.region_code = tmp2.region_code AND m_dau.platform = tmp2.platform
        -- 大盘gmv
        left join (select nvl(fp.region_code, 'all')                             as region_code,
                          nvl(fp.platform, 'all')                                as platform,
                          count(distinct fp.buyer_id)                            as mkt_pay_user,
                          count(distinct fp.order_id)                            as mkt_pay_num,
                          sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as mkt_gmv
                   from dwd.dwd_vova_fact_pay fp
                   where date(fp.pay_time) = '${cur_date}'
                     and fp.datasource = 'vova'
                     and fp.from_domain like '%api%'
                   group by cube (fp.region_code, fp.platform)
    ) as tmp4 on m_dau.region_code = tmp4.region_code and m_dau.platform = tmp4.platform
        -- 榜单gmv
        left join (select nvl(fp.region_code, 'all')                             as region_code,
                          nvl(og.platform, 'all')                                as platform,
                          count(distinct fp.buyer_id)                            as bod_pay_user,
                          count(distinct fp.order_id)                            as bod_pay_num,
                          count(distinct pd_agn.buyer_id)                        as bod_pay_again,
                          sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as bod_gmv,
                          sum(if(dg.brand_id <= 0,fp.goods_number * fp.shop_price + fp.shipping_fee,0)) as bod_no_brand_gmv,
                          count(distinct ou.buyer_id)                            as bod_pay_user_old
                    from dwd.dwd_vova_fact_order_cause_v2 og
                            INNER JOIN dwd.dwd_vova_fact_pay fp
                                       on og.order_goods_id = fp.order_goods_id
                            left join (select distinct og.buyer_id
                                       from dwd.dwd_vova_fact_order_cause_v2 og
                                                INNER JOIN dwd.dwd_vova_fact_pay fp
                                                           on og.order_goods_id = fp.order_goods_id
                                       where date(fp.pay_time) < '${cur_date}'
                                         and fp.datasource = 'vova'
                                         and fp.from_domain like '%api%'
                                         and og.pt < '${cur_date}'
                                         and og.datasource = 'vova'
                                        and og.pre_page_code = 'vovalist_goodpage'
                                        and og.pre_list_type in ('/top-rated','/hottest','/best-sellers')
                    ) as pd_agn on pd_agn.buyer_id = og.buyer_id
                            left join (select distinct fp.buyer_id
                                       from dwd.dwd_vova_fact_pay fp
                                       where date(fp.pay_time) < '${cur_date}'
                                         and date(fp.pay_time) > date_sub('${cur_date}', 90)
                                         and fp.datasource = 'vova'
                                         and fp.from_domain like '%api%'
                    ) as ou on ou.buyer_id = og.buyer_id
                    left join dim.dim_vova_goods dg on og.goods_id = dg.goods_id
                    where date(fp.pay_time) = '${cur_date}'
                     and fp.datasource = 'vova'
                     and fp.from_domain like '%api%'
                     and og.pt = '${cur_date}'
                     and og.datasource = 'vova'
                    and og.pre_page_code = 'vovalist_goodpage'
                    and og.pre_list_type in ('/top-rated','/hottest','/best-sellers')
                    group by cube (fp.region_code, og.platform)
    ) as tmp5 on m_dau.region_code = tmp5.region_code and m_dau.platform = tmp5.platform
        -- 榜单订单
        left join (select nvl(fp.region_code, 'all')                             as region_code,
                          nvl(fp.platform, 'all')                                as platform,
                          sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as bod_order_gmv
                   from dwd.dwd_vova_fact_pay fp
                            inner join (select distinct fp.order_id
                                        from dwd.dwd_vova_fact_pay fp
                                                 inner join dim.dim_vova_order_goods og on fp.order_goods_id = og.order_goods_id
                                        where date(fp.pay_time) = '${cur_date}'
                                          and fp.datasource = 'vova'
                                          and fp.from_domain like '%api%'
                                          and order_goods_tag like '%ranking_list_id%'
                   ) as oi on oi.order_id = fp.order_id
                   group by cube (fp.region_code, fp.platform)
    ) as tmp6 on m_dau.region_code = tmp6.region_code and m_dau.platform = tmp6.platform
        -- 榜单下单
        left join (select nvl(og.region_code, 'all')  as region_code,
                          nvl(og.platform, 'all')     as platform,
                          count(distinct og.buyer_id) as bod_order_user,
                          count(distinct og.order_id) as bod_order_num,
                          count(distinct ou.buyer_id) as bod_order_user_old
                   from dim.dim_vova_order_goods og
                            INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
                            left join (select distinct fp.buyer_id
                                       from dwd.dwd_vova_fact_pay fp
                                       where date(fp.pay_time) < '${cur_date}'
                                         and date(fp.pay_time) > date_sub('${cur_date}', 90)
                                         and fp.datasource = 'vova'
                                         and fp.from_domain like '%api%'
                   ) as ou on ou.buyer_id = og.buyer_id
                   where date(oi.order_time) = '${cur_date}'
                     and og.datasource = 'vova'
                     and og.from_domain like '%api%'
                     and og.order_goods_tag like '%ranking_list_id%'
                   group by cube (og.region_code, og.platform)
    ) as tmp7 on m_dau.region_code = tmp7.region_code and m_dau.platform = tmp7.platform
        -- page_view 事件
        LEFT JOIN (SELECT nvl(pgv.region_code, 'all')   AS region_code,
                          nvl(pgv.platform, 'all')      AS platform,
                          count(DISTINCT pgv.bod_hmpg)  AS bod_hmpg_uv,
                          count(bod_hmpg)               AS bod_hmpg_pv,
                          count(DISTINCT pgv.bod_goods) AS bod_goods_uv,
                          count(bod_goods)              AS bod_goods_pv
                   FROM (SELECT if(pgv.page_code = 'vovalist_homepage', pgv.device_id, NULL) AS bod_hmpg,
                                if(pgv.page_code = 'vovalist_goodpage', pgv.device_id, NULL) AS bod_goods,
                                nvl(pgv.geo_country, 'NALL')                                 AS region_code,
                                nvl(pgv.os_type, 'NA')                                       AS platform
                         FROM (
                                  SELECT geo_country, os_type, device_id, page_code
                                  FROM dwd.dwd_vova_log_page_view log1
                                  WHERE pt = '${cur_date}'
                                    AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                                  UNION
                                  SELECT geo_country, os_type, device_id, page_code
                                  FROM dwd.dwd_vova_log_screen_view log2
                                  WHERE pt = '${cur_date}'
                                    AND page_code IN ('vovalist_homepage', 'vovalist_goodpage')
                              ) pgv
                        ) AS pgv
                   group by cube (pgv.region_code, pgv.platform)
    ) as tmp8 on m_dau.region_code = tmp8.region_code and m_dau.platform = tmp8.platform
        -- common_click 事件
        left join (select nvl(cc.region_code, 'all')                 as region_code,
                          nvl(cc.platform, 'all')                 as platform,
                          count(distinct cc.bod_hmpg_list_1_3)       as bod_hmpg_list_1_3_uv,
                          count(distinct cc.bod_hmpg_list_4_9)       as bod_hmpg_list_4_9_uv,
                          count(distinct cc.bod_hmpg_list_10_15)       as bod_hmpg_list_10_15_uv,
                          count(distinct cc.bod_hmpg_list_16_21)       as bod_hmpg_list_16_21_uv,
                          count(distinct cc.bod_hmpg_list_22_27)       as bod_hmpg_list_22_27_uv,
                          count(distinct cc.bod_hmpg_list_28_33)       as bod_hmpg_list_28_33_uv,
                          count(distinct cc.bod_hmpg_list_34_39)       as bod_hmpg_list_34_39_uv,
                          count(distinct cc.bod_hmpg_goods_click_rcmd)       as bod_hmpg_goods_click_rcmd_uv,
                          count(distinct cc.bod_hmpg_list_1_5)       as bod_hmpg_list_1_5_uv,
                          count(cc.bod_hmpg_list_1_5)       as bod_hmpg_list_1_5_pv,
                          count(distinct cc.bod_hmpg_goods_rcmd)       as bod_hmpg_goods_rcmd_uv,
                          count(cc.bod_hmpg_goods_rcmd)       as bod_hmpg_goods_rcmd_pv,
                          count(distinct cc.bod_goodspage_list)       as bod_goodspage_list_uv,
                          count(cc.bod_goodspage_list)       as bod_goodspage_list_pv,
                          count(distinct cc.bod_goodspalace_rcmd)       as bod_goodspalace_rcmd_uv,
                          count(cc.bod_goodspalace_rcmd)       as bod_goodspalace_rcmd_pv,
                          count(distinct cc.bod_goods_rcmd)       as bod_goods_rcmd_uv,
                          count(cc.bod_goods_rcmd)       as bod_goods_rcmd_pv,
                          count(distinct cc.bod_goods_dtl)       as bod_goods_dtl_uv,
                          count(cc.bod_goods_dtl)       as bod_goods_dtl_pv
                   from (select if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist1'), cc.device_id, null) as bod_hmpg_list_1_3,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist2'), cc.device_id, null) as bod_hmpg_list_4_9,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist3'), cc.device_id, null) as bod_hmpg_list_10_15,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist4'), cc.device_id, null) as bod_hmpg_list_16_21,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist5'), cc.device_id, null) as bod_hmpg_list_22_27,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist6'), cc.device_id, null) as bod_hmpg_list_28_33,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name IN ('homepagelist7'), cc.device_id, null) as bod_hmpg_list_34_39,
                                if(cc.page_code = 'vovalist_homepage' AND cc.list_uri = 'vovalist_click' AND cc.element_name = 'recommendgood', cc.device_id, null) as bod_hmpg_goods_click_rcmd,
                                if(cc.page_code = 'vovalist_homepage' AND cc.element_name IN ('homepagelist1', 'homepagelist2', 'homepagelist3', 'homepagelist3', 'homepagelist3','homepagelist4', 'homepagelist5', 'homepagelist6', 'homepagelist7'), cc.device_id, null) as bod_hmpg_list_1_5,
                                if(cc.page_code = 'vovalist_homepage' AND cc.element_name = 'recommendgood', cc.device_id, null) as bod_hmpg_goods_rcmd,
                                if(cc.page_code = 'vovalist_goodpage' AND cc.element_name like 'goodpagegood%', cc.device_id, null)     as bod_goodspage_list,
                                if(cc.page_code = 'vovalist_goodpage' AND cc.element_name like 'goodpagepalace%', cc.device_id, null)   as bod_goodspalace_rcmd,
                                if(cc.page_code = 'vovalist_goodpage' AND cc.element_name = 'recommendgood', cc.device_id, null) as bod_goods_rcmd,
                                if(cc.page_code = 'product_detail' and cc.referrer rlike 'vovalist_homepage|vovalist_goodpage', cc.device_id, null)   as bod_goods_dtl,
                                nvl(cc.geo_country, 'NALL')       as region_code,
                                nvl(cc.os_type, 'NA')       as platform
                         from dwd.dwd_vova_log_common_click cc
                         where cc.pt = '${cur_date}'
                           and cc.page_code IN ('vovalist_homepage', 'vovalist_goodpage', 'product_detail')
                        ) as cc
                   group by cube (cc.region_code, cc.platform)
) as tmp9 on m_dau.region_code = tmp9.region_code and m_dau.platform = tmp9.platform
left join (
    select
        nvl(nvl(geo_country,'NA'),'all') region_code,
        nvl(nvl(os_type,'NA'),'all') platform,
        count(distinct virtual_goods_id,buyer_id) total_goods_num
    from dwd.dwd_vova_log_screen_view a
    where a.pt = '${cur_date}' and a.page_code in ('product_detail') and a.referrer like '%vovalist_goodpage%'
    group by cube (nvl(geo_country,'NA'), nvl(os_type,'NA'))
) as tmp10 on m_dau.region_code = tmp10.region_code and m_dau.platform = tmp10.platform
;
"


spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_board_rank_v2" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi