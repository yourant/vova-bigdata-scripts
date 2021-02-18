#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#dependence
#ods_vova_vts.ods_vova_order_info
#ods_vova_vts.ods_vova_region
#dwd_vova_log_page_view
#dim_vova_web_domain_userid
#dim_vova_buyers
#dwd_vova_fact_web_start_up

sql="
INSERT OVERWRITE TABLE dwb.dwb_ac_web_ltv PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
'${cur_date}' AS action_date,
order_final.region_code,
order_final.is_new_user,
order_final.medium,
order_final.source,
order_final.cur_paid_uv,
order_final.cur_bonus,
order_final.cur_gmv,
order_final.cur_order_amount,

order_final.three_paid_uv,
order_final.three_bonus,
order_final.three_gmv,
order_final.three_order_amount,

order_final.seven_paid_uv,
order_final.seven_bonus,
order_final.seven_gmv,
order_final.seven_order_amount,

order_final.thirty_paid_uv,
order_final.thirty_bonus,
order_final.thirty_gmv,
order_final.thirty_order_amount,
order_final.half_paid_uv,
order_final.half_bonus,
order_final.half_gmv,
order_final.half_order_amount,
dau.dau,
order_final.is_new_reg_time,
order_final.is_new_register_success_time

FROM (
    SELECT nvl(region_code, 'all')        AS region_code,
           nvl(is_new_user, 'all')        AS is_new_user,
           nvl(medium, 'all')             AS medium,
           nvl(source, 'all')             AS source,
           nvl(is_new_reg_time, 'all') AS is_new_reg_time,
           nvl(is_new_register_success_time, 'all') AS is_new_register_success_time,
           count(DISTINCT cur_paid_buyer_id)  AS cur_paid_uv,
           sum(cur_bonus)  AS cur_bonus,
           sum(cur_gmv)  AS cur_gmv,
           sum(cur_order_amount)  AS cur_order_amount,
           count(DISTINCT three_paid_buyer_id)  AS three_paid_uv,
           sum(three_bonus)  AS three_bonus,
           sum(three_gmv)  AS three_gmv,
           sum(three_order_amount)  AS three_order_amount,
           count(DISTINCT seven_paid_buyer_id)  AS seven_paid_uv,
           sum(seven_bonus)  AS seven_bonus,
           sum(seven_gmv)  AS seven_gmv,
           sum(seven_order_amount)  AS seven_order_amount,
           count(DISTINCT thirty_paid_buyer_id)  AS thirty_paid_uv,
           sum(thirty_bonus)  AS thirty_bonus,
           sum(thirty_gmv)  AS thirty_gmv,
           sum(thirty_order_amount)  AS thirty_order_amount,
           count(DISTINCT half_paid_buyer_id)  AS half_paid_uv,
           sum(half_bonus)  AS half_bonus,
           sum(half_gmv)  AS half_gmv,
           sum(half_order_amount)  AS half_order_amount

    FROM (
SELECT nvl(r.region_code, 'NALL')                                                                      region_code,
       if(date(page_view_log.activate_time) = '${cur_date}', 'Y', 'N')                                  AS is_activate,
       if(date(page_view_log.is_new_reg_time) = '${cur_date}', 'Y', 'N')                                  AS is_new_reg_time,
       if(date(page_view_log.is_new_register_success_time) = '${cur_date}', 'Y', 'N')                                  AS is_new_register_success_time,
       if(db.first_order_id is null OR to_date(db.first_pay_time) = '${cur_date}' ,'Y','N') AS is_new_user,
       nvl(page_view_log.medium, 'NA')                                                                 AS medium,
       nvl(page_view_log.source, 'NA')                                                                 AS source,
       if(to_date(oi.pay_time) = '${cur_date}', oi.user_id, NULL)                                  AS cur_paid_buyer_id,
       if(to_date(oi.pay_time) = '${cur_date}', oi.bonus, 0)                                   AS cur_bonus,
       if(to_date(oi.pay_time) = '${cur_date}', oi.goods_amount + oi.shipping_fee, 0) AS cur_gmv,
       if(to_date(oi.pay_time) = '${cur_date}', oi.goods_amount + oi.shipping_fee + oi.bonus, 0) AS cur_order_amount,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 3) , oi.user_id, NULL)                                  AS three_paid_buyer_id,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 3), oi.bonus, 0)                                   AS three_bonus,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 3), oi.goods_amount + oi.shipping_fee, 0) AS three_gmv,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 3), oi.goods_amount + oi.shipping_fee + oi.bonus, 0) AS three_order_amount,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 7) , oi.user_id, NULL)                                  AS seven_paid_buyer_id,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 7), oi.bonus, 0)                                   AS seven_bonus,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 7), oi.goods_amount + oi.shipping_fee, 0) AS seven_gmv,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 7), oi.goods_amount + oi.shipping_fee + oi.bonus, 0) AS seven_order_amount,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 30) , oi.user_id, NULL)                                  AS thirty_paid_buyer_id,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 30), oi.bonus, 0)                                   AS thirty_bonus,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 30), oi.goods_amount + oi.shipping_fee, 0) AS thirty_gmv,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 30), oi.goods_amount + oi.shipping_fee + oi.bonus, 0) AS thirty_order_amount,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 180) , oi.user_id, NULL)                                  AS half_paid_buyer_id,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 180), oi.bonus, 0)                                   AS half_bonus,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 180), oi.goods_amount + oi.shipping_fee, 0) AS half_gmv,
       if(to_date(oi.pay_time) >= '${cur_date}' AND to_date(oi.pay_time) < date_add('${cur_date}', 180), oi.goods_amount + oi.shipping_fee + oi.bonus, 0) AS half_order_amount


FROM ods_vova_vts.ods_vova_order_info oi
   inner join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
         LEFT JOIN (
    SELECT log.buyer_id,
           first_value(activate_time) AS activate_time,
           first_value(reg_time) AS is_new_reg_time,
           first_value(register_success_time) AS is_new_register_success_time,
           first_value(medium) AS medium,
           first_value(source) AS source
    FROM dwd.dwd_vova_log_page_view log
             INNER JOIN dim.dim_vova_web_domain_userid dwdu ON log.domain_userid = dwdu.domain_userid
    WHERE log.pt = '${cur_date}'
      AND log.datasource = 'airyclub'
      AND log.platform in ('web','pc')
      AND log.buyer_id > 0
    GROUP BY log.buyer_id
) page_view_log ON page_view_log.buyer_id = oi.user_id
         LEFT JOIN dim.dim_vova_buyers db ON oi.user_id = db.buyer_id
WHERE to_date(oi.pay_time) <= date_add('${cur_date}', 180)
  AND to_date(oi.pay_time) >= '${cur_date}'
  AND oi.from_domain NOT LIKE '%api.airyclub%'
  AND oi.from_domain like '%airyclub%'
  AND oi.pay_status >= 1
  and oi.parent_order_id = 0
         ) order_final
  WHERE order_final.is_activate = 'Y'
    GROUP BY CUBE (order_final.region_code, order_final.is_new_user, order_final.medium,
                   order_final.source, order_final.is_new_reg_time, order_final.is_new_register_success_time)
) order_final
left join
(
SELECT nvl(region_code, 'all') AS region_code,
       nvl(is_new_user, 'all') AS is_new_user,
       nvl(is_new_reg_time, 'all') AS is_new_reg_time,
       nvl(is_new_register_success_time, 'all') AS is_new_register_success_time,
       nvl(medium, 'all') AS medium,
       nvl(source, 'all') AS source,
       count(DISTINCT domain_userid)      AS dau
FROM (
         SELECT region_code,
                is_activate,
                is_new_reg_time,
                is_new_register_success_time,
                is_new_user,
                medium,
                source,
                domain_userid,
                pt
         FROM (
                  SELECT nvl(su.region_code, 'NALL')                               AS region_code,
                         nvl(su.domain_userid, NULL)                               AS domain_userid,
                         if(date(dim_web.activate_time) = su.pt, 'Y', 'N') AS is_activate,
                         if(date(dim_web.reg_time) = '${cur_date}', 'Y', 'N')                                  AS is_new_reg_time,
                         if(date(dim_web.register_success_time) = '${cur_date}', 'Y', 'N')                                  AS is_new_register_success_time,
                         if(dim_web.first_order_id IS NULL OR to_date(dim_web.first_pay_time) = su.pt, 'Y','N')                                                   AS is_new_user,
                         nvl(dim_web.medium, 'NA')                                 AS medium,
                         nvl(dim_web.source, 'NA')                                 AS source,
                         su.pt
                  FROM (
                           SELECT domain_userid,
                                  region_code,
                                  pt
                           FROM dwd.dwd_vova_fact_web_start_up su
                           WHERE su.pt = '${cur_date}' AND su.datasource = 'airyclub'
                           GROUP BY domain_userid, region_code, pt) su
                           LEFT JOIN dim.dim_vova_web_domain_userid dim_web ON su.domain_userid = dim_web.domain_userid
              ) temp
     ) final
     where final.is_activate = 'Y'
      GROUP BY CUBE(final.region_code, final.is_new_user, final.medium, final.source, final.is_new_reg_time, final.is_new_register_success_time)

) dau
 ON order_final.region_code = dau.region_code
 and order_final.is_new_user = dau.is_new_user
 and order_final.medium = dau.medium
 and order_final.source = dau.source
 and order_final.is_new_reg_time = dau.is_new_reg_time
 and order_final.is_new_register_success_time = dau.is_new_register_success_time
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=60" \
--conf "spark.app.name=dwb_ac_web_ltv" \
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


