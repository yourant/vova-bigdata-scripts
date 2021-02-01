#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-01`
fi
###逻辑sql
##dependence
#dwd.dwd_vova_fact_pay
#dim.dim_vova_merchant
#ods_vova_vts.ods_vova_sponsor

sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_merchant_kpi PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       nvl(action_month, 'all') action_month,
       nvl(spsor, 'all')        spsor,
       sum(reg_cnt)             reg_cnt,
       sum(activate_cnt)        activate_cnt,
       sum(publish_cnt)         publish_cnt,
       sum(paid_order_cnt)      paid_order_cnt,
       sum(paid_gmv)            paid_gmv,
       sum(paid_order_cnt_30)   paid_order_cnt_30,
       sum(paid_order_cnt_1)    paid_order_cnt_1,
       sum(paid_order_cnt_0)   paid_order_cnt_0
FROM (SELECT '${cur_date}' AS action_month,
             vs.spsor AS spsor,
             nvl(reg_data.reg_cnt, 0)              reg_cnt,
             nvl(activate_data.activate_cnt, 0)    activate_cnt,
             nvl(publish_data.publish_cnt, 0)      publish_cnt,
             nvl(order_data.paid_order_cnt, 0)     paid_order_cnt,
             nvl(order_data.paid_gmv, 0)           paid_gmv,
             nvl(order_data2.paid_order_cnt_30, 0) paid_order_cnt_30,
             nvl(order_data2.paid_order_cnt_1, 0)  paid_order_cnt_1,
             nvl(order_data2.paid_order_cnt_0, 0)  paid_order_cnt_0
      FROM
      (select distinct nick as spsor from ods_vova_vts.ods_vova_sponsor) vs
      LEFT JOIN
           (
               SELECT temp_data.spsor,
                      count(temp_data.mct_id)                   AS publish_cnt,
                      trunc(temp_data.first_publish_time, 'MM') AS action_month
               FROM (SELECT dm.spsor_name AS spsor,
                            dm.mct_id,
                            dm.reg_time,
                            dm.pay_or_verify_time,
                            dm.first_publish_time
                     FROM dim.dim_vova_merchant dm
                     WHERE trunc(dm.first_publish_time, 'MM') = '${cur_date}'
                    ) temp_data
               GROUP BY trunc(temp_data.first_publish_time, 'MM'), temp_data.spsor
           ) publish_data ON publish_data.spsor = vs.spsor
      LEFT JOIN
      (
               SELECT temp_data.spsor,
                      count(temp_data.mct_id)         AS reg_cnt,
                      trunc(temp_data.reg_time, 'MM') AS action_month
               FROM (SELECT dm.spsor_name AS spsor,
                            dm.mct_id,
                            dm.reg_time,
                            dm.pay_or_verify_time
                     FROM dim.dim_vova_merchant dm
                     WHERE trunc(dm.reg_time, 'MM') = '${cur_date}'
                    ) temp_data
               GROUP BY trunc(temp_data.reg_time, 'MM'), temp_data.spsor
           ) reg_data ON reg_data.spsor = vs.spsor
               LEFT JOIN
           (
               SELECT temp_data.spsor,
                      count(temp_data.mct_id)                   AS activate_cnt,
                      trunc(temp_data.pay_or_verify_time, 'MM') AS action_month
               FROM (SELECT dm.spsor_name AS spsor,
                            dm.mct_id,
                            dm.reg_time,
                            dm.pay_or_verify_time
                     FROM dim.dim_vova_merchant dm
                     WHERE trunc(dm.pay_or_verify_time, 'MM') = '${cur_date}'
                    ) temp_data
               GROUP BY trunc(temp_data.pay_or_verify_time, 'MM'), temp_data.spsor
           ) activate_data ON activate_data.spsor = vs.spsor
               LEFT JOIN
           (
               SELECT temp_data.spsor,
                      trunc(temp_data.first_publish_time, 'MM') AS action_month,
                      count(order_goods_id)                     AS paid_order_cnt,
                      sum(gmv)                                  AS paid_gmv
               FROM (SELECT dm.spsor_name AS spsor,
                            dm.mct_id,
                            dm.reg_time,
                            dm.pay_or_verify_time,
                            dm.first_publish_time,
                            fp.order_goods_id,
                            fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv
                     FROM dim.dim_vova_merchant dm
                              INNER JOIN dwd.dwd_vova_fact_pay fp ON fp.mct_id = dm.mct_id
                     WHERE trunc(dm.first_publish_time, 'MM') = '${cur_date}'
                       AND trunc(fp.pay_time, 'MM') = '${cur_date}'
                    ) temp_data
               GROUP BY trunc(temp_data.first_publish_time, 'MM'), temp_data.spsor
           ) order_data ON order_data.spsor = vs.spsor
               LEFT JOIN
           (
               SELECT spsor,
                      action_month,
                      sum(IF(paid_order_cnt >= 30, 1, 0))                         AS paid_order_cnt_30,
                      sum(IF(paid_order_cnt >= 1 AND paid_order_cnt < 30, 1, 0))  AS paid_order_cnt_1,
                      sum(IF(paid_order_cnt >= 1, 1, 0)) AS paid_order_cnt_0
               FROM (SELECT FIRST_VALUE(temp_data.spsor)              AS spsor,
                            temp_data.mct_id,
                            trunc(temp_data.first_publish_time, 'MM') AS action_month,
                            COUNT(order_goods_id)                     AS paid_order_cnt
                     FROM (SELECT dm.spsor_name AS spsor,
                                  dm.mct_id,
                                  dm.first_publish_time,
                                  fp.order_goods_id
                           FROM dim.dim_vova_merchant dm
                                    LEFT JOIN dwd.dwd_vova_fact_pay fp
                                              ON fp.mct_id = dm.mct_id AND trunc(fp.pay_time, 'MM') = '${cur_date}'
                           WHERE trunc(dm.first_publish_time, 'MM') = '${cur_date}'
                          ) temp_data
                     GROUP BY trunc(temp_data.first_publish_time, 'MM'), temp_data.mct_id) temp_data2
               GROUP BY temp_data2.spsor, temp_data2.action_month
           ) order_data2 ON order_data2.spsor = vs.spsor) final
GROUP BY CUBE(final.spsor, final.action_month)
HAVING action_month != 'all'
;

INSERT OVERWRITE TABLE dwb.dwb_vova_merchant_gmv PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       temp_data.spsor,
       first_publish_month,
       pay_month,
       sum(gmv) AS paid_gmv
FROM (SELECT dm.spsor_name                                         AS spsor,
             dm.mct_id,
             fp.order_goods_id,
             fp.goods_number * fp.shop_price + fp.shipping_fee     AS gmv,
             nvl(trunc(dm.first_publish_time, 'MM'), '2019-01-01') AS first_publish_month,
             trunc(fp.pay_time, 'MM')                              AS pay_month
      FROM dim.dim_vova_merchant dm
               INNER JOIN dwd.dwd_vova_fact_pay fp ON fp.mct_id = dm.mct_id
      WHERE trunc(fp.pay_time, 'MM') = '${cur_date}'
     ) temp_data
GROUP BY first_publish_month, temp_data.spsor, pay_month
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_vova_merchant_kpi" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
