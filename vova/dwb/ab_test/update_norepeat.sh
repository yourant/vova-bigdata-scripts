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
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_ab_test_norepeat" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.autoBroadcastJoinThreshold=31457280" \
-e "

INSERT OVERWRITE TABLE tmp.tmp_ab_expre_not_repeat
select /*+ REPARTITION(100) */ a.datasource,
       a.platform,
       a.os,
       a.rec_page_code,
       a.rec_code,
       a.rec_version,
       a.device_id,
       a.buyer_id
from dwd.dwd_vova_ab_test_expre a
         left join
     ( select device_id,buyer_id
     from (
              select device_id,
                     buyer_id,
                     count(distinct rec_version) cnt
              from dwd.dwd_vova_ab_test_expre
              where pt = '${cur_date}'
              group by device_id, buyer_id, rec_code
              having cnt != 1
          ) tmp group by device_id,buyer_id
     ) b
     on a.device_id = b.device_id
         and a.buyer_id = b.buyer_id
where a.pt = '${cur_date}'
  and b.device_id is null
;

INSERT OVERWRITE TABLE  tmp.tmp_ab_clk_not_repeat
select /*+ REPARTITION(50) */ a.datasource,
       a.platform,
       a.os,
       a.rec_page_code,
       a.rec_code,
       a.rec_version,
       a.device_id,
       a.buyer_id
from dwd.dwd_vova_ab_test_clk a
         left join
     ( select device_id,buyer_id
     from (
              select device_id,
                     buyer_id,
                     count(distinct rec_version) cnt
              from dwd.dwd_vova_ab_test_clk
              where pt = '${cur_date}'
              group by device_id, buyer_id, rec_code
              having cnt != 1
          ) tmp group by device_id,buyer_id
     ) b
     on a.device_id = b.device_id
         and a.buyer_id = b.buyer_id
where a.pt = '${cur_date}'
  and b.device_id is null
;

INSERT OVERWRITE TABLE  tmp.tmp_ab_cart_not_repeat
select /*+ REPARTITION(10) */ a.datasource,
       a.platform,
       a.os,
       a.rec_page_code,
       a.rec_code,
       a.rec_version,
       a.device_id,
       a.buyer_id
from dwd.dwd_vova_ab_test_cart a
         left join
     ( select device_id,buyer_id
     from (
         select device_id,
                buyer_id,
                count(distinct rec_version) cnt
         from dwd.dwd_vova_ab_test_cart
         where pt = '${cur_date}'
         group by device_id, buyer_id, rec_code
         having cnt != 1
         ) tmp group by device_id,buyer_id
     ) b
     on a.device_id = b.device_id
         and a.buyer_id = b.buyer_id
where a.pt = '${cur_date}'
  and b.device_id is null
;
DROP TABLE IF EXISTS tmp.tmp_ab_pay_not_repeat;
CREATE TABLE IF NOT EXISTS tmp.tmp_ab_pay_not_repeat as
select  /*+ REPARTITION(5) */ a.datasource,
       a.platform,
       a.os,
       a.rec_page_code,
       a.rec_code,
       a.rec_version,
       a.price,
       a.device_id,
       a.buyer_id,
       a.order_goods_id
from dwd.dwd_vova_ab_test_pay a
         left join
     (select device_id,buyer_id
     from (
         select device_id,
                buyer_id,
                count(distinct rec_version) cnt
         from dwd.dwd_vova_ab_test_pay
         where pt = '${cur_date}'
         group by device_id, buyer_id, rec_code
         having cnt != 1
         ) tmp group by device_id,buyer_id
     ) b
     on a.device_id = b.device_id
         and a.buyer_id = b.buyer_id
where a.pt = '${cur_date}'
  and b.device_id is null
;

insert overwrite table dwb.dwb_vova_ab_test_norepeat PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
    '${cur_date}'                                               cur_date,
    coalesce(a.datasource,c.datasource) datasource,
    coalesce(a.platform,c.platform) platform,
    coalesce(a.os,c.os) os,
    coalesce(a.rec_page_code,c.rec_page_code) rec_page_code,
    coalesce(a.rec_code,c.rec_code) rec_code,
    coalesce(a.rec_version,c.rec_version) rec_version,
    a.expre_pv,
    e.clk_pv,
    concat(round(e.clk_pv * 100 / a.expre_pv, 2), '%')          ctr,
    d.expre_uv,
    e.clk_uv,
    nvl(b.cart_uv, 0)                                           cart_uv,
    concat(nvl(round(b.cart_uv * 100 / d.expre_uv, 2), 0), '%') cart_rate,
    nvl(round(c.gmv,2), 0),
    0,
    nvl(c.pay_uv, 0),
    concat(nvl(round(c.pay_uv * 100 / d.expre_uv, 3), 0), '%')  cr,
    nvl(round(c.pay_uv / d.expre_uv, 6), 0)                     impressions_cr,
    nvl(round(c.gmv / d.expre_uv, 6), 0)                        gmv_cr,
    nvl(c.order_cnt, 0)
from (
         select nvl(datasource, 'all')    datasource,
                nvl(platform, 'all')      platform,
                nvl(os, 'all')            os,
                nvl(rec_page_code, 'all') rec_page_code,
                nvl(rec_code, 'all')      rec_code,
                nvl(rec_version, 'all')   rec_version,
                count(1)                  expre_pv
         from tmp.tmp_ab_expre_not_repeat
         group by cube (datasource, platform, os, rec_page_code, rec_code, rec_version)
     ) a
         left join (
    select nvl(datasource, 'all')              datasource,
           nvl(platform, 'all')                platform,
           nvl(os, 'all')                      os,
           nvl(rec_page_code, 'all')           rec_page_code,
           nvl(rec_code, 'all')                rec_code,
           nvl(rec_version, 'all')             rec_version,
           count(distinct device_id, buyer_id) expre_uv
    from (
             select datasource,
                    platform,
                    os,
                    rec_page_code,
                    rec_code,
                    rec_version,
                    device_id,
                    buyer_id
             from tmp.tmp_ab_expre_not_repeat
             group by datasource,
                      platform,
                      os,
                      rec_page_code,
                      rec_code,
                      rec_version,
                      device_id, buyer_id
         )
    group by cube (datasource, platform, os, rec_page_code, rec_code, rec_version)
) d
                   on a.datasource = d.datasource
                       and a.platform = d.platform
                       and a.os = d.os
                       and a.rec_page_code = d.rec_page_code
                       and a.rec_code = d.rec_code
                       and a.rec_version = d.rec_version
         left join (
    select nvl(datasource, 'all')              datasource,
           nvl(platform, 'all')                platform,
           nvl(os, 'all')                      os,
           nvl(rec_page_code, 'all')           rec_page_code,
           nvl(rec_code, 'all')                rec_code,
           nvl(rec_version, 'all')             rec_version,
           count(1)                            clk_pv,
           count(distinct device_id, buyer_id) clk_uv
    from tmp.tmp_ab_clk_not_repeat
    group by cube (datasource, platform, os, rec_page_code, rec_code, rec_version)
) e
                   on a.datasource = e.datasource
                       and a.platform = e.platform
                       and a.os = e.os
                       and a.rec_page_code = e.rec_page_code
                       and a.rec_code = e.rec_code
                       and a.rec_version = e.rec_version
         left join (
    select nvl(datasource, 'all')              datasource,
           nvl(platform, 'all')                platform,
           nvl(os, 'all')                      os,
           nvl(rec_page_code, 'all')           rec_page_code,
           nvl(rec_code, 'all')                rec_code,
           nvl(rec_version, 'all')             rec_version,
           count(distinct device_id, buyer_id) cart_uv
    from tmp.tmp_ab_cart_not_repeat
    group by cube (datasource, platform, os, rec_page_code, rec_code, rec_version)
) b
                   on a.datasource = b.datasource
                       and a.platform = b.platform
                       and a.os = b.os
                       and a.rec_page_code = b.rec_page_code
                       and a.rec_code = b.rec_code
                       and a.rec_version = b.rec_version
         full join (
    select nvl(datasource, 'all')              datasource,
           nvl(platform, 'all')                platform,
           nvl(os, 'all')                      os,
           nvl(rec_page_code, 'all')           rec_page_code,
           nvl(rec_code, 'all')                rec_code,
           nvl(rec_version, 'all')             rec_version,
           count(distinct device_id, buyer_id) pay_uv,
           sum(price)                          gmv,
           count(distinct order_goods_id) order_cnt
    from tmp.tmp_ab_pay_not_repeat
    group by cube (datasource, platform, os, rec_page_code, rec_code, rec_version)
) c
                   on a.datasource = c.datasource
                       and a.platform = c.platform
                       and a.os = c.os
                       and a.rec_page_code = c.rec_page_code
                       and a.rec_code = c.rec_code
                       and a.rec_version = c.rec_version
;

"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
