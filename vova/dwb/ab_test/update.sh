#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi


spark-sql   --conf "spark.sql.autoBroadcastJoinThreshold=31457280"  \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.app.name=dwb_vova_ab_test" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "

INSERT OVERWRITE TABLE  tmp.tmp_ab_expre_pv
select /*+ REPARTITION(10) */ nvl(a.datasource, 'all')    datasource,
    nvl(a.platform, 'all')      platform,
    nvl(a.is_brand, 'all')            is_brand,
    nvl(a.rec_page_code, 'all') rec_page_code,
    nvl(a.rec_code, 'all')      rec_code,
    nvl(a.rec_version, 'all')   rec_version,
    nvl(if(a.rp_name like '%47%','Y','N'), 'all')   brand_status,
    count(1)                  expre_pv
from dwd.dwd_vova_ab_test_expre a
where pt = '${cur_date}'
group by cube (a.datasource, a.platform, a.is_brand, a.rec_page_code, a.rec_code, a.rec_version,if(a.rp_name like '%47%','Y','N'))
;

INSERT OVERWRITE TABLE  tmp.tmp_ab_expre_uv_pre
select /*+ REPARTITION(60) */ a.datasource,
    a.platform,
    a.is_brand,
    a.rec_page_code,
    a.rec_code,
    a.rec_version,if(a.rp_name like '%47%','Y','N') brand_status,
    a.device_id,
    a.buyer_id
from dwd.dwd_vova_ab_test_expre a
where pt = '${cur_date}'
group by a.datasource,
      a.platform,
      a.is_brand,
      a.rec_page_code,
      a.rec_code,
      a.rec_version,if(a.rp_name like '%47%','Y','N'),
      a.device_id, a.buyer_id
;

INSERT OVERWRITE TABLE  tmp.ab_expre_uv
select /*+ REPARTITION(10) */ nvl(datasource, 'all')              datasource,
       nvl(platform, 'all')                platform,
       nvl(is_brand, 'all')                      is_brand,
       nvl(rec_page_code, 'all')           rec_page_code,
       nvl(rec_code, 'all')                rec_code,
       nvl(rec_version, 'all')             rec_version,
       nvl(brand_status, 'all')   brand_status,
       count(distinct device_id, buyer_id) expre_uv
from tmp.tmp_ab_expre_uv_pre
group by cube (datasource, platform, is_brand, rec_page_code, rec_code, rec_version,brand_status)
;

INSERT OVERWRITE TABLE  tmp.ab_clk_uv
 select /*+ REPARTITION(10) */ nvl(datasource, 'all')              datasource,
       nvl(platform, 'all')                platform,
       nvl(is_brand, 'all')                      is_brand,
       nvl(rec_page_code, 'all')           rec_page_code,
       nvl(rec_code, 'all')                rec_code,
       nvl(rec_version, 'all')             rec_version,
       nvl(brand_status, 'all')             brand_status,
       count(distinct device_id, buyer_id) clk_uv
from (
     select a.datasource,
        a.platform,
        a.is_brand,
        a.rec_page_code,
        a.rec_code,
        a.rec_version,if(a.rp_name like '%47%','Y','N') brand_status,
        a.device_id,
        a.buyer_id
    from dwd.dwd_vova_ab_test_clk a
    where pt = '${cur_date}'
    group by a.datasource,
          a.platform,
          a.is_brand,
          a.rec_page_code,
          a.rec_code,
          a.rec_version,if(a.rp_name like '%47%','Y','N'),
          a.device_id, a.buyer_id
         )
 tmp
group by cube (datasource, platform, is_brand, rec_page_code, rec_code, rec_version,brand_status)
;

insert overwrite table dwb.dwb_vova_ab_test PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(4) */
    '${cur_date}'                                               cur_date,
    coalesce(a.datasource,c.datasource) datasource,
    coalesce(a.platform,c.platform) platform,
    coalesce(a.is_brand,c.is_brand) is_brand,
    coalesce(a.rec_page_code,c.rec_page_code) rec_page_code,
    coalesce(a.rec_code,c.rec_code) rec_code,
    coalesce(a.rec_version,c.rec_version) rec_version,
    a.expre_pv,
    e.clk_pv,
    concat(round(e.clk_pv * 100 / a.expre_pv, 2), '%')          ctr,
    d.expre_uv,
    f.clk_uv,
    nvl(b.cart_uv, 0)                                           cart_uv,
    concat(nvl(round(b.cart_uv * 100 / d.expre_uv, 2), 0), '%') cart_rate,
    nvl(round(c.gmv,2), 0),
    0,
    nvl(c.pay_uv, 0),
    concat(nvl(round(c.pay_uv * 100 / d.expre_uv, 3), 0), '%')  cr,
    nvl(round(c.pay_uv / d.expre_uv, 6), 0)                     impressions_cr,
    nvl(round(c.gmv / d.expre_uv, 6), 0)                        gmv_cr,
    coalesce(a.brand_status,c.brand_status) brand_status
from tmp.tmp_ab_expre_pv a
         left join tmp.ab_expre_uv d
                   on a.datasource = d.datasource
                       and a.platform = d.platform
                       and a.is_brand = d.is_brand
                       and a.rec_page_code = d.rec_page_code
                       and a.rec_code = d.rec_code
                       and a.rec_version = d.rec_version
                       and a.brand_status = d.brand_status
         left join (
             select nvl(e.datasource, 'all')              datasource,
                   nvl(e.platform, 'all')                platform,
                   nvl(e.is_brand, 'all')                      is_brand,
                   nvl(e.rec_page_code, 'all')           rec_page_code,
                   nvl(e.rec_code, 'all')                rec_code,
                   nvl(e.rec_version, 'all')             rec_version,
                   nvl(if(e.rp_name like '%47%','Y','N'), 'all')             brand_status,
                   count(1)                            clk_pv
            from dwd.dwd_vova_ab_test_clk e
            where pt = '${cur_date}'
            group by cube (e.datasource, e.platform, e.is_brand, e.rec_page_code, e.rec_code, e.rec_version,if(e.rp_name like '%47%','Y','N'))
             ) e
                   on a.datasource = e.datasource
                       and a.platform = e.platform
                       and a.is_brand = e.is_brand
                       and a.rec_page_code = e.rec_page_code
                       and a.rec_code = e.rec_code
                       and a.rec_version = e.rec_version
                       and a.brand_status = e.brand_status
         left join tmp.ab_clk_uv f
                   on a.datasource = f.datasource
                       and a.platform = f.platform
                       and a.is_brand = f.is_brand
                       and a.rec_page_code = f.rec_page_code
                       and a.rec_code = f.rec_code
                       and a.rec_version = f.rec_version
                       and a.brand_status = f.brand_status
         left join (
    select nvl(c.datasource, 'all')              datasource,
           nvl(c.platform, 'all')                platform,
           nvl(c.is_brand, 'all')                      is_brand,
           nvl(c.rec_page_code, 'all')           rec_page_code,
           nvl(c.rec_code, 'all')                rec_code,
           nvl(c.rec_version, 'all')             rec_version,
           nvl(if(c.rp_name like '%47%','Y','N'), 'all')             brand_status,
           count(distinct c.device_id, c.buyer_id) cart_uv
    from dwd.dwd_vova_ab_test_cart c
    where pt = '${cur_date}'
    group by cube (c.datasource, c.platform, c.is_brand, c.rec_page_code, c.rec_code, c.rec_version,if(c.rp_name like '%47%','Y','N'))
) b
                   on a.datasource = b.datasource
                       and a.platform = b.platform
                       and a.is_brand = b.is_brand
                       and a.rec_page_code = b.rec_page_code
                       and a.rec_code = b.rec_code
                       and a.rec_version = b.rec_version
                       and a.brand_status = b.brand_status
         full join (
    select nvl(c.datasource, 'all')              datasource,
           nvl(c.platform, 'all')                platform,
           nvl(c.is_brand, 'all')                      is_brand,
           nvl(c.rec_page_code, 'all')           rec_page_code,
           nvl(c.rec_code, 'all')                rec_code,
           nvl(c.rec_version, 'all')             rec_version,
           nvl(if(c.rp_name like '%47%','Y','N'), 'all')             brand_status,
           count(distinct c.device_id, c.buyer_id) pay_uv,
           sum(price)                          gmv
    from dwd.dwd_vova_ab_test_pay c
    where pt = '${cur_date}'
    group by cube (c.datasource, c.platform, c.is_brand, c.rec_page_code, c.rec_code, c.rec_version,if(c.rp_name like '%47%','Y','N'))
) c
                   on a.datasource = c.datasource
                       and a.platform = c.platform
                       and a.is_brand = c.is_brand
                       and a.rec_page_code = c.rec_page_code
                       and a.rec_code = c.rec_code
                       and a.rec_version = c.rec_version
                       and a.brand_status = c.brand_status
;
insert overwrite table dwb.dwb_vova_gcr_ab PARTITION (pt = '${cur_date}')
select '${cur_date}' cur_date,
       platform,
       rec_page_code,
       cast(sum(expre_pv_a) as int),
       cast(sum(expre_pv_b) as int),
       round(sum(expre_incom_a),6),
       round(sum(expre_incom_b),6),
       max(change_rate_a),
       max(change_rate_b)
from (select platform,
             rec_page_code,
             if(rec_version = 'a', expre_pv, 0)       expre_pv_a,
             if(rec_version = 'b', expre_pv, 0)       expre_pv_b,
             if(rec_version = 'a', gmv / expre_pv, 0) expre_incom_a,
             if(rec_version = 'b', gmv / expre_pv, 0) expre_incom_b,
             if(rec_version = 'a', cr, 0)             change_rate_a,
             if(rec_version = 'b', cr, 0)             change_rate_b
      from dwb.dwb_vova_ab_test
      where pt = '${cur_date}'
        and rec_page_code in ('rec_best_selling', 'rec_most_popular')
        and rec_code in ('rec_gcr') --test
        and datasource = 'all'
        and os = 'all' and brand_status = 'all'
        and rec_version in ('a', 'b'))
group by platform, rec_page_code

"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi


