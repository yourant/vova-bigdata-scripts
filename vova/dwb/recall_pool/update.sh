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
--conf "spark.app.name=dwb_vova_recall_pool_v2" \
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
INSERT OVERWRITE TABLE tmp.tmp_vova_recall_pool_v2_tmp
select /*+ REPARTITION(1) */
a.datasource1,
a.rec_page_code1,
a.rp_name1,
a.is_single1,
a.rec_code1,
a.rec_version1,
a.expre_pv,
e.clk_pv,
d.expre_uv,
nvl(c.pay_uv, 0) pay_uv,
nvl(c.gmv, 0) gmv,
nvl(c.order_cnt, 0) order_cnt
from (
         select nvl(datasource, 'all')    datasource1,
                nvl(rec_page_code, 'all') rec_page_code1,
                nvl(rp_name, 'discard')   rp_name1,
                nvl(is_single, 'all')     is_single1,
                nvl(rec_code, 'all')      rec_code1,
                nvl(rec_version, 'all')   rec_version1,
                sum(cnt)                  expre_pv
         from (select datasource,
                      rec_page_code,
                      rec_code,
                      explode(split(concat(rp_name, ',all'), ',')) rp_name,
                      is_single,
                      rec_version,cnt
               from dwd.dwd_vova_ab_test_expre
               where pt = '${cur_date}') tmp
         group by cube (datasource, rec_page_code, rp_name, is_single, rec_code, rec_version)
         having rp_name1 != 'discard'
            and datasource1 != 'all'
            and is_single1 != 'NA'
     ) a
left join (

         select nvl(datasource, 'all')    datasource1,
                nvl(rec_page_code, 'all') rec_page_code1,
                nvl(rp_name, 'discard')   rp_name1,
                nvl(is_single, 'all')     is_single1,
                nvl(rec_code, 'all')      rec_code1,
                nvl(rec_version, 'all')   rec_version1,
                count(distinct device_id,buyer_id)                  expre_uv
         from (
              select datasource,rec_page_code,rec_code,rp_name,is_single,rec_version,device_id,buyer_id
              from (select datasource,rec_page_code,rec_code,explode(split(concat(rp_name, ',all'), ',')) rp_name,is_single,rec_version,device_id,buyer_id
                   from dwd.dwd_vova_ab_test_expre
                   where pt = '${cur_date}'
                   ) tmp2
               group by datasource,rec_page_code,rec_code,rp_name,is_single,rec_version,device_id,buyer_id
              ) tmp
         group by cube (datasource, rec_page_code, rp_name, is_single, rec_code, rec_version)
         having rp_name1 != 'discard'
            and datasource1 != 'all'
            and is_single1 != 'NA'

    ) d
on a.datasource1 = d.datasource1
and a.rec_page_code1 = d.rec_page_code1
and a.rec_code1 = d.rec_code1
and a.rec_version1 = d.rec_version1
and a.rp_name1 = d.rp_name1
and a.is_single1 = d.is_single1
left join (
         select nvl(datasource, 'all')    datasource1,
                nvl(rec_page_code, 'all') rec_page_code1,
                nvl(rp_name, 'discard')   rp_name1,
                nvl(is_single, 'all')     is_single1,
                nvl(rec_code, 'all')      rec_code1,
                nvl(rec_version, 'all')   rec_version1,
                sum(cnt)                  clk_pv
         from (select datasource,
                      rec_page_code,
                      rec_code,
                      explode(split(concat(rp_name, ',all'), ',')) rp_name,
                      is_single,
                      rec_version,cnt
               from dwd.dwd_vova_ab_test_clk
               where pt = '${cur_date}') tmp
         group by cube (datasource, rec_page_code, rp_name, is_single, rec_code, rec_version)
         having rp_name1 != 'discard'
            and datasource1 != 'all'
            and is_single1 != 'NA'
    ) e
on a.datasource1 = e.datasource1
and a.rec_page_code1 = e.rec_page_code1
and a.rec_code1 = e.rec_code1
and a.rec_version1 = e.rec_version1
and a.rp_name1 = e.rp_name1
and a.is_single1 = e.is_single1
left join (
         select nvl(datasource, 'all')    datasource1,
                nvl(rec_page_code, 'all') rec_page_code1,
                nvl(rp_name, 'discard')   rp_name1,
                nvl(is_single, 'all')     is_single1,
                nvl(rec_code, 'all')      rec_code1,
                nvl(rec_version, 'all')   rec_version1,
                count(distinct device_id,buyer_id)                  pay_uv,
                sum(price)                gmv,
                count(distinct order_goods_id) order_cnt
         from (select datasource,
                      rec_page_code,
                      rec_code,
                      explode(split(concat(rp_name, ',all'), ',')) rp_name,
                      is_single,
                      rec_version,price,device_id,buyer_id,order_goods_id
               from dwd.dwd_vova_ab_test_pay
               where pt = '${cur_date}') tmp
         group by cube (datasource, rec_page_code, rp_name, is_single, rec_code, rec_version)
         having rp_name1 != 'discard'
            and datasource1 != 'all'
            and is_single1 != 'NA'
    ) c
on a.datasource1 = c.datasource1
and a.rec_page_code1 = c.rec_page_code1
and a.rec_code1 = c.rec_code1
and a.rec_version1 = c.rec_version1
and a.rp_name1 = c.rp_name1
and a.is_single1 = c.is_single1
;

insert overwrite table dwb.dwb_vova_recall_pool_v2 PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
'${cur_date}' ptS,
tmp.datasource1,
tmp.rec_page_code1,
tmp.rp_name1,
tmp.is_single1,
tmp.rec_code1,
tmp.rec_version1,
tmp.expre_pv,
tmp.clk_pv,
tmp.expre_uv,
tmp.pay_uv,
0,
tmp.gmv,
concat(round(tmp.expre_pv * 100 / tmp2.expre_pv, 2),'%') expre_pv_rate,
concat(round(tmp.expre_uv * 100 / tmp2.expre_uv, 2),'%') expre_uv_rate,
'',
'',tmp.order_cnt
from tmp.tmp_vova_recall_pool_v2_tmp tmp
left join (select * from tmp.tmp_vova_recall_pool_v2_tmp where rp_name1 = 'all') tmp2
on tmp.datasource1 = tmp2.datasource1
and tmp.rec_page_code1 = tmp2.rec_page_code1
and tmp.rec_code1 = tmp2.rec_code1
and tmp.rec_version1 = tmp2.rec_version1
and tmp.is_single1 = tmp2.is_single1
;
"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

