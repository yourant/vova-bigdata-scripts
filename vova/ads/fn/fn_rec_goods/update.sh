#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="

INSERT OVERWRITE TABLE ads.ads_fn_rec_b_session_data_d PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       log.domain_userid,
       fdg.goods_id,
       log.session_id,
       log.derived_tstamp
FROM dwd.dwd_vova_log_goods_click log
INNER JOIN dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.virtual_goods_id AND fdg.datasource = log.datasource
INNER JOIN dim.dim_zq_site fds on fds.datasource = log.datasource AND fds.domain_group = 'FN'
WHERE log.pt > date_sub('${cur_date}', 30)
  AND log.pt <= '${cur_date}'
  AND log.datasource = 'florynight'
;

INSERT OVERWRITE TABLE ads.ads_fn_rec_b_goods_map
select
/*+ REPARTITION(5) */
fdg.datasource,
fdg.goods_id,
t1.goods_id AS fn_goods_id
from
(
select
fdg.goods_id,
fdg.commodity_id
from
dim.dim_zq_goods fdg
where fdg.datasource = 'florynight'
AND fdg.commodity_id != 'NA'
) t1
INNER JOIN dim.dim_zq_goods fdg ON fdg.commodity_id = t1.commodity_id
INNER JOIN dim.dim_zq_site fds on fds.datasource = fdg.datasource AND fds.domain_group = 'FN'
;
"
echo "$sql"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=ads_fn_rec_b_session_data_d" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
