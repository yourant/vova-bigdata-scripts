#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-01`
fi
###逻辑sql
#dependence
#dim.dim_vova_merchant
#dim_vova_goods
#dwd_vova_fact_pay

sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_new_merchant_sponsor_gmv_m PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
t2.pt AS event_date,
t2.spsor_name,
t1.reg_cnt,
t2.activate_cnt,
t3.gmv,
t3.not_brand_gmv,
t4.gmv AS gmv_m,
t4.not_brand_gmv AS not_brand_gmv_m,
t2.pt
from
(
select
nvl(dm.spsor_name, 'NALL') AS spsor_name,
trunc(dm.first_publish_time, 'MM') AS pt,
count(*) AS activate_cnt
from
dim.dim_vova_merchant dm
WHERE trunc(dm.first_publish_time, 'MM') <= '${cur_date}'
AND trunc(dm.first_publish_time, 'MM') >= add_months('${cur_date}', -4)
group by nvl(dm.spsor_name, 'NALL'), trunc(dm.first_publish_time, 'MM')
) t2
LEFT JOIN
(
select
nvl(dm.spsor_name, 'NALL') AS spsor_name,
trunc(dm.reg_time, 'MM') AS pt,
count(*) AS reg_cnt
from
dim.dim_vova_merchant dm
WHERE trunc(dm.reg_time, 'MM') <= '${cur_date}'
AND trunc(dm.reg_time, 'MM') >= add_months('${cur_date}', -4)
group by nvl(dm.spsor_name, 'NALL'), trunc(dm.reg_time, 'MM')
) t1 ON t1.pt = t2.pt AND t1.spsor_name = t2.spsor_name
LEFT JOIN
(
select
nvl(dm.spsor_name, 'NALL') AS spsor_name,
trunc(dm.first_publish_time, 'MM') AS pt,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv,
sum(if(dg.brand_id = 0, fp.goods_number * fp.shop_price + fp.shipping_fee, 0)) as not_brand_gmv
from
dim.dim_vova_merchant dm
INNER JOIN dwd.dwd_vova_fact_pay fp on fp.mct_id = dm.mct_id
INNER JOIN dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
WHERE trunc(dm.first_publish_time, 'MM') <= '${cur_date}'
AND trunc(dm.first_publish_time, 'MM') >= add_months('${cur_date}', -4)
AND fp.pay_time >= dm.first_publish_time
AND date(fp.pay_time) <= date_add(date(dm.first_publish_time), 30)
group by nvl(dm.spsor_name, 'NALL'), trunc(dm.first_publish_time, 'MM')
) t3 ON t2.pt = t3.pt AND t2.spsor_name = t3.spsor_name
LEFT JOIN
(
SELECT
trunc(fp.pay_time, 'MM') AS pt,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv,
sum(if(dg.brand_id = 0, fp.goods_number * fp.shop_price + fp.shipping_fee, 0)) as not_brand_gmv
from
dwd.dwd_vova_fact_pay fp
INNER JOIN dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
WHERE trunc(fp.pay_time, 'MM') <= '${cur_date}'
AND trunc(fp.pay_time, 'MM') >= add_months('${cur_date}', -4)
group by trunc(fp.pay_time, 'MM')
) t4 ON t2.pt = t4.pt

;

INSERT OVERWRITE TABLE dwb.dwb_vova_new_merchant_gmv_d PARTITION (pt)
select
/*+ REPARTITION(1) */
date(dm.first_publish_time) AS event_date,
dm.spsor_name,
dm.mct_name,
dm.first_publish_time AS activate_time,
t2.on_sale_cnt,
t2.goods_gsn_cnt,
t2.goods_cnt,
t3.gmv,
t3.not_brand_gmv,
date(dm.first_publish_time) AS pt
from
dim.dim_vova_merchant dm
left join
(
select
dg.mct_id,
count(distinct if(dg.is_on_sale = 1, dg.goods_id, NULL)) AS on_sale_cnt,
count(distinct if(dg.goods_sn like 'GSN%', dg.goods_id, NULL)) AS goods_gsn_cnt,
count(*) AS goods_cnt
from
dim.dim_vova_goods dg
group by dg.mct_id
) t2 ON dm.mct_id = t2.mct_id
LEFT JOIN
(
select
dm.mct_id,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv,
sum(if(dg.brand_id = 0, fp.goods_number * fp.shop_price + fp.shipping_fee, 0)) as not_brand_gmv
from
dim.dim_vova_merchant dm
INNER JOIN dwd.dwd_vova_fact_pay fp on fp.mct_id = dm.mct_id
INNER JOIN dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
WHERE trunc(dm.first_publish_time, 'MM') <= '${cur_date}'
AND trunc(dm.first_publish_time, 'MM') >= add_months('${cur_date}', -4)
AND fp.pay_time >= dm.first_publish_time
AND date(fp.pay_time) <= date_add(date(dm.first_publish_time), 30)
group by dm.mct_id
) t3 ON dm.mct_id = t3.mct_id
WHERE trunc(dm.first_publish_time, 'MM') <= '${cur_date}'
AND trunc(dm.first_publish_time, 'MM') >= add_months('${cur_date}', -4)
;
"



#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_new_merchant_sponsor_gmv_m" \
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

