#!/bin/bash
#指定日期和引擎
sql="
insert overwrite table dwd.dwd_vova_fact_shield_goods
select
goods_id,
region_id,
mct_id,
shield_type,
create_time
from (select
        key_id goods_id,
        region_id,
        merchant_id mct_id,
        key_type shield_type,
        create_time
    from ods_vova_themis.ods_vova_merchant_region
    where key_type = 'goods'
    union all
    select
        g.goods_id,
        m.region_id,
        m.key_id mct_id,
        m.key_type shield_type,
        m.create_time
    from ods_vova_themis.ods_vova_merchant_region m
    left join ods_vova_themis.ods_vova_goods g on m.key_id = g.merchant_id
    where key_type = 'merchant'
    ) t
group by goods_id,
region_id,
mct_id,
shield_type,
create_time
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwd_vova_fact_shield_goods"   --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
