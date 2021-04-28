#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
mon_date=`date -d "-31 day" +%Y-%m-%d`
fi
sql="
with tmp_img as (select
gg.img_id,
gg.goods_id,
gg.img_url
from ods_vova_vteos.ods_vova_goods_gallery gg
join dim.dim_vova_goods g on gg.goods_id = g.goods_id
where g.brand_id = 0 and gg.is_default=0
),


tmp_clk (
    select
        distinct b.goods_id
        from
        dwd.dwd_vova_log_goods_click a
    left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
    where a.datasource='vova' and a.pt between '$mon_date' and '$cur_date'
)

insert overwrite table ads.ads_vova_brand_img_recognition_d partition(pt='$cur_date')
select t2.goods_id,t2.img_id,t2.img_url from tmp_clk t1
left join tmp_img t2 on t1.goods_id=t2.goods_id
where t2.img_id is not null
limit 100000;
"
spark-sql --conf "spark.app.name=ads_vova_goods_pre_attribute_data" --conf "spark.dynamicAllocation.maxExecutors=300" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi