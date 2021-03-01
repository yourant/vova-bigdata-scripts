#!/bin/bash
etime=$1
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"

sql="
insert overwrite table ads.ads_vova_goods_sn_to_id PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
goods_sn,
cast(collect_list(goods_id) as string) goods_id_list,
'top' source
from
(
select
goods_id,
goods_sn,
row_number() over(partition by goods_sn order by score desc) row_num
from ads.ads_vova_goods_id_behave
where pt='$pt'
) t where row_num<=3 group by goods_sn
union all
select
/*+ REPARTITION(1) */
goods_sn,
cast(collect_list(goods_id) as string) goods_id_list,
'lowprice' source
from
(
select
goods_id,
goods_sn,
row_number() over(partition by goods_sn order by show_price) row_num
from ads.ads_vova_goods_id_behave
where pt='$pt'
) t where row_num<=3
group by goods_sn;
"
spark-sql --conf "spark.app.name=ads_vova_goods_sn_to_id_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=50" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

#sqoop export \
#-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
#-Dmapreduce.job.queuename=default \
#-Dsqoop.export.records.per.statement=1000 \
#--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
#--username bimaster --password sYG2Ri3yIDu2NPki \
#--table rec_sntoid \
#--update-key "goods_sn,source" \
#--update-mode allowinsert \
#--hcatalog-database ads \
#--hcatalog-table ads_goods_sn_to_id \
#--hcatalog-partition-keys pt  \
#--hcatalog-partition-values  ${pt} \
#--fields-terminated-by '\001' \
#--columns "goods_sn,source,goods_id_list"
#if [ $? -ne 0 ];then
#   exit 1
#fi