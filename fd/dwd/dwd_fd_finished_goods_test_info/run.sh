#!/bin/sh
## 脚本参数注释:
## $1 日期%Y-%m-%d【非必传】

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit 1
    fi
    pt=$1

fi

#hive sql中使用的变量
echo $pt

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwd"

#
# hive -hiveconf pt=$pt -hiveconf mapred.job.name=fd_dwd_fd_finished_goods_test_info_ekkozhang -f ${shell_path}/dwd_fd_finished_goods_test_info/dwd_fd_finished_goods_test_info.hql

sql="
insert overwrite table dwd.dwd_fd_finished_goods_test_info
select t.goods_id,
       dg.virtual_goods_id,
       t.pipeline_id,
       s.project ,
       s.platform ,
       s.country,
       dg.cat_id ,
       dg.cat_name ,
       t.state,
       t.type_id,
       t.result,
       t.reason,
       t.production_reached,
       t.goods_type,
       t.goods_source,
       t.test_count,
       to_date(t.create_time) as create_time,
       to_date(t.last_update_time) as last_update_time
from (
         select goods_id,
                pipeline_id,
                state,
                type_id,
                create_time,
                result,
                reason,
                production_reached,
                goods_type,
                goods_source,
                test_count,
                last_update_time
         from ods_fd_vb.ods_fd_goods_test_goods
     ) t
         left join ods_fd_vb.ods_fd_goods_test_pipeline s on t.pipeline_id = s.pipeline_id
         left join dim.dim_fd_goods dg
                   on dg.goods_id = t.goods_id and dg.project_name = s.project;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=dwd_fd_finished_goods_test_info"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi