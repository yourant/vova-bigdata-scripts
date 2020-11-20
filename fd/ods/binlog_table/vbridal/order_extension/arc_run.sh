#!/bin/sh
## 脚本参数注释:
## $1 表名【必传】
## $2 日期 %Y-%m-%d【非必传】

if [[ $# -lt 1 ]]; then
        echo "脚本必传一个参数，该参数代表是要执行的表名 【字符串类型】!"
        exit 1

elif [[ $# -ge 1 && $# -le 2 ]]; then
        echo $1 | grep "[a-zA-Z]" > /dev/null
        if [[ $? -eq 1 ]]; then
                echo "第一个参数[ $1 ]不符合要执行的表名, 请输入正确的表名!"
                exit 1
        fi
        table_name=$1
        pt=`date -d "-1 days" +%Y-%m-%d`
        pt_last=`date -d "-2 days" +%Y-%m-%d`

        if [[ $# -eq 2 ]]; then
                echo $2 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $2 +%Y-%m-%d > /dev/null
                if [[ $? -ne 0 ]]; then
                        echo "接收的第二个参数【${2}】不符合:%Y-%m-%d 时间个数，请输入正确的格式!"
                        exit 1
                fi
                table_name=$1
                pt=$2
                pt_last=`date -d "$2 -1 days" +%Y-%m-%d`
        fi
fi

#hive sql中使用的变量
echo $table_name
echo $pt
echo $pt_last

#脚本路径
sql="
alter table ods_fd_vb.ods_fd_order_extension_arc drop if not exists partition (pt='$pt');

INSERT into table ods_fd_vb.ods_fd_order_extension_arc PARTITION (pt='$pt')
select /*+ REPARTITION(10) */ id, order_id, ext_name, ext_value, is_delete, last_update_time
from (
select pt, id, order_id, ext_name, ext_value, is_delete, last_update_time,
row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
from (
select
pt,
id,
order_id,
ext_name,
ext_value,
is_delete,
last_update_time
from ods_fd_vb.ods_fd_order_extension_arc where pt='$pt_last'
UNION
select
pt,
id,
order_id,
ext_name,
ext_value,
is_delete,
last_update_time
from ods_fd_vb.ods_fd_order_extension_inc where pt = '$pt'
)arc
) tab where tab.rank = 1;
"


spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_order_extension_arc_gaohaitao"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step: ${table_name}_arc table is finished !"

