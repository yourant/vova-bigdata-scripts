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

#创建表
hive -f ${shell_path}/dwd_fd_order_info/create_table.hql


spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.app.name=fd_dwd_order_info_gaohaitao"   \
--conf "spark.sql.output.coalesceNum=40" \
--conf "spark.dynamicAllocation.minExecutors=40" \
--conf "spark.dynamicAllocation.initialExecutors=60" \
-f ${shell_path}/dwd_fd_order_info/insert.hql


if [ $? -ne 0 ];then
  exit 1
fi
