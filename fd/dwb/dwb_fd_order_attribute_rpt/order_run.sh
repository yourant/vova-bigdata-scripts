#!/bin/sh
if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt3=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt3=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt3

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_order_attribute_rpt"

#主流程事实表
#hive -hiveconf pt=$pt -f ${shell_path}/snowplow_order.hql

spark-sql \
  --conf "spark.app.name=snowplow_order_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=100" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/snowplow_order.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "snowplow_order table is finished !"
