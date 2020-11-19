#!/bin/sh
home=`dirname "$0"`
cd $home

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

#商品订单事实表dwd_fd_order_goods
#hive -hiveconf pt=$pt -hiveconf mapred.job.name=fd_dwd_fd_order_goods_gaohaitao -f ${shell_path}/dwd_fd_order_goods/dwd_fd_order_goods.hql

hive -f ${shell_path}/dwd_fd_order_goods/create_table.hql

spark-sql --master yarn  --driver-cores 2 --driver-memory 2g  --executor-memory 20g --executor-cores 5 --num-executors 10  --conf "spark.app.name=fd_dwd_fd_order_goods_gaohaitao" --conf "spark.sql.parquet.writeLegacyFormat=true" -f ${shell_path}/dwd_fd_order_goods/dwd_fd_order_goods.hql

if [ $? -ne 0 ];then
  exit 1
fi
