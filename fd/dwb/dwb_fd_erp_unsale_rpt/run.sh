#!/bin/sh

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt3=`date -d "-4 days" +%Y-%m-%d`
    pt11=`date -d "-12 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt3=`date -d "$1 -3 days" +%Y-%m-%d`
    pt11=`date -d "$1 -11 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt3
echo $pt11

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_erp_unsale_rpt"

#最近14天每天平均销量
spark-sql \
  --conf "spark.app.name=erp_unsale_detail_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=10" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_unsale_detail.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_unsale_detail table is finished !"

#月销售数
spark-sql \
  --conf "spark.app.name=erp_unsale_rpt_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=40" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_unsale_rpt.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_unsale_rpt table is finished !"
