#!/bin/sh

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_newsletter_rpt"

spark-sql \
  --conf "spark.app.name=dwb_fd_newsletter_snowplow_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt=$pt \
  -d pt_last=$pt_last \
  -f ${shell_path}/dwb_fd_newsletter_snowplow.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "dwb_fd_newsletter_snowplow table is finished !"

#spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_newsletter_sp_gaohaitao"   --conf "spark.sql.output.coalesceNum=30" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=40" -e "$sql"
