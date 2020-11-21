#!/bin/sh
home=`dirname "$0"`
cd $home

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

#app用户优惠券使用情况
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table dwb.dwb_fd_app_user_coupon_order_report PARTITION (pt)
select
project_name,
country_code,
coupon_config_id,
coupon_give_cnt,
coupon_used_cnt,
coupon_used_success_cnt,
coupon_used_1h_cnt,
coupon_used_24h_cnt,
coupon_used_48h_cnt,
coupon_used_72h_cnt,
coupon_used_greater_72h_cnt,
pt
from (
select
nvl(pt,'all') as pt,
nvl(project_name,'all') as project_name,
nvl(country_code,'all') as country_code,
nvl(coupon_config_id,'all') as coupon_config_id,
count(distinct coupon_give) as coupon_give_cnt , /*红包发放量*/
count(distinct coupon_used) as coupon_used_cnt, /*红包使用量*/
count(distinct coupon_used_success) as coupon_used_success_cnt, /*Coupon使用成功量*/
count(distinct coupon_used_1h) as coupon_used_1h_cnt, /*获取红包1h内使用量*/
count(distinct coupon_used_24h) as coupon_used_24h_cnt, /*获取红包1h-24h内使用量*/
count(distinct coupon_used_48h) as coupon_used_48h_cnt, /*获取红包24h-48h内使用量*/
count(distinct coupon_used_72h) as coupon_used_72h_cnt, /*获取红包48h-72h内使用量*/
count(distinct coupon_used_greater_72h) as coupon_used_greater_72h_cnt /*获取红包大于72h内使用量*/
from dwb.dwb_fd_app_user_coupon_order
where pt >= date_sub('$pt',30)
group by
pt,
project_name,
country_code,
coupon_config_id with cube
)tab where tab.pt != 'all';
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_app_user_coupon_gaohaitao"   --conf "spark.sql.output.coalesceNum=30" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=50" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: dwb_fd_app_user_coupon_gaohaitao table is finished !"

