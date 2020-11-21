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

#脚本路径
#shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_country_order_rpt"

#报表country_order_report
sql="
INSERT overwrite table dwb.dwb_fd_country_order_rpt PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
nvl(tab1.project_name,'all') as project_name,
nvl(tab1.platform,'all') as platform,
nvl(tab1.country_code,'all') as country_code,
nvl(tab1.pay_status,'all') as pay_status,
nvl(COUNT(DISTINCT tab1.order_id),0) as orders, /*下单数*/
nvl(SUM(tab1.goods_amount)+SUM(tab1.shipping_fee),0.0) as order_amount_free,
nvl(SUM(tab1.goods_amount),0.0) as order_amount,
nvl(COUNT(DISTINCT tab1.user_id),0.0) as users, /*用户数*/
nvl(cast((SUM(tab1.goods_amount)+SUM(tab1.shipping_fee))/COUNT(DISTINCT tab1.user_id) as decimal(15,4)), 0.0) as customer_price_free,
nvl(cast(SUM(tab1.goods_amount)/COUNT(DISTINCT tab1.user_id) as decimal(15,4)) ,0.0) as customer_price,
nvl(SUM(tab1.shipping_fee),0.0) as shipping_free
from (

select
oi.order_id,
oi.user_id,
if(oi.is_app is null, 'other', if(oi.is_app = 0, 'web', 'mob'))  AS platform,
if(oi.device_type is null, 'other', oi.device_type)   AS device_type,
oi.platform_type as platform_type,
oi.order_time as order_time,
cast(oi.pay_status as string) as pay_status,
if(oi.pay_time < 0 or oi.pay_time is null, 0, oi.pay_time)  AS pay_time,
oi.country_code as country_code,
oi.language_code as language_code,
oi.order_currency_code as order_currency_code,
oi.project_name as project_name,
oi.goods_amount as goods_amount,
oi.goods_amount_exchange as goods_amount_exchange,
oi.bonus as bonus,
oi.shipping_fee as shipping_fee,
oi.shipping_fee_exchange as shipping_fee_exchange,
oi.email as email
from dwd.dwd_fd_order_info oi
where (date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) = '$pt' or date(from_unixtime(pay_time,'yyyy-MM-dd hh:mm:ss')) = '$pt')
and oi.email NOT REGEXP 'tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com'
)tab1
group by
tab1.project_name,
tab1.platform,
tab1.country_code,
tab1.pay_status with cube;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_newsletter_order_gaohaitao"   --conf "spark.sql.output.coalesceNum=30" --conf "spark.dynamicAllocation.minExecutors=10" --conf "spark.dynamicAllocation.initialExecutors=20" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi