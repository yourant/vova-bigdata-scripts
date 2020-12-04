#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date  +%Y-%m-%d`
   # pt_last=`date -d "-2 days" +%Y-%m-%d`
    #pt_format=`date -d "-1 days" +%Y%m%d`
    #pt_format_last=`date -d "-2 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    #pt_last=`date -d "$1 -1 days" +%Y-%m-%d`
    #pt_format=`date -d "$1" +%Y%m%d`
   # pt_format_last=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $pt
#echo $pt_last
#echo $pt_format
#echo $pt_format_last

#shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_last_7_days_goods_rpt"


#hive -hiveconf pt=$dt -f ${shell_path}/dwb_fd_last_7_days_goods_rpt.hql


sql="
insert overwrite table dwb.dwb_fd_last_7_days_goods_rpt
SELECT
    project_name,
    goods_id,
    sum(goods_number) as goods_num
FROM (
select
    oi.project_name,
    oi.pay_status,
    og.goods_id,
    og.goods_number
    from (select
                order_id,
                project_name,
                order_time
        from ods_fd_vb.ods_fd_order_info
    where   oi.email  NOT REGEXP 'tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com'
    and to_date(oi.order_time)>= date_add(to_date(from_utc_timestamp('$pt','America/Los_Angeles')),-6)
                           and to_date(oi.order_time) < to_date(from_utc_timestamp('$pt','America/Los_Angeles'))
                           and pay_status>=1
         ) oi
    inner join ods_fd_vb.ods_fd_order_goods og
             on  oi.order_id=og.order_id
     left join ods_fd_vb.ods_fd_goods_project gp
     on og.goods_id=gp.goods_id and gp.is_on_sale=0
     )tab1
     group by project_name,goods_id
     having sum(goods_number)>=35;
"


spark-sql \
--conf "spark.app.name=dwb_fd_abtest_funnel_rate_rpt_yjzhang"   \
-d pt=$pt \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "last_7_days_goods report  table is finished !"
