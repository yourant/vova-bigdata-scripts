#bin/sh
table="dwb_fd_mail_floryday_YearOnYear"
user="zhubao"

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

if [ ! -n "$1" ]; then
    pt=`date -d "- 1 days" +"%Y-%m-%d"`
    src_day=`date -d "- 1 days" +"%Y-%m-%d"`
    end_day=`date -d "- 30 days" +"%Y-%m-%d"`
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"
echo "src_day: ${src_day}"
echo "end_day: ${end_day}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql

spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=100" \
  --conf "spark.executor.memory=3g" \
  -d pt="${pt}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


spark-submit \
  --deploy-mode client \
  --name 'dwb_fd_mail_floryday_YearOnYear_send_email' \
  --master yarn  \
  --conf spark.executor.memory=1g \
  --conf spark.dynamicAllocation.maxExecutors=60 \
  --conf spark.executor.memoryOverhead=1024 \
  --class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
  --env prod \
  -sql "select
           date1
          ,round(this_year_GMV,2)
          ,round(this_year_sales_amount,2)
          ,this_year_paid_amount
          ,concat(round(this_year_paid_amount/this_year_dau*100,2),'%') this_year_cr

          ,round(last_year_GMV,2)
          ,round(last_year_sales_amount,2)
          ,last_year_paid_amount
          ,concat(round(last_year_paid_amount/last_year_dau*100,2),'%') last_year_cr

          ,round(this_year_PC_sales_amount,2)
          ,concat(round(this_year_PC_paid_amount/this_year_PC_dau*100,2),'%') pc_cr

          ,round(this_year_M_sales_amount,2)
          ,concat(round(this_year_M_paid_amount/this_year_M_dau*100,2),'%') m_cr

          ,round(this_year_IOS_sales_amount,2)
          ,concat(round(this_year_IOS_paid_amount/this_year_IOS_dau*100,2),'%') ios_cr

          ,round(this_year_Android_sales_amount,2)
          ,concat(round(this_year_Android_paid_amount/this_year_Android_dau*100,2),'%') android_cr
        from
          dwb.dwb_fd_mail_floryday_YearOnYear
        where
          date1 between date_sub('${pt}',29) and '${pt}'
        order by
          date1 desc"  \
  -head "floryday,Total(PC+Tablet+H5+IOS+Android)##4,去年同比数据##4,PC##2,H5##2,IOS##2,Android##2;Date,GMV(含运费),销售额,paid,CR(Paid/DAU)(%),GMV(含运费),销售额,paid,CR(Paid/DAU)(%),销售额,CR,销售额,CR,销售额,CR,销售额,CR"  \
  -receiver "bob.zhu@i9i8.com,eason.chen@i9i8.com,mixian@i9i8.com,lilia.wang@i9i8.com,michael.wang@i9i8.com,qiezi@vova.com.hk" \
  -title "fd_floryday同比30天报表(北京时间:${src_day}__${end_day})"


if [ $? -ne 0 ]; then
  exit 1
fi
echo "send email is finished !"