 #!/bin/bash
#status=50  --succ
#判断任务是否执行成功
#获取日期
#默认检查当前最新日期，如果想指定日期，则第二个参数传日期，如：sh /data1/scripts/etl-shell/judge/judge_check.sh 'ods_erp_clients' ’2019-03-22‘
#示例 sh /data1/scripts/etl-shell/judge/judge_check.sh 'ods_erp_clients'
api_url=10.108.11.8:18081
exec_job=$1
exec_date=$2
flow_name=$3
if [ ! -n "$2" ];then
exec_date=`date +%Y-%m-%d`
fi

resp=`curl -s -H "Content-Type:application/json" -X POST -d '{
    "date":"'${exec_date}'",
    "jobName":"'${exec_job}'",
    "flowName":"'${flow_name}'"
}' http://api_url/job/judgeJobStatus`
code=`echo $resp |jq '.code'`
echo "$resp"
if [[ "0" == "$code" ]];then
   exit 0
fi
   exit 1
