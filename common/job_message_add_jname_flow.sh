#!/bin/bash
show_usage="args:[--jname=任务名, --jfrom=发送者, --jto=接收者, --project_name=azkaban project name, --flow_name=azkaban flow name, --knock_alias=通知者花名(逗号分隔)]"
ARGS=$(getopt -o jn --long jname:,jfrom:,jto:,project_name:,flow_name:,knock_alias: -- "$@")
eval set -- "${ARGS}"

uri="ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com"
jname=""
jfrom="java_server"
jto="data"
project_name=""
flow_name="success"
knock_alias="0"

while true; do
  case "$1" in
  --jname)
    jname=$2
    shift 2
    ;;
  --jfrom)
    jfrom=$2
    shift 2
    ;;
  --jto)
    jto=$2
    shift 2
    ;;
  --project_name)
    project_name=$2
    shift 2
    ;;
  --flow_name)
    flow_name=$2
    shift 2
    ;;
  --knock_alias)
    knock_alias=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "$show_usage"
    exit 1
    break
    ;;
  esac
done

if [ -z "$jname" ] || [ -z "$jfrom" ] || [ -z "$jto" ] || [ -z "$project_name" ] || [ -z "$flow_name" ] || [ -z "$knock_alias" ]; then
    echo "jname,jfrom,jto,project_name,flow_name,knock_alias can not be empty"
    exit 1
fi

dataRow='{
    "data":[
        {
            "jname":"'${jname}'",
            "jfrom":"'${jfrom}'",
            "jto":"'${jto}'",
            "project_name":"'${project_name}'",
            "flow_name":"'${flow_name}'",
            "knock_alias":"'${knock_alias}'"
        }
    ]
}'

echo "${dataRow}"
resp=`curl -s -H "Content-Type:application/json" -X POST --data-raw "${dataRow}" ${uri}/vova/api/jobmss/upsert-job-flow`
code=`echo $resp | jq '.code'`
echo "$resp"
# 完成则返回code=200, 否则认为未发送成功
if [[ "200" == "$code" ]];then
   echo ${jname} " send success"
   exit 0
fi
   echo ${jname} " send failed"
   exit 1
