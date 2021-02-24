#!/bin/bash
# 获取依赖任务当前执行周期中是否有执行成功过
show_usage="args:[--jname=任务名, --from=发送者, --to=接收者"
ARGS=$(getopt -o jn --long jname:,from:,to: -- "$@")
eval set -- "${ARGS}"

uri="ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com"
jname=""
from=""
to="data"

while true; do
  case "$1" in
  --jname)
    jname=$2
    shift 2
    ;;
  --from)
    from=$2
    shift 2
    ;;
  --to)
    to=$2
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

if [ -z "$jname" ] || [ -z "$from" ] || [ -z "$to" ]; then
    echo "jname,from,to can not be empty"
    exit 1
fi

echo "jname: ${jname}"

dataRow='{
    "data":{
            "jname":"'${jname}'",
            "from":"'${from}'",
            "to":"'${to}'"
    }
}'

# echo "${dataRow}"
resp=`curl ${uri}/vova/api/jobmss/get-last -s -H "Content-Type:application/json" -X POST --data-raw "${dataRow}" `

data=""
code=`echo $resp | jq '.code'`
data=`echo $resp | jq '.data'`
echo "data2 ${data}"

echo "$resp"
# code = 200 并且 data 不为空 则依赖任务当前执行周期有执行成功过
if [ "200" == "$code" ] && [ "${data}" != "null" ];then
   echo "${jname} success"
   exit 0
else
   echo "${jname} failed"
   exit 1
fi
