#!/bin/bash
show_usage="args:[--jname=任务名, --from=发送者, --to=接收者, --jtype=任务类型(任务周期), --retry=是否强制重新执行, --freedoms=自定义参数(json)]"
ARGS=$(getopt -o jn --long jname:,from:,to:,jtype:,retry:,freedoms: -- "$@")
eval set -- "${ARGS}"

uri="ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com"
jname=""
from="data"
to="java"
jtype=""
jstatus="success"
retry="0"
freedoms=""

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
  --jtype)
    jtype=$2
    shift 2
    ;;
  --retry)
    retry=$2
    shift 2
    ;;
  --freedoms)
    freedoms=$2
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

if [ -z "$jname" ] || [ -z "$from" ] || [ -z "$to" ] || [ -z "$jtype" ]; then
    echo "jname,from,to,jtype can not be empty"
    exit 1
fi

cur_date=`date -d "-1 day" +%Y-%m-%d`

dataRow='{
    "data":[
        {
            "jname":"'${jname}'",
            "from":"'${from}'",
            "to":"'${to}'",
            "jstatus":"'${jstatus}'",
            "jtype":"'${jtype}'",
            "retry":"'${retry}'",
            "freedoms":"pt='${cur_date}'"
        }
    ]
}'

echo "${freedoms}"
if [ -n "${freedoms}" ]; then
dataRow='{
    "data":[
        {
            "jname":"'${jname}'",
            "from":"'${from}'",
            "to":"'${to}'",
            "jstatus":"'${jstatus}'",
            "jtype":"'${jtype}'",
            "retry":"'${retry}'",
            "freedoms":'${freedoms}'
        }
    ]
}'
fi

echo "${dataRow}"
resp=`curl -s -H "Content-Type:application/json" -X POST --data-raw "${dataRow}" ${uri}/vova/api/jobmss/out`
code=`echo $resp | jq '.code'`
echo "$resp"
# 发消息完成则返回code=200, 否则认为消息未发送成功
if [[ "200" == "$code" ]];then
   echo ${jname} " send success"
   exit 0
fi
   echo ${jname} " send failed"
   exit 1
