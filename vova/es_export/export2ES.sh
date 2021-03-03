#!/bin/bash
show_usage="args:[--sql= hive sql,--index=es index,--partition=spark partition num,--mode=complete or append,--id=id]"
ARGS=$(getopt -o src:t:ds:m --long sql:,index:,partition:,mode:,id: -- "$@")
eval set -- "${ARGS}"
partition=5
#pt=$(date -d "-1 day" +%Y-%m-%d)
mode="complete"
while true; do
  case "$1" in
  -sql | --sql)
    sql=$2
    shift 2
    ;;
  -index | --index)
    index=$2
    shift 2
    ;;
  -partition | --partition)
    partition=$2
    shift 2
    ;;
  -mode | --mode)
    mode=$2
    shift 2
    ;;
  -id | --id)
    id=$2
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



if [ "$mode" == "complete" ]
then
   indexName="${index}"
else
   indexName="${index}_${id}"
fi

alias="alias_${index}"

echo "sql:${sql},index:${indexName},alias:${alias}"

spark-submit \
--queue important \
--deploy-mode client \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.app.name="export2ES_${alias}" \
--conf spark.executor.memoryOverhead=2048 \
--driver-java-options "-Dlog4j.configuration=log4j-driver.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-executor.properties" \
--files hdfs:///conf/log4j-driver.properties,hdfs:///conf/log4j-executor.properties \
--class  com.vova.process.ExportHive2Es s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
-sql "${sql}"  \
-index $indexName \
-partition $partition \
-aliasName $alias \
-mode $mode


if [ $? -ne 0 ];then
  exit 1
fi
