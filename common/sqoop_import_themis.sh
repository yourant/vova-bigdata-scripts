#!/bin/bash
show_usage="args:[--db_code= MySQL地址CODE, --etl_type=etl类型, --pt=时间, --table_name=表名,--mapers=并发数量(数字)，--split_id=并发切割ID(默认MySQL第一个主键),--inc_column=增量字段(只允许日期类型或者数字ID),--partition_num=ods表文件数量, --period_type=间隔类型（day，hour两种,--primary_key=主键）]"
#1 判断有没有传入参数
if [ ! -n "$1" ];then
    echo $show_usage
    exit 1
fi
api_url=10.108.11.8:18081
ARGS=$(getopt -o jn --long db_code:,etl_type:,pt:,table_name:,mapers:,split_id:,inc_column:,partition_num:,period_type:,primary_key: -- "$@")
eval set -- "${ARGS}"
inc_column=""
#2 定义一些变量　mapers 默认为３　date 默认为前1天
mapers=3
pt=$(date -d "-1 day" +%Y-%m-%d)
primary_key=""
etl_type="INIT"
table=""
inc_column="last_update_time"
mt="spark"
#3 table文件数量
partition_num=100
#4 默认类型是天，可选hour
period_type=day
yarn_queue=default


while true; do
  case "$1" in
    --db_code)
    db_code=$2
    shift 2
    ;;
    --etl_type)
    etl_type=$2
    shift 2
    ;;
    --pt)
    pt=$2
    shift 2
    ;;
    --table_name)
    table_name=$2
    shift 2
    ;;
    --mapers)
    mapers=$2
    shift 2
    ;;
    --split_id)
    split_id=$2
    shift 2
    ;;
    --inc_column)
    inc_column=$2
    shift 2
    ;;
    --partition_num)
    partition_num=$2
    shift 2
    ;;
    --period_type)
    period_type=$2
    shift 2
    ;;
    --primary_key)
    primary_key=$2
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

if [ "$period_type" == "hour" ];then
    yarn_queue=important
fi

#5 检查数据列是否变动
resp=$(curl -s -H "Content-Type:application/json" -X POST -d '{
    "dbCode":"'${db_code}'",
    "tableName":"'${table_name}'",
    "periodType":"'${period_type}'",
    "etlType":"'${etl_type}'"
}' http://${api_url}/ddl/checkTable)
code=$(echo $resp | jq '.code')
if [[ "$code" == "" ]]; then
  echo "服务器异常，请检查服务器"
  exit 1
fi
if [[ "0" != "$code" ]]; then
  echo "检查表列出错"
  echo $resp
  exit 1
fi
#6 从返回的json中取出hiveTable 例："incTable":"vova_order_info_inc",这里取出vova_order_info_inc
hiveTable=$(echo $resp | jq '.incTable'| sed $'s/\"//g')
#7 取出相应的列
hiveColumns=$(echo $resp | jq '.columns'| sed $'s/\"//g')
#8 如果pri主键没有传入，也从resp里面取
if [  "$primary_key" = "" ]; then
  primary_key=$(echo $resp | jq '.pri'| sed $'s/\"//g')
fi
url=$(echo $resp | jq '.url'| sed $'s/\"//g')
user=$(echo $resp | jq '.user'| sed $'s/\"//g')
pwd=$(echo $resp | jq '.pwd'| sed $'s/\"//g')
hiveDb=$(echo $resp | jq '.hiveDb'| sed $'s/\"//g')
#9 删除临时目录为后面做准备
#note:sqoop先将数据导入到HDFS的临时目录,然后再将导入到HDFS的数据迁移到Hive仓库,第一步默认的临时目录是hdfs:///tmp/sqoop/themis/vova_order_info_inc
tmp_path=s3://bigdata-offline/tmp/sqoop/${hiveDb}/${table_name}
hadoop fs -rm -r $tmp_path
#10 根据不同的etl_type　ALL,INIT,INCTIME,INCID　走不同的逻辑处理
#note:target-dir #11已解释
if [[ "$etl_type" == "ALL" || "INIT" == "$etl_type" ]]; then
  sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" -Dmapreduce.job.queuename=${yarn_queue} \
    --connect ${url} \
    --username ${user} \
    --password ${pwd} \
    --hive-import \
    --hive-overwrite \
    --hive-database ${hiveDb} \
    --hive-table ${hiveTable} \
    --fields-terminated-by '\0x01' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-partition-key pt \
    --hive-partition-value ${pt} \
    --hive-delims-replacement ' ' \
    --target-dir $tmp_path \
    --fetch-size 10000 \
    --query "select ${hiveColumns} from ${table_name} where \$CONDITIONS" \
    --split-by ${primary_key} -m ${mapers}
fi

if [ $? -ne 0 ]; then
  exit 1
fi

if [ "$etl_type" == "INCTIME" ]; then
  sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" -Dmapreduce.job.queuename=${yarn_queue} \
    --connect  ${url} \
    --username ${user} \
    --password ${pwd} \
    --hive-import \
    --hive-overwrite \
    --hive-database ${hiveDb} \
    --hive-table ${hiveTable} \
    --fields-terminated-by '\0x01' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-partition-key pt \
    --hive-partition-value ${pt} \
    --hive-delims-replacement ' ' \
    --target-dir $tmp_path \
    --fetch-size 10000 \
    --query "select ${hiveColumns} from ${table_name} where ${inc_column} >= '${pt}' and \$CONDITIONS" \
    --split-by ${primary_key} -m ${mapers}
fi

if [ $? -ne 0 ]; then
  exit 1
fi


if [ "$etl_type" == "INCTIMENOMERGE" ]; then
  sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" -Dmapreduce.job.queuename=${yarn_queue} \
    --connect ${url} \
    --username ${user} \
    --password ${pwd} \
    --hive-import \
    --hive-overwrite \
    --hive-database ${hiveDb} \
    --hive-table ${hiveTable} \
    --fields-terminated-by '\0x01' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-partition-key pt \
    --hive-partition-value ${pt} \
    --hive-delims-replacement ' ' \
    --target-dir $tmp_path \
    --fetch-size 10000 \
    --query "select ${hiveColumns} from ${table_name} where ${inc_column} >= '${pt} 00:00:00' and ${inc_column} <= '${pt} 23:59:59'  and \$CONDITIONS" \
    --split-by ${primary_key} -m ${mapers}
fi

if [ $? -ne 0 ]; then
  exit 1
fi


if [ "$etl_type" == "INCID" ]; then

  inc=$(curl -s -H "Content-Type:application/json" -X POST -d '{
    "dbCode":"'${db_code}'",
    "incColumn":"'${inc_column}'",
    "tableName":"'${table_name}'",
    "pt":"'${pt}'"
    }' http://${api_url}/dql/getMaxId)
  if [[ "0" != "$code" ]]; then
    echo "获取最大值失败"
    echo $inc
    exit 1
  fi
  idValue=$(echo $inc | jq '.idValue')
  echo $inc
  sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" -Dmapreduce.job.queuename=${yarn_queue} \
    --connect ${url} \
    --username ${user} \
    --password ${pwd} \
    --hive-import \
    --hive-overwrite \
    --hive-database ${hiveDb} \
    --hive-table ${hiveTable} \
    --fields-terminated-by '\0x01' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-partition-key pt \
    --hive-partition-value ${pt} \
    --hive-delims-replacement ' ' \
    --fetch-size 10000 \
    --target-dir $tmp_path \
    --query "select ${hiveColumns} from ${table_name} where ${inc_column}  > ${idValue} and \$CONDITIONS" \
    --split-by ${primary_key} -m ${mapers}
fi

if [ $? -ne 0 ]; then
  exit 1
fi
#11 再次优化，如果etlType是ALL,INIT不会走merge，直接正常退出

#12 根据pri,table,datasource,pt拿到merge的SQL
merge_code=$(curl -s -H "Content-Type:application/json" -X POST -d '{
    "dbCode":"'${db_code}'",
    "primaryKey":"'${primary_key}'",
    "tableName":"'${table_name}'",
    "pt":"'${pt}'",
    "incColumn":"'${inc_column}'",
    "etlType" : "'${etl_type}'",
    "partitionNum" : "'${partition_num}'",
    "periodType" : "'${period_type}'",
    "mt" : "'${mt}'"
    }' http://${api_url}/dql/mergeSql)
flag=$(echo $merge_code | jq '.code')
if [[ "0" == "$flag" ]]; then
  echo "$merge_code"
else
  echo "$merge_code"
  echo "merge数据失败"
  exit 1
fi

sql=$(echo "$merge_code" | jq '.sql')

if [[ "hive" == "$mt" ]]; then
  echo "merge with hive enginer"
  hive -e "$sql"
else
  echo "merge with spark enginer"
  spark-sql \
  --queue ${yarn_queue} \
  --executor-memory 6G --executor-cores 1 \
  --conf "spark.sql.parquet.writeLegacyFormat=true"  \
  --conf "spark.dynamicAllocation.minExecutors=5" \
  --conf "spark.dynamicAllocation.initialExecutors=20" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  --conf "spark.default.parallelism = 380" \
  --conf "spark.sql.shuffle.partitions=380" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.join.enabled=true" \
  --conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
  --conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
  --conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
  --conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
  --conf "spark.sql.autoBroadcastJoinThreshold=10485760"  \
  --conf "spark.sql.broadcastTimeout=600" \
  --conf "spark.network.timeout=300" \
  --name $hiveTable -e "$sql"
fi

if [ $? -ne 0 ]; then
  exit 1
fi