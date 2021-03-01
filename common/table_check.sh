table=$1
threshold=$2
where=$3

if [ ! -n "$3" ];then
  spark-submit \
  --deploy-mode client \
  --master yarn  \
  --driver-memory 4G \
  --conf "spark.dynamicAllocation.minExecutors=2" \
  --conf "spark.dynamicAllocation.initialExecutors=2" \
  --conf "spark.dynamicAllocation.maxExecutors=200" \
  --conf spark.app.name=HiveTableDataCheck \
  --conf spark.eventLog.enabled=false \
  --class com.vova.process.HiveTableDataCheck s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
  --env prod \
  --table $table \
  --threshold $threshold
else
  spark-submit \
  --deploy-mode client \
  --master yarn  \
  --driver-memory 4G \
  --conf "spark.dynamicAllocation.minExecutors=2" \
  --conf "spark.dynamicAllocation.initialExecutors=2" \
  --conf "spark.dynamicAllocation.maxExecutors=200" \
  --conf spark.app.name=HiveTableDataCheck \
  --conf spark.eventLog.enabled=false \
  --driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
  --class com.vova.process.HiveTableDataCheck s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
  --env prod \
  --table $table \
  --threshold $threshold \
  --where $where
fi



if [ $? -ne 0 ]; then
  echo "check failed"
  exit 1
else
  echo "check succ"
fi
