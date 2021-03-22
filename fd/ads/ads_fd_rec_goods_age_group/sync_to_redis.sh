#bin/sh
ts=$(date -d "$1" +"%Y-%m-%d %H")
if [ "$#" -ne 2 ]; then
  pt=$(date -d "$ts" +"%Y-%m-%d")
else
  pt=$2
fi

table="fd_goods_age_group"
key="goods_id"
sql="select goods_id,age_group from ads.ads_fd_goods_age_group where pt = '${pt}'"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.dynamicAllocation.maxExecutors=60 \
  --conf spark.executor.memory=4096M \
  --conf spark.yarn.appMasterEnv.sparkMaster=yarn \
  --conf spark.yarn.appMasterEnv.redisHost=realtime-rec-data.ijc0ku.ng.0001.use1.cache.amazonaws.com \
  --conf spark.yarn.appMasterEnv.redisTable=$table \
  --conf spark.yarn.appMasterEnv.redisKey=$key \
  --conf spark.yarn.appMasterEnv.sql="${sql}" \
  --conf spark.app.name=FDSparkToRedis_${table} \
  --class com.fd.bigdata.sparkbatch.util.job.SparkToRedis \
  s3://vomkt-emr-rec/jar/warehouse/fd/SparkToRedis.jar

if [ $? -ne 0 ]; then
  exit 1
fi

echo "sync redis [$table] is finished !"
