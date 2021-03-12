#bin/sh
table="fd_cat_info"
key="cat_id"
sql="select cat_id,first_cat_id from dim.dim_fd_category where root_cat_id = 194"

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