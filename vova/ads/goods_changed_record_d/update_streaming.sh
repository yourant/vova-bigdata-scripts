spark-submit \
--master yarn --deploy-mode cluster \
--num-executors 10 \
--conf spark.executor.memory=3g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.yarn.am.nodeLabelExpression=CORE \
--conf spark.yarn.executor.nodeLabelExpression=CORE \
--conf spark.app.name=goods_binlog_monitor \
--class com.vova.data.GoodsBinlogMonitor s3://vomkt-emr-rec/jar/vova-bd-goods-log-1.0.0.jar \
--env product --op kafka2s3  --basePath s3://vova-bd-offline/binlog/  \
--topic Vovavovadbthemischange-themis-goods --servers 172.31.47.121:9092,172.31.2.154:9092,172.31.66.172:9092 \
--batch_time 60
