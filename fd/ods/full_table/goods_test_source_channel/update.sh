#!/bin/bash

#测款商品渠道来源

spark-submit --master yarn   \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.app.name=FDGoodsTestChannel \
--conf spark.executor.memoryOverhead=2048 \
--class com.fd.bigdata.sparkbatch.log.jobs.GoodsTestChannel \
s3://vomkt-emr-rec/jar/warehouse/fd/GoodsTestChannel.jar