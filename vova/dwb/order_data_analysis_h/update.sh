#!/bin/bash
spark-submit   \
--conf spark.executor.memory=10g \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.app.name=dwb_vova_order_data_analysis_h \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.rpt.OrderAndGMVAnalys s3://vomkt-emr-rec/jar/vova-order-and-gmv-analys.jar