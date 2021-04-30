#!/bin/bash
spark-submit --master yarn --deploy-mode cluster --name me-u2i --class com.vova.model.order_i2i_u2i s3://vova-mlb/REC/util/me-u2i.jar
if [ $? -ne 0 ];then
  exit 1
fi
