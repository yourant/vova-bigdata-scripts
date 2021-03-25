#!/bin/bash
#指定日期和引擎
appId=$1
mkdir /mnt/yarn_logs
cd /mnt/yarn_logs
yarn logs --applicationId $appId > $appId.log
aws s3 cp $appId.log s3://vomkt-emr-rec/yarn_logs/

echo "日志上传到如下链接"
echo "https://s3.console.aws.amazon.com/s3/buckets/vomkt-emr-rec?prefix=yarn_logs%2F&region=us-east-1"
rm $appId.log
