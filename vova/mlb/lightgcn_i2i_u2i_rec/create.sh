[9404]lightgcn取数调度

https://zt.gitvv.com/index.php?m=task&f=view&taskID=34453

confluence:
https://confluence.gitvv.com/pages/viewpage.action?pageId=21275052

1、数据来源
  dim.dim_vova_goods
  dim.dim_vova_buyers
  dws.dws_vova_buyer_goods_behave
  ads.ads_vova_goods_portrait
  每天在表更新后，数据组运行jar包取数

  jar包执行命令：
  spark-submit --master yarn --deploy-mode cluster --name lightgcn --class com.vova.model.lightgcn s3://vova-mlb/REC/util/lightgcn.jar 15

2、完成取数任务后，数据组发送jbname: data_lightgcn_rec 给服务。

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=data_lightgcn_rec --from=data --to=java_server --jtype=1D --retry=0