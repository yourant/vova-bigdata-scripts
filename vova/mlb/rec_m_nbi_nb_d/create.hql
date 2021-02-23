[8429]Me页面推荐策略调整
需求背景：

经过一段时间的ab实验数据观察，对Me页面的召回策略进行调整，并清理掉一些不再用的标记位。

需求描述：
（1）优化NBI的算法和数据生成流程，具体见：https://confluence.gitvv.com/pages/viewpage.action?pageId=6112307；
（2）清理掉经过ab实验验证不符合预期的召回，包括：0 多融合搜索协同相似、9 黑五活动报名商品、10 黑五活动大盘商品、32 swing协同-nobrand-rating、63 双塔DNN、25-28 embedding协同session-点击、加车、收藏、下单。
（3）增加兜底策略，当用户所有推荐结果曝光后，则兜底补足200条。
（4）拆分vova&ac的推荐策略如下：

VOVA
调整Me页面的召回策略，并对订单召回进行ab实验。

依赖的表：dws.dws_vova_buyer_goods_behave, dim.dim_vova_goods

s3://vova-mlb/REC/model/nbi/nbi-1.0-SNAPSHOT.jar

DROP TABLE  mlb.mlb_vova_rec_m_nbi_nb_d;
create external TABLE  mlb.mlb_vova_rec_m_nbi_nb_d
(
    buyer_id        bigint COMMENT '用户id',
    rec_goods_list  string COMMENT 'base64 编码后的goods_id list',
    score_list      string COMMENT 'base64 编码后的goods_scores list'
)COMMENT 'NBI U2I召回' PARTITIONED BY (pt STRING)
STORED AS PARQUET
LOCATION "s3://vova-mlb/REC/data/match/nbi_recall/"
;

spark-submit --name nbi_v2_hel_chenkai_req7592 \
--master yarn --deploy-mode cluster \
--driver-memory 8G \
--executor-memory 8G \
--num-executors 100 \
--conf spark.akka.frameSize=1024 \
--conf spark.yarn.executor.memoryOverhead=5120 \
--conf spark.sql.broadcastTimeout=-1 \
--conf spark.dynamicAllocation.maxExecutors=120 \
--conf spark.driver.maxResultSize=10G \
--conf spark.storage.blockManagerTimeoutIntervalMs=10000 \
--conf spark.default.parallelism=1000 \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.memory.storageFraction=0.2 \
--conf spark.memory.fraction=0.8 \
--conf spark.sql.shuffle.partitions=500 \
--conf spark.dynamicAllocation.enabled=true \
--class com.vova.nbi \
s3://vova-mlb/REC/model/nbi/nbi-1.0-SNAPSHOT.jar 20 200






