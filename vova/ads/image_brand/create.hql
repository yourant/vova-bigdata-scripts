[9464] brand图像识别接入后台打标流程
数据：
配合算法进行商品数据提取和定时任务部署；
统计符合条件的商品数量和每日产生的gmv。

###################
# 取数: 相似图片组中历史有销量的非brand商品
# s3://vova-computer-vision/product_data/vova_image_brand/src_data/
CREATE TABLE ads.ads_vova_no_brand_goods_img (
  goods_id bigint,
  img_id   bigint,
  img_url  string
) COMMENT 'brand图像识别接入后台打标-取数' PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vova-computer-vision/product_data/vova_image_brand/src_data/"
stored as textfile
;

# 导数
  s3://vova-computer-vision/product_data/vova_image_brand/dst_data/

drop table ads.ads_vova_image_brand_d;
msck repair table ads.ads_vova_image_brand_d;
create external TABLE ads.ads_vova_image_brand_d
(
  goods_id                bigint      COMMENT '用户id',
  img_id                  bigint      COMMENT '品类id',
  img_url                 string      COMMENT '推荐商品序列化结果',
  det_id                  int         COMMENT '检测类别id',
  det_name                string      COMMENT '检测类别名称',
  online_id               int         COMMENT '映射线上brand_id',
  det_conf                double      COMMENT '检测置信度',
  box                     string      COMMENT '检测框',
  cls_id                  int         COMMENT '分类id',
  cls_conf                double      COMMENT '分类置信度',
  not_fake_flag           int         COMMENT '是否正品,1正品,0赝品,-1没有分类',
  brand_id                int         COMMENT '最终brand_id',
  is_update               int         COMMENT '是否更新, 默认0'
) COMMENT '列表页个性化召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS textfile
LOCATION "s3://vova-computer-vision/product_data/vova_image_brand/dst_data/"

# 结果表需要处理后再导数： 需要对goods_id, brand_id去重，且brand_id <> -1
create TABLE ads.ads_vova_image_brand_d_export
(
    goods_id                bigint      COMMENT '用户id',
    brand_id                int         COMMENT '最终brand_id',
    is_update               int         COMMENT '是否更新, 默认0'
) COMMENT '列表页个性化召回结果表'
STORED AS PARQUETFILE
;


# mysql建表:
drop table als_images.ads_vova_image_brand_d;
create table if not exists als_images.ads_vova_image_brand_d (
  id                    bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id              bigint(11)  NOT NULL COMMENT '商品id',
  brand_id              bigint(11)  NOT NULL COMMENT '品牌id',
  is_update             int(1)      DEFAULT 0 COMMENT '是否更新，默认0',

  update_time           datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE,
  KEY update_time (update_time) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='brand图像识别接入后台打标'
;

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_vova_brand_qdata --from=data --to=cv --jtype=1D --retry=0

sh /mnt/vova-bigdata-scripts/common/job_message_get.sh --jname=ads_vova_brand_qdata --from=data --to=cv


sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_vova_brand_pcv --from=cv --to=data --jtype=1D --retry=0

sh /mnt/vova-bigdata-scripts/common/job_message_get.sh --jname=ads_vova_brand_pcv --from=cv --to=data

curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/out' -H 'Content-Type: application/json' --data-raw '{
"data":[
{
"jname": "ads_vova_brand_pcv",
"from": "cv",
"to": "data",
"jstatus": "success",
"jtype": "1D",
"retry": "0",
"freedoms":{"pt":"2021-05-28"}
}
]
}'

vova_sqoop_ads_vova_image_brand_d.flow

curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
data:{
"jname" : "ads_vova_brand_pcv",
"jfrom" : "cv",
"jto" : "data",
"project_name" : "vova_ads_d_chenkai",
"flow_name" : "vova_sqoop_ads_vova_image_brand_d",
"knock_alias":"kaicheng,chen.guixiong,Shuishan,weidu"
    }
}'

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_vova_image_brand_d --from=data --to=java_server --jtype=1D --retry=0











