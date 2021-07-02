[9485]搜索词会场个性化banner图像提取
任务描述
（1）取近30天高频搜索词结果下商品评分大于30的商品和对应的图像数据。
需要的字段为：
bod_id, query, goods_id, image_id, url, cat_id, brand_id, brand_name

（2）将生成好的banner图保存到数据库。

@@@@@@@@@@@@@@@@@@@@@@@@@@@@
-- 依赖:
desc mlb.mlb_vova_highfreq_query_match_d
  query_keys  string  归并映射后的query 与性别的组合，例如：query 为 nike, 性别为male 则query_key 为nike@male
  goods_list  string  序列化的商品列表，做大出500条，不足的从根据翻译后query从语义或者ES进行补充
  pt  string  NULL
  # Partition Information
  # col_name  data_type   comment
  pt  string  NULL

desc mlb.mlb_vova_highfreq_query_mapping_d
  source_origin   string  原始query
  target_query    string  归并映射mapping之后的query
  pt  string  NULL
  # Partition Information
  # col_name  data_type   comment
  pt  string  NULL

# 增量表
desc ads.ads_vova_image_matting;
  goods_id	    int	商品id
  img_id	    int	图片id
  url	        string	s3图片url
  old_url	    string	原始s3图片url
  pt	        string	NULL
  # Partition Information
  # col_name	data_type	comment
  pt	string	NULL


# 建 udf 函数
aws s3 cp s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar ./
mv vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar base64_to_long_udtf.jar
hadoop fs -put base64_to_long_udtf.jar /tmp/jar/
hadoop fs -ls /tmp/jar/

ods_vova_vts.ods_vova_brand
dim.dim_vova_goods
mlb.mlb_vova_highfreq_query_match_d
mlb.mlb_vova_highfreq_query_mapping_d
mlb.mlb_vova_rec_b_catgoods_score_d
ads.ads_vova_image_matting
ads.ads_vova_bod_name_translation

# 取数表: 每天一次 增量更新
drop table ads.ads_vova_highfreq_query_goods_banner;
create external table ads.ads_vova_highfreq_query_goods_banner
(
    source_origin         string COMMENT '原始query',
    query                 string COMMENT '归并映射mapping之后的query',
    goods_id              bigint COMMENT '商品ID',
    img_id                bigint COMMENT '图片ID',
    img_url               string COMMENT '图片url',
    cat_id                bigint COMMENT '品类ID',
    brand_id              bigint COMMENT '品牌ID',
    brand_name            string COMMENT '品牌名称',
    bod_id                bigint COMMENT '榜单id',
    bod_name_translation  string COMMENT '榜单名称(转译)',
    language_id           bigint COMMENT '语言id'
) COMMENT '搜索词会场个性化banner图像提取取数' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "s3://vova-computer-vision/product_data/vova_home_info_banner/src_data/";


# 输出表
msck repair table ads.ads_vova_home_info_banner;
create external table ads.ads_vova_home_info_banner
(
  goods_id      bigint COMMENT '商品ID',
  banner_url    string COMMENT 'banner图片地址',
  language_id   bigint COMMENT '语言id',
  bod_id	    bigint COMMENT '榜单id'
) COMMENT '搜索词会场个性化banner图像提取' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "s3://vova-computer-vision/product_data/vova_home_info_banner/dst_data/";

#每日增量图片
--jname=ads_vova_home_info_banner_qdata
--from=data
--to=cv

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_vova_home_info_banner_qdata --from=data --to=cv --jtype=1D --retry=0

sh /mnt/vova-bigdata-scripts/common/job_message_get.sh --jname=ads_vova_home_info_banner_qdata --from=data --to=cv



curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/out' -H 'Content-Type: application/json' --data-raw '{
"data":[
{
"jname": "ads_vova_home_info_banner_qdata",
"from": "data",
"to": "cv",
"jstatus": "success",
"jtype": "1D",
"retry": "0",
"freedoms":{"dt":"2021-05-30"}
}
]
}'


#每日增量处理
--jname=ads_vova_home_info_banner_pcv
--from=cv
--to=data

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_vova_home_info_banner_pcv --from=cv --to=data --jtype=1D --retry=0

sh /mnt/vova-bigdata-scripts/common/job_message_get.sh --jname=ads_vova_home_info_banner_pcv --from=cv --to=data

curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
data:{
"jname" : "ads_vova_home_info_banner_pcv",
"jfrom" : "cv",
"jto" : "data",
"project_name" : "vova_ads_d_chenkai",
"flow_name" : "vova_sqoop_ads_vova_home_info_banner",
"knock_alias":"kaicheng,chen.guixiong,Shuishan,weidu"
    }
}'





