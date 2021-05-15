create external table ads.ads_vova_image_banner(
goods_id int comment '商品id',
img_id int comment '图片id',
language string comment '语言code',
url  string comment 's3图片url',
is_used int comment '是否使用，1：使用，0未使用'
)
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vomkt-emr-rec/data/banner_data/tab/ads_vova_image_banner"
stored as textfile;


create external table ads.ads_vova_image_matting(
goods_id int comment '商品id',
img_id int comment '图片id',
url  string comment 's3图片url',
old_url string comment '原始s3图片url'
)
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vomkt-emr-rec/data/banner_data/tab/ads_vova_image_matting"
stored as textfile;



create external table ads.ads_vova_image_matting_old(
goods_id int comment '商品id',
url  string comment 's3图片url'
)
row format delimited fields terminated by ','
LOCATION "s3://vomkt-emr-rec/data/banner_data/tab/ads_vova_image_matting_old"
stored as textfile;


create table ads.ads_banner_image_pre(
goods_id int comment 'd_商品id',
img_id int comment 'd_图片id',
img_url string comment 'i_图片url',
is_default int comment 'i_是否主图',
clk_cnt_1m int comment 'i_最近一个月点击数'
)
PARTITIONED BY (pt string) COMMENT 'banner图片数据'
     STORED AS PARQUETFILE;


create table ads.ads_banner_image_pre_s3(
goods_id int comment 'd_商品id',
img_id int comment 'd_图片id',
img_url string comment 'i_图片url',
is_default int comment 'i_是否主图'
)
row format delimited fields terminated by ','
LOCATION "s3://vomkt-emr-rec/data/banner_data/tab/ads_banner_image_pre_s3";


create external table ads.ads_banner_image_pre_s3_v2(
goods_id int comment 'd_商品id',
img_id int comment 'd_图片id',
img_url string comment 'i_图片url',
is_default int comment 'i_是否主图'
)PARTITIONED BY (rand string)
row format delimited fields terminated by ','
LOCATION "s3://vomkt-emr-rec/data/banner_data/tab/ads_banner_image_pre_s3_v2";