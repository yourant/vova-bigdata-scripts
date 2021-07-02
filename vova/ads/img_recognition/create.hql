【数据】提取brand图像识别样本数据
需求背景：
目前打标流程需要接入brand图像识别，要先提供一批样本数据给运营确定打标规则。

需求描述：

（1）在近一个月有点击的商品里，随机取10w条非brand商品数据；

（2）需要的字段为：goods_id, img_id, img_url；

（3）将取好的数据放在s3上，供图像进行模型识别用。

drop table ads.ads_vova_brand_img_recognition_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_brand_img_recognition_d
(
    goods_id                  bigint COMMENT '商品id',
    img_id                    bigint COMMENT '图片id',
    img_url                   string COMMENT '图片链接'
) COMMENT 'brand图像识别'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vova-computer-vision/project/vova-brand-recognition/test_datasets/vova_image_check/input/';

