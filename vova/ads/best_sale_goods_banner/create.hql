[7348]女装类目图谱应用于个性化会场搭建
需求描述
需求背景：
构建女装类目知识图谱，定义女装商品属性，并在线上进行落地。

需求描述：
1. 属性定义
参考shein，定义vova女装类目商品属性。
具体为：https://docs.google.com/spreadsheets/d/1aTeyVVCAN-2gNwh_Mt9C2SwWkV7byO_KGflom8lLbZI/edit#gid=1959360031

2. 属性填充
计算top2000女装商品（非brand）（#7313 vova女装品类top2000 gsn拉取），运营在商家后台完善商品属性标签。

3. 数据存储
将2000个完善好标签的女装商品写入es，以供查询等。

4. 个性化banner生成（暂时使用现有outlets流程）
将banner文案翻译成英法德意西繁体六种文案。
根据商品属性中的Style、Fit Type、Silhouette、Features、Function、Lining Material、Occasion任意挑选两个不重复的关键词（同一个属性中可能包含多个关键词，以“,”进行分割）yy, yy，再加二级品类名称（如果二级品类名称为xx, xx & xx形式），拼接成banner文案（关键词之间用空格分隔，单词首字母大写）。
banner文案形式为：
yy, yy, xx
UP TO 90% OFF
BUY NOW>
5. 个性化banner展示
当该用户的top30 rating商品中有2000个女装商品时，则将outlets首页banner位置转化为对应的女装个性化banner，点击进入会场后第一个商品换为该banner商品。
支持对这种形式的banner进行ab实验，rec_banner，o1-线上版本，o2-在线上版本增加了个性化女装banner，c-无个性化banner版本。o1和o2各灰度50%流量。


banner数据上传了
s3://vova-computer-vision/product_data/vova_best_sale_banner/goods_banner/tab/pt=2020-12-16/

goods_id, img_id, language, banner_url

分隔符    '\t'
hive: ads
mysql: rec-recall

drop table ads.ads_best_sale_goods_banner;
CREATE EXTERNAL TABLE ads.ads_best_sale_goods_banner (
    goods_id         bigint,
    img_id           bigint,
    language         string,
    banner_url       string
) COMMENT '女装类目图谱热销商品banner' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://vova-computer-vision/product_data/vova_best_sale_banner/goods_banner/tab/';

msck repair table ads.ads_best_sale_goods_banner;

create table if not exists rec_recall.ads_best_sale_goods_banner (
    id             int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    goods_id       int(11)       NOT NULL COMMENT '商品id',
    img_id         int(11)       NOT NULL COMMENT '图片id',
    languages_id   int(4)        NOT NULL COMMENT '语言ID',
    banner_url     varchar(255)  NOT NULL COMMENT 'banner_url',
    PRIMARY KEY (id) USING BTREE,
    KEY goods_id (goods_id) USING BTREE,
    UNIQUE KEY ux_goods_id (goods_id, languages_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='女装类目图谱热销商品banner';
