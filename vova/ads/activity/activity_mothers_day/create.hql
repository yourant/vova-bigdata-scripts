[9345] 母亲节活动会场新增数据源需求
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34326

背景：母亲节活动需要新增7个数据源，实现方式为：1、每个数据源内商品由运营人工根据不同规则圈定（即提供商品清单）；2、算法把每个数据源开发成单独的biztype，并且每个biztype内商品走算法模型排序。

商品清单以及每个biztype命名见如下链接：

https://docs.google.com/spreadsheets/d/13gxYOib7gLwMR79bB3UygJBGjMZtebj3cKF2jubR15o/edit#gid=423196970

1. 母亲节活动数据源:
#
序号  一级品类                                                                biz_type    商品展示方式
1   Women's Clothing                                                         mqj_women   个性化排序
2   Bags, Watches & Accessories                                              mqj_bags
3   Health & Beauty                                                          mqj_health
4   Shoes                                                                    mqj_shoes
5   Home & Garden                                                            mqj_home
6   Baby Stuff                                                               mqj_baby
7   Electronics、Mobile Phones & Accessories、Pet products、Sports & Outdoors  mqj_others
按照cr进行排序。
预期开始日期：4月29日
有效期：5月31日
#'

####
sn/gsn  goods_id  虚拟id
aws s3 cp /mnt/chenkai/mothers_day_activity_goods.txt s3://bigdata-offline/warehouse/tmp/tmp_activity_mothers_day_goods/

aws s3 ls s3://bigdata-offline/warehouse/tmp/tmp_activity_mothers_day_goods/

-- 母亲节活动原始数据 数据由运营提供，只有一版数据，不用更新
create external TABLE tmp.tmp_activity_mothers_day_goods
(
    gsn               string COMMENT 'gsn',
    goods_id          bigint COMMENT '商品id',
    virtual_goods_id  bigint COMMENT '虚拟id'
) COMMENT '母亲节活动数据源'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "s3://bigdata-offline/warehouse/tmp/tmp_activity_mothers_day_goods/"
STORED AS textfile;

-- 输出：
create table if  not exists  ads.ads_vova_activity_mothers_day (
    goods_id                  bigint COMMENT 'i_商品ID',
    region_id                 int    COMMENT 'i_国家id',
    biz_type                  STRING COMMENT 'i_biz type',
    rp_type                   int    COMMENT 'i_rp type',
    first_cat_id              int    COMMENT 'd_一级品类id',
    second_cat_id             int    COMMENT 'd_二级品类id',
    rank                      bigint COMMENT 'd_排名'
)COMMENT '母亲节活动' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
