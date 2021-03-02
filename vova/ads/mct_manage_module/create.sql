[7747]配合后台商家管理模块提供数据及接口
https://zt.gitvv.com/index.php?m=task&f=view&taskID=29340

https://docs.google.com/spreadsheets/d/1troDZ3suKun95JssUSSHkeufeYTjMnwran9BgY3tHbI/edit#gid=0
表名  字段名 字段业务口径（定义、公式或逻辑）    备注
店铺的类目数据表现
店铺名称  英文名称
品类名称  店铺所有的一级品类，及all  品类名称为all时的类目等级为所有一级品类的最高类目等级
类目等级  该店铺该品类对应的类目等级
店铺状态  在售，休假，禁售
近7日gmv  该店铺该品类下的近7日gmv
自上架在售商品数  该店铺该品类下的在架sn商品数
上新商品数（自上架）  该店铺该品类下的近一月在架上新sn商品数
克隆商品数 该店铺该品类下的在架克隆gsn商品数量
上新商品数（克隆） 该店铺该品类下的近一月在架上新克隆gsn商品数
商品动销率 该店铺该品类下的出单商品数/总在架商品数，当天
新品动销率 该店铺该品类下的近一月上新商品中出单商品数/近1月在架上新商品数，当天
在架brand商品数量占比 该店铺该品类下的在架brand商品数/在架商品数
物流原因退款率 该店铺该品类下的，物流原因退款率（9-12周）：前63-84天的物流原因退款子订单数/前63-84天的确认子订单数
非物流原因退款率  该店铺该品类下的，非物流原因退款率（5-8周）：前35-56天的非物流原因退款子订单数/前35-56天的确认子订单数
商家合规分数  待后台提供
账期  待后台提供

dim.dim_vova_merchant
desc ads.ads_vova_mct_rank ;
  first_cat_id            bigint
  mct_id                  bigint
  gmv_1m                  double
  bs_inter_rate_3_6w      double
  bs_lrf_rate_9_12w       double
  bs_nlrf_rate_5_8w       double
  bs_rep_rate_1mth        double
  gmv_rank                bigint
  rank                    bigint
  score                   bigint
  inter_rate_3_6w         double
  lrf_rate_9_12w          double
  nlrf_rate_5_8w          double
  rep_rate_1mth           double
  pt                      string

drop table ads.ads_vova_mct_manage_module;
CREATE  TABLE IF NOT EXISTS ads.ads_vova_mct_manage_module
(
    mct_id                          bigint         COMMENT '店铺ID',
    mct_name                        string         COMMENT '店铺英文名称',
    first_cat_id                    bigint         COMMENT '商品一级类目',
    first_cat_name                  string         COMMENT '商品一级类目名称',
    rank                            bigint         COMMENT '类目等级',
    mct_status                      bigint         COMMENT '店铺状态1：删除；2：休假；3：在售；4：禁售',
    gmv_day7                        DECIMAL(14, 4) COMMENT '近7日gmv',
    sn_goods_count                  bigint         COMMENT '在架sn商品数',
    sn_new_goods_count              bigint         COMMENT '近一月在架上新sn商品数',
    gsn_goods_count                 bigint         COMMENT '在架克隆gsn商品数量',
    new_gsn_goods_count             bigint         COMMENT '近一月在架上新克隆gsn商品数',
    sold_goods_count                bigint         COMMENT '出单商品数',
    on_sale_goods_count             bigint         COMMENT '总在架商品数',
    sold_new_goods_count            bigint         COMMENT '近一月上新商品中出单商品数',
    on_sale_new_goods_count         bigint         COMMENT '近1月在架上新商品数',
    on_sale_brand_goods_count       bigint         COMMENT '在架brand商品数',
    lrf_order_goods_count_9_12      bigint         COMMENT '前63-84天的物流原因退款子订单数',
    tot_order_goods_count_9_12      bigint         COMMENT '前63-84天的确认子订单数',
    nlrf_order_goods_count_5_8      bigint         COMMENT '前35-56天的非物流原因退款子订单数',
    tot_order_goods_count_5_8       bigint         COMMENT '前35-56天的确认子订单数',
    goods_turnover_rate             double         COMMENT '商品动销率，当天',
    new_goods_turnover_rate         double         COMMENT '新品动销率，当天',
    on_sale_brand_goods_count_rate  double         COMMENT '在架brand商品数量占比',
    lrf_rate_9_12w                  double         COMMENT '物流原因退款率（9-12周）',
    nlrf_rate_5_8w                  double         COMMENT '非物流原因退款率（5-8周）',
    no_brand_gmv_last7d             DECIMAL(14, 4) comment '近7日非brand gmv',
    no_brand_gmv_last1d             DECIMAL(14, 4) comment '近1日非brand gmv',
    no_brand_avg_cr_last7d          DECIMAL(14, 4) comment '近7日非brand平均转化率'
) COMMENT '后台商家管理模块(to es)'
PARTITIONED BY ( pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

-- 2021-01-15
新增字段
近7日非brand gmv	该店铺该品类下的近7日非brand gmv
近1日非brand gmv	该店铺该品类下的近1日非brand gmv
alter table ads.ads_vova_mct_manage_module add columns(`no_brand_gmv_last7d` DECIMAL(14, 4) comment '近7日非brand gmv') cascade;
alter table ads.ads_vova_mct_manage_module add columns(`no_brand_gmv_last1d` DECIMAL(14, 4) comment '近1日非brand gmv') cascade;

3#
-- 2021-01-25
新增字段
近7日非brand平均转化率: 近7日每天的非brand转化率加和 / 7
    非brand转化率=非brand商品支付人数 / 非brand商品曝光uv
alter table ads.ads_vova_mct_manage_module add columns(`no_brand_avg_cr_last7d` DECIMAL(14, 4) comment '近7日非brand平均转化率') cascade;




hadoop fs -du -s -h s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/*

hadoop fs -du -s -h /user/hive/warehouse/ads.db/ads_mct_manage_module/*

hadoop distcp -overwrite -m 30 hdfs://ha-nn-uri/user/hive/warehouse/ads.db/ads_mct_manage_module/  s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module

hadoop fs -du -s -h s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/*

emrfs sync s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/

msck repair table ads.ads_vova_mct_manage_module;
select * from ads.ads_vova_mct_manage_module limit 20;



hadoop distcp -overwrite -m 30 hdfs://ha-nn-uri/user/hive/warehouse/ads.db/ads_mct_manage_module/pt=2021-02-11  s3://bigdata-offline/warehouse/ads/ads_vova_mct_manage_module/pt=2021-02-11
