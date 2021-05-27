[8721]首页上线新激活用户兴趣探测策略
https://zt.gitvv.com/index.php?m=task&f=view&taskID=32439
需求背景：
增加对新激活用户的兴趣探测，便于更快发掘用户的品类偏好。
本次先在vova上进行验证，如果效果较好，则推广到ac及app群上使用。

需求描述：
算法：
输出新激活用户兴趣探测模型，详细见：https://confluence.gitvv.com/pages/viewpage.action?pageId=21270610。

数据：
接入首页bestselling的实时曝光数据，并根据算法文档计算用户对品类的实时兴趣反馈，将uid、second_cat_id、α、β、time字段保存到redis中，供服务同学使用。
导入近180天有访问行为的用户的激活时间，供服务同学使用。

服务：
（1）用户兴趣探测策略只对新用户生效，新老用户根据近180天访问用户的激活时间进行区分，
新用户：激活时间小于等于10天的用户或取不到激活时间的用户；
老用户：激活时间大于10天的用户。
同时调整62 用户属性召回的新老用户区分策略同上。

（2）用户兴趣探测策略为重排序策略，即对经过排序模型排序后的结果再根据用户品类兴趣反馈进行一次重排序，
优先从未被曝光的剩余商品结果中按照用户品类兴趣反馈选择指定类目下rank分数最高的topn条结果进行输出，
如果该类目下商品结果数不足topn，则从es中选取得分较高的商品结果补足到n条，补足商品结果标记位为12。

（3）对该策略进行ab实验，具体为：
实验号：rec_home_sort，对照组-nodiscover：无新用户兴趣探索策略；实验组-todiscover：有新用户兴趣探索策略。


# 离线取 goods_id, second_cat_id
DROP TABLE  mlb.mlb_vova_goods_second_cat;
create table mlb.mlb_vova_goods_second_cat (
  virtual_goods_id  int  COMMENT '商品虚拟id',
  goods_id          int  COMMENT '商品id',
  second_cat_id     int  COMMENT '二级品类id',
  cat_id            int  COMMENT '品类id',
  group_id          int  COMMENT '商品组id',
  brand_id          int  comment '品牌ID'
) COMMENT '在架商品及二级品类id' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE;
;
hadoop fs -du -s -h s3://bigdata-offline/warehouse/mlb/mlb_vova_goods_second_cat/*

*/#
select max(virtual_goods_id),max(goods_id),max(second_cat_id) from  dim.dim_vova_goods;
# 61135477	59684530	6025

create table themis.mlb_vova_goods_second_cat(
  id                 int(11)     NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  virtual_goods_id   int         NOT NULL COMMENT '商品虚拟id',
  goods_id           int         NOT NULL COMMENT '商品id',
  second_cat_id      int         NOT NULL COMMENT '二级品类id',
  cat_id             int         NOT NULL COMMENT '品类id',
  group_id           int         NOT NULL COMMENT '商品组id',
  brand_id           int         NOT NULL COMMENT '品牌ID',
  update_time        datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id) USING BTREE,
UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='在架商品及二级品类id';


#########################
[9558]推荐过滤用户负反馈
https://zt.gitvv.com/index.php?m=task&f=view&taskID=35131
在mlb.mlb_vova_goods_second_cat 中添加两个字段:
cat_id
group_id
brand_id

alter table mlb.mlb_vova_goods_second_cat add columns(`cat_id`   int comment '品类ID') cascade;
alter table mlb.mlb_vova_goods_second_cat add columns(`group_id` int comment '商品组ID') cascade;
alter table mlb.mlb_vova_goods_second_cat add columns(`brand_id` int comment '品牌ID') cascade;

// 商品分组
desc ods_vova_vbts.ods_vova_rec_gid_pic_similar;
  id          bigint
  goods_id    bigint
  group_id    bigint
  update_time timestamp














