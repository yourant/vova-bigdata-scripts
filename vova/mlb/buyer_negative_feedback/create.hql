[9558] 推荐过滤用户负反馈
任务描述
需求背景：
从2.116.0开始，首页信息流开始支持用户进行负反馈，推荐页面需要在全站对用户负反馈的内容进行过滤。

需求描述：
（1）目前负反馈包括 用户对商品的负反馈 和 用户对组合卡片/会场卡片/纯图卡片的负反馈，

负反馈类型有
榜单级别：当用户对除商品卡片v2之外的卡片（即element_id中为空）有负反馈操作时（包括element_type为no_interest、ban //ban_category和ban_this、bought、disgusting的点击行为），则认为该用户对该榜单（bod_id）进行了负反馈（bod_id在extra的url中，需要解析出来）。
商品级别：当用户对商品卡片v2（即element_id中为虚拟id）有如下负反馈操作（包括element_type为no_interest、ban_this、bought的点击行为），则认为该用户对该商品（goods_id）进行了负反馈。
类目级别：当用户对商品卡片v2（即element_id中为虚拟id）有如下负反馈操作（包括element_type为ban_category的点击行为），则认为该用户对该类目（该商品对应的cat_id）进行了负反馈。
图片级别（分为ab两种类型）：当用户对商品卡片v2（即element_id中为虚拟id）有如下负反馈操作（包括element_type为disgusting的点击行为），则认为该用户对该图像相似组进行了负反馈。

（2）需要将相关数据清洗并按照服务过滤时需要的格式落表，负反馈设置有效期，有效期为三个月。

mlb_vova_buyer_negative_feedback

desc mlb.mlb_vova_goods_second_cat;
virtual_goods_id  int     商品虚拟id
  goods_id          int     商品id
  second_cat_id     int     二级品类id
  cat_id            int     品类ID
  group_id          int     商品组ID
  brand_id          int     品牌ID
  pt                string  NULL

create table themis.mlb_vova_buyer_negative_feedback(
  id                 int(11)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  buyer_id           bigint   NOT NULL COMMENT '用户id',
  feedback_type      int      NOT NULL COMMENT '负反馈类型,1:榜单级别(bod_id);2:商品级别(goods_id);3:类目级别(cat_id);4:图片级别',
  filter_type        int      NOT NULL COMMENT '屏蔽类型,11:榜单级别(bod_id);21:商品级别(goods_id);31:类目级别(cat_id);41:图片级别(商品);42:图片级别(商品组)',
  filter_value       bigint   NOT NULL COMMENT '屏蔽对应类型的值',

  create_time        datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  update_time        datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY buyer_feedback_type (buyer_id, feedback_type) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='推荐过滤用户负反馈';

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

负反馈设置有效期，有效期为三个月。
离线任务定时删除过期的负反馈





