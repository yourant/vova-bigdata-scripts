[8367]商品列表主图展示
需求背景：
目前商品主图均由商家上传直接展示，风格差异较大，不同类型的商品混合展示时观感较差。计划将商品热门sku图处理成白底图，并用于主图展示，以提升商品点击转化和app整体观感

需求内容：
1.取近一个月有曝光的商品，取图：
  a.按销量top1sku（历史）→加车top1sku（一个月）→默认的顺位取商品最优一张的sku图
  b.取商品的所有图片。每日更新
2.批量处理商品图：对a图其进行抠图并处理成白底图；对b里的图进行分类和评分，取最优的一张图
3.所有推荐搜索接口支持输出商品+图片的组合，支持获取商品白底图和精选sku图
4.api在所有页面支持主图替换，对于接口有提供图片的商品，将该图作为主图展示。若无图片则仍取主图展示
5.该功能暂时只在首页开放，对有图片输出的商品做标记，recallpool标记位44。开ab，新老各50%流量


# 加车top1sku（一个月） sku 近 30 天加购 uv
# s3://bigdata-offline/warehouse/pdb/vova/vovadbthemis/themis/shopping_cart/pt=2021-02-09
CREATE EXTERNAL TABLE ods_vova_vts.ods_vova_shopping_cart_log_src (
    data string
) COMMENT '商品购物车blog' PARTITIONED BY (pt STRING, hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3://bigdata-offline/warehouse/pdb/vova/vovadbthemis/themis/shopping_cart/';


# add_time 从 2.1 号开始
# ods_vova_vts.ods_vova_shopping_cart_log_src 分区从 pt=2021-02-09 开始
create table ods_vova_vts.ods_vova_shopping_cart_log (
  user_id      bigint        comment '用户id',
  session_id   string        comment 'session_id',
  goods_id     bigint        comment '商品id',
  sku_id       bigint        comment 'sku',
  goods_sn     string        comment 'sn',
  market_price decimal(10,2) ,
  shop_price   decimal(10,2) comment '单价',
  goods_number int           comment '加购数量',
  is_real      int           ,
  parent_id    bigint        ,
  add_time     timestamp     comment '添加时间',
  is_sale      int
) COMMENT '商品购物车blog加车' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;