[8367]商品列表主图展示
需求描述
需求背景：
目前商品主图均由商家上传直接展示，风格差异较大，不同类型的商品混合展示时观感较差。计划将商品热门sku图处理成白底图，并用于主图展示，以提升商品点击转化和app整体观感

需求内容：
1.取近一个月有曝光的商品，取图：a.按销量top1sku（历史）→加车top1sku（一个月）→默认的顺位取商品最优一张的sku图  b.取商品的所有图片。每日更新
2.批量处理商品图：对a图其进行抠图并处理成白底图（若抠图效果差则不处理）；对b里的图进行分类和评分，取最优的一张图
3.所有推荐搜索接口支持输出商品+图片的组合，支持获取商品白底图和精选sku图
4.api在所有页面支持主图替换，对于接口有提供图片的商品，将该图作为主图展示。若无图片则仍取主图展示
5.该功能暂时只在首页开放，对有图片输出的商品做标记，recallpool标记位44。开ab，a:b:c=25%:25%:50%，a/b分别为a图和b图的版本，c为原版

-- a 每日全量
create table ads.ads_vova_goods_white_bg_img_a_arc(
  goods_id  bigint  comment 'd_商品id',
  sku_id    bigint  comment 'd_sku id',
  img_id    bigint  comment 'd_图片id',
  img_url   string  comment 'i_图片url'
) COMMENT '商品列表主图展示a组全量'
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;
-- a 每日增量
drop table ads.ads_vova_goods_white_bg_img_a_inc;
create external table ads.ads_vova_goods_white_bg_img_a_inc(
  goods_id  bigint  comment 'd_商品id',
  sku_id    bigint  comment 'd_sku id',
  img_id    bigint  comment 'd_图片id',
  img_url   string  comment 'i_图片url'
) COMMENT '商品列表主图展示a组增量'
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vova-computer-vision/product_data/vova_goods_list_white_bg_image/src_data/a";

-- a 更新结果
drop table ads.ads_vova_goods_white_bg_img_res_a;
create external table ads.ads_vova_goods_white_bg_img_res_a(
  goods_id   int    comment 'd_商品id',
  img_id     int    comment 'd_图片id',
  old_url    string comment 'i_原图url',
  new_url    string comment 'i_新图url'
) COMMENT '商品列表主图展示a组结果'
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vova-computer-vision/product_data/vova_goods_list_white_bg_image/white_bg/a";

emrfs sync s3://vova-computer-vision/product_data/vova_goods_list_white_bg_image/white_bg/a


-- a mysql
drop table ads_vova_goods_white_bg_img_res_a;
create table ads_vova_goods_white_bg_img_res_a(
  id         int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id   int(11)       comment 'd_商品id',
  img_id     int(11)       comment 'd_图片id',
  old_url    varchar(500) comment 'i_原图url',
  new_url    varchar(500) comment 'i_新图url',
  update_time datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品列表主图展示a组结果';


# b 组
-- b 每日全量
drop table ads.ads_vova_goods_white_bg_img_b_arc;
create table ads.ads_vova_goods_white_bg_img_b_arc(
  goods_id   bigint    comment 'd_商品id',
  img_id     bigint    comment 'd_图片id',
  img_url    string    comment 'i_图片url',
  is_default bigint    comment 'i_是否为默认图',
  img_ctime  timestamp comment '图片上传时间'
) COMMENT '商品列表主图展示b组全量'
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;

-- b 每日增量
drop table ads.ads_vova_goods_white_bg_img_b_inc;
create external table ads.ads_vova_goods_white_bg_img_b_inc(
  goods_id  bigint  comment 'd_商品id',
  img_id    bigint  comment 'd_图片id',
  img_url   string  comment 'i_图片url',
  is_default bigint  comment 'i_是否为默认图'
) COMMENT '商品列表主图展示b组增量'
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vova-computer-vision/product_data/vova_goods_list_white_bg_image/src_data/b"
;

-- b 更新结果
drop table ads.ads_vova_goods_white_bg_img_res_b;
create external table ads.ads_vova_goods_white_bg_img_res_b(
  goods_id   int    comment 'd_商品id',
  img_id     int    comment 'd_图片id',
  old_url    string comment 'i_原图url',
  new_url    string comment 'i_新图url'
) COMMENT '商品列表主图展示b组结果'
PARTITIONED BY (pt string)
row format delimited fields terminated by ','
LOCATION "s3://vova-computer-vision/product_data/vova_goods_list_white_bg_image/white_bg/b"
;

-- b mysql
drop table ads_vova_goods_white_bg_img_res_b;
create table ads_vova_goods_white_bg_img_res_b(
  id         int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id   int(11)      comment 'd_商品id',
  img_id     int(11)      comment 'd_图片id',
  old_url    varchar(300) comment 'i_原图url',
  new_url    varchar(300) comment 'i_新图url',
  update_time datetime     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品列表主图展示b组结果';

alter table ads_vova_goods_white_bg_img_res_b ADD COLUMN update_time datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间' AFTER new_url;


