-- 商品画像表
drop table if exists ads.ads_vova_goods_portrait;
CREATE external TABLE `ads.ads_vova_goods_portrait`(
  `gs_id` bigint COMMENT 'd_商品id',
  `cat_id` bigint COMMENT 'i_品类id',
  `first_cat_id` bigint COMMENT 'i_一级品类id',
  `second_cat_id` bigint COMMENT 'i_二级品类id',
  `brand_id` bigint COMMENT 'i_品牌id',
  `shop_price` decimal(13,2) COMMENT 'i_商品价格',
  `gs_discount` decimal(13,2) COMMENT 'i_商品折扣',
  `shipping_fee` decimal(13,2) COMMENT 'i_商品运费',
  `mct_id` bigint COMMENT 'i_商家ID',
  `comment_cnt_6m` bigint COMMENT 'i_近180天评论数',
  `comment_good_cnt_6m` bigint COMMENT 'i_近180天好评数',
  `comment_bad_cnt_6m` bigint COMMENT 'i_近180天差评数',
  `gmv_1w` decimal(13,2) COMMENT 'i_近7天gmv',
  `gmv_15d` decimal(13,2) COMMENT 'i_近15天gmv',
  `gmv_1m` decimal(13,2) COMMENT 'i_近30天gmv',
  `sales_vol_1w` bigint COMMENT 'i_近7天销量',
  `sales_vol_15d` bigint COMMENT 'i_近15天销量',
  `sales_vol_1m` bigint COMMENT 'i_近30天销量',
  `expre_cnt_1w` bigint COMMENT 'i_近7天曝光数',
  `expre_cnt_15d` bigint COMMENT 'i_近15天曝光数',
  `expre_cnt_1m` bigint COMMENT 'i_近30天曝光数',
  `clk_cnt_1w` bigint COMMENT 'i_近7天点击数',
  `clk_cnt_15d` bigint COMMENT 'i_近15天点击数',
  `clk_cnt_1m` bigint COMMENT 'i_近30天点击数',
  `collect_cnt_1w` bigint COMMENT 'i_近7天收藏数',
  `collect_cnt_15d` bigint COMMENT 'i_近15天收藏数',
  `collect_cnt_1m` bigint COMMENT 'i_近30天收藏数',
  `add_cat_cnt_1w` bigint COMMENT 'i_近7天加购数',
  `add_cat_cnt_15d` bigint COMMENT 'i_近15天加购数',
  `add_cat_cnt_1m` bigint COMMENT 'i_近30天加购数',
  `clk_rate_1w` decimal(13,2) COMMENT 'i_近7天点击率',
  `clk_rate_15d` decimal(13,2) COMMENT 'i_近15天点击率',
  `clk_rate_1m` decimal(13,2) COMMENT 'i_近30天点击率',
  `pay_rate_1w` decimal(13,2) COMMENT 'i_近7天支付转换率',
  `pay_rate_15d` decimal(13,2) COMMENT 'i_近15天支付转换率',
  `pay_rate_1m` decimal(13,2) COMMENT 'i_近30天支付转换率',
  `add_cat_rate_1w` decimal(13,2) COMMENT 'i_近7天加购转化率',
  `add_cat_rate_15d` decimal(13,2) COMMENT 'i_近15天加购转化率',
  `add_cat_rate_1m` decimal(13,2) COMMENT 'i_近30天加购转化率',
  `cr_rate_1w` decimal(13,2) COMMENT 'i_近7天转换率',
  `cr_rate_15d` decimal(13,2) COMMENT 'i_近15天转换率',
  `cr_rate_1m` decimal(13,2) COMMENT 'i_近30天转换率',
  `key_words` string COMMENT '商品关键词',
  `gs_gender` string COMMENT '商品性别',
  `mp_clk_pv_1w` string COMMENT 'Most Popular页面被点击次数7天',
  `mp_clk_pv_15d` bigint COMMENT 'Most Popular页面被点击次数15天',
  `mp_clk_pv_1m` bigint COMMENT 'Most Popular页面被点击次数30天',
  `mp_cart_pv_1w` bigint COMMENT 'Most Popular页面被加购次数7天',
  `mp_cart_pv_15d` bigint COMMENT 'Most Popular页面被加购次数15天',
  `mp_cart_pv_1m` bigint COMMENT 'Most Popular页面被加购次数30天',
  `mp_clk_pv_1w_rk` bigint COMMENT 'Most Popular页面被点击次数7天排名',
  `mp_clk_pv_15d_rk` bigint COMMENT 'Most Popular页面被点击次数15天排名',
  `mp_clk_pv_1m_rk` bigint COMMENT 'Most Popular页面被点击次数30天排名',
  `mp_cart_pv_1w_rk` bigint COMMENT 'Most Popular页面被加购次数7天排名',
  `mp_cart_pv_15d_rk` bigint COMMENT 'Most Popular页面被加购次数15天排名',
  `mp_cart_pv_1m_rk` bigint COMMENT 'Most Popular页面被加购次数30天排名',
  `gr_1w` decimal(13,4) COMMENT '近一周gr',
  `gr_15d` decimal(13,4) COMMENT '近15天gr',
  `gr_1m` decimal(13,4) COMMENT '近一个月gr',
  `gcr_1w` decimal(13,4) COMMENT '近一周gcr',
  `gcr_15d` decimal(13,4) COMMENT '近15天gcr',
  `gcr_1m` decimal(13,4) COMMENT '近一个月gcr',
  `clk_uv_1w` bigint COMMENT '近一周点击人数',
  `clk_uv_15d` bigint COMMENT '近15天点击人数',
  `clk_uv_1m` bigint COMMENT '近一个月点击人数',
  `inter_rate_3_6w` decimal(13,4) COMMENT '前42天到前21天的订单7天上网率',
  `lrf_rate_9_12w` decimal(13,4) COMMENT '前84天至前63天订单的物流退款率',
  `nlrf_rate_5_8w` decimal(13,4) COMMENT '前56天至前35天订单的非物流退款率',
  `bs_inter_rate_3_6w` decimal(13,4) COMMENT '前42天到前21天的订单7天上网率(贝叶斯平滑0.9)',
  `bs_lrf_rate_9_12w` decimal(13,4) COMMENT '前84天至前63天订单的物流退款率(贝叶斯平滑0.1)',
  `bs_nlrf_rate_5_8w` decimal(13,4) COMMENT '前56天至前35天订单的非物流退款率(贝叶斯平滑0.1)')
COMMENT '商品画像表' PARTITIONED BY ( `pt` string) STORED AS PARQUETFILE;

alter table ads.ads_vova_goods_portrait add columns(`ord_cnt_1w` int comment '7天支付订单数') cascade;
alter table ads.ads_vova_goods_portrait add columns(`ord_cnt_15d` int comment '15天支付订单数') cascade;
alter table ads.ads_vova_goods_portrait add columns(`ord_cnt_1m` int comment '30天支付订单数') cascade;


alter table ads.ads_vova_goods_portrait add columns(`goods_id` int comment '商品ID') cascade;
alter table ads.ads_vova_goods_portrait add columns(`goods_name` string comment '商品名称') cascade;
alter table ads.ads_vova_goods_portrait add columns(`goods_sn` string comment '商品所属sn') cascade;
alter table ads.ads_vova_goods_portrait add columns(`is_on_sale` int comment '真实是否在售,1:已上架，0：已下架') cascade;
alter table ads.ads_vova_goods_portrait add columns(`is_recommend` int comment '是否可推荐,1:可，0：不可') cascade;


alter table ads.ads_vova_goods_portrait change column `goods_id` bigint comment '商品ID' cascade;

alter table ads.ads_vova_goods_portrait add columns(`third_cat_id` int comment '三级品类') cascade;
alter table ads.ads_vova_goods_portrait add columns(`fourth_cat_id` int comment '四级品类') cascade;


alter table ads.ads_vova_goods_portrait add columns(`goods_thumb` string comment '商品主图') cascade;

CREATE TABLE `tmp.tmp_vova_goods_key_words`(
  `goods_id` int,
  `goods_name` string,
  `goods_desc` string,
  `gender` string,
  `style` string,
  `season` string,
  `color` string,
  `model_number` string,
  `key_words` string,
  `last_update_time` string) stored as parquet;

##########
[9380] 推荐管理平台--商品评分查询模块新增逻辑-添加指标
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34369

平均上网天数 = 7天再往前一个月的确认订单的平均上网天数
alter table ads.ads_vova_goods_portrait add columns(`avg_inter_days_3_6w` int comment '商品平均上网天数') cascade;

############
[9698] 商品评分增加72小时集运入库率
https://zt.gitvv.com/index.php?m=task&f=view&taskID=35245
新增字段:
expre_uv_1w            bigint  近一周曝光UV
expre_uv_15d           bigint  近15天曝光UV
expre_uv_1m            bigint  近一月曝光UV

pay_uv_1w              bigint  近一周支付UV
pay_uv_15d             bigint  近15天支付UV
pay_uv_1m              bigint  近一月支付UV

entry_warehouse_72h_order_goods bigint  72小时入库订单数: 入库时间减订单确认的时间小于3天的子订单数
collection_order_goods          bigint  商品集运总订单数:3天前再往前一个月的确认子订单数

alter table ads.ads_vova_goods_portrait add columns(`expre_uv_1w` bigint comment '近一周曝光UV') cascade;
alter table ads.ads_vova_goods_portrait add columns(`expre_uv_15d` bigint comment '近15天曝光UV') cascade;
alter table ads.ads_vova_goods_portrait add columns(`expre_uv_1m` bigint comment '近一月曝光UV') cascade;

alter table ads.ads_vova_goods_portrait add columns(`pay_uv_1w` bigint comment '近一周支付UV') cascade;
alter table ads.ads_vova_goods_portrait add columns(`pay_uv_15d` bigint comment '近15天支付UV') cascade;
alter table ads.ads_vova_goods_portrait add columns(`pay_uv_1m` bigint comment '近一月支付UV') cascade;

alter table ads.ads_vova_goods_portrait add columns(`entry_warehouse_72h_order_goods` bigint comment '72小时入库订单数') cascade;
alter table ads.ads_vova_goods_portrait add columns(`collection_order_goods` bigint comment '商品集运总订单数') cascade;






