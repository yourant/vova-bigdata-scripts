个性化列表u2i召回

列表页实时u2i需要实时数据支持

限定条件
日期（必填）
组织（必填）	vova
平台（必填）	ios+android/所有平台
其他限定条件

字段说明
字段名	字段业务口径（定义、公式或逻辑）	备注
buyer_id	用户id
goods_id	商品id
cat_id	品类id
first_cat_id	一级品类id
second_cat_id	二级品类id
third_cat_id	三级品类id
brand_id
is_click	是否点击
is_collect	是否收藏
is_add_cart	是否加购
is_order	是否下单

"数据说明：每次更新获取当天0点开始当更新时刻的所有信息，直接覆盖原表。
例如17号从2点开始更新17号当天0点到2点的数据，4点的任务则是直接计算0点到4点的数据。
18号清空原表，计算从0点开始到当前时刻的所有数据。"

# 每小时执行一次， 只计算当天的打点数据，不需要分区
## FIXME: dim.dim_vova_goods 不是每小时更新
create table dws.dws_vova_buyer_goods_behave_h (
  buyer_id            bigint    COMMENT 'd_用户id',
  goods_id            bigint    COMMENT 'd_商品id',
  cat_id              bigint    COMMENT 'd_品类id',
  first_cat_id        bigint    COMMENT 'd_一级品类id',
  second_cat_id       bigint    COMMENT 'd_二级品类id',
  third_cat_id        bigint    COMMENT 'd_三级品类id',
  brand_id            bigint    COMMENT 'd_品牌id',

  impression_cnt      bigint      COMMENT 'i_曝光次数',
  clk_cnt             bigint      COMMENT 'i_点击次数',
  collect_cnt         bigint      COMMENT 'i_收藏次数',
  add_cat_cnt         bigint      COMMENT 'i_加购次数',
  ord_cnt             bigint      COMMENT 'i_购买次数' -- 打点中获取

) COMMENT '当天用户商品行为(每小时更新)'
     STORED AS PARQUETFILE;

hadoop fs -du -h -s s3://bigdata-offline/warehouse/dws/dws_vova_buyer_goods_behave_h/