[8713]红包商品报表
https://zt.gitvv.com/index.php?m=task&f=view&taskID=32162
任务描述
需求背景：需了解红包活动会场整体数据表现，观察会场的活动效果

需求内容：https://docs.google.com/spreadsheets/d/1aKnFw04E16OAJEj5dSp_BgXnQA_2uRJj47Rv9i7ASnQ/edit#gid=0

-- table1:
drop table dwb.dwb_vova_red_packet_mct;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_red_packet_mct (
  mct_id                  bigint           COMMENT 'd_商家ID',
  mct_name                string           COMMENT 'd_商家名称',
  first_cat_id            bigint           COMMENT 'd_一级品类ID',
  first_cat_name          string           COMMENT 'd_一级品类名称',
  is_brand                string           COMMENT 'd_是否brand(Y,N)',
  rank                    int              COMMENT 'i_店铺一级类目等级',
  first_cat_rank_gmv_avg  DECIMAL (14, 4)  COMMENT 'i_所属类目当前等级的gmv均值',
  red_packet_order_gmv    DECIMAL (14, 4)  COMMENT 'i_使用红包的子订单gmv',
  red_packet_discount     DECIMAL (14, 4)  COMMENT 'i_红包折扣',
  red_packet_gmv          DECIMAL (14, 4)  COMMENT 'i_红包带来的gmv(red_packet_order_gmv - red_packet_discount)',
  mct_gmv                 DECIMAL (14, 4)  COMMENT 'i_店铺gmv',
  coupon_num              int              COMMENT 'i_红包总数',
  used_num                int              COMMENT 'i_消耗红包数量',
  order_gsn_num           int              COMMENT 'i_有出单的红包gsn数',
  activity_gsn_num        int              COMMENT 'i_已成团的gsn数',
  no_end_gsn_num          int              COMMENT 'i_未售罄红包gsn数',
  end_gsn_num             int              COMMENT 'i_售罄红包gsn数',
  turnover_rate           double           COMMENT 'i_动销率',
  sell_out_rate           double           COMMENT 'i_售罄率'
) COMMENT '红包商品报表-店铺' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;

-- table2:
drop table dwb.dwb_vova_red_packet_mct;

CREATE TABLE IF NOT EXISTS dwb.dwb_vova_red_packet_goods (
  first_cat_id            bigint           COMMENT '一级品类ID',
  first_cat_name          string           COMMENT '一级品类名称',
  second_cat_id           bigint           COMMENT '二级品类ID',
  second_cat_name         string           COMMENT '二级品类名称',
  is_brand                string           COMMENT '是否brand(Y,N)',
  virtual_goods_id        bigint           COMMENT '商品虚拟ID',
  goods_id                bigint           COMMENT '商品ID',
  activity_start_time     timestamp        COMMENT '参与红包活动的开始时间',
  gsn_status              bigint           COMMENT '活动状态,0默认,1报名中,2补充报名,3活动中,4活动结束',
  mct_id                  bigint           COMMENT '商家ID',
  mct_name                string           COMMENT '商家名称',
  rank                    int              COMMENT '参与活动时店铺等级',
  coupon_num              bigint           COMMENT '报名红包数量',
  used_num                bigint           COMMENT '消耗红包数量',
  impression_pv           bigint           COMMENT '报名红包曝光pv',
  impression_uv           bigint           COMMENT '报名红包曝光uv',
  red_packet_order_gmv    DECIMAL (14, 4)  COMMENT '红包子订单gmv',
  red_packet_gmv          DECIMAL (14, 4)  COMMENT '使用红包的子订单金额(gmv-红包)',
  red_packet_order_cnt    int              COMMENT '子订单数',
  red_packet_avg_gmv      DECIMAL (14, 4)  COMMENT '红包商品均价',
  pay_uv                  int              COMMENT '红包活动中的支付人数',
  cr                      double           COMMENT '转化率'
) COMMENT '红包商品报表-商品' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;

-- table3:
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_red_packet_cat (
  first_cat_id                bigint   COMMENT '一级品类ID',
  first_cat_name              string   COMMENT '一级品类名称',
  second_cat_id               bigint   COMMENT '二级品类ID',
  second_cat_name             string   COMMENT '二级品类名称',
  is_brand                    string   COMMENT '是否brand(Y,N)',
  gsn_cnt                     bigint   COMMENT 'gsn总数',
  applying_gsn_cnt            bigint   COMMENT '报名中gsn总数',
  replenish_applying_gsn_cnt  bigint   COMMENT '补充报名中gsn总数',
  activity_gsn_cnt            bigint   COMMENT '活动中gsn总数',
  group_gsn_cnt               bigint   COMMENT '成团gsn数量',
  order_gsn_cnt               bigint   COMMENT '已出单gsn数量',
  sell_out_gsn_cnt            bigint   COMMENT '售罄红包gsn数量',
  turnover_rate               double   COMMENT '动销率',
  sell_out_rate               double   COMMENT '售罄率'
) COMMENT '红包商品报表-类目' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE
;




