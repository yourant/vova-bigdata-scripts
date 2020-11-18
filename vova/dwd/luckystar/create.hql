drop table if exists dwd.dwd_fact_vova_luk_act;
create table dwd.dwd_fact_vova_luk_act
(
    datasource     string comment '数据站点',
    act_id         bigint comment '活动id',
    act_cfg_id     bigint comment '活动配置id',
    start_time     timestamp comment '开始时间',
    end_time       timestamp comment '结束',
    round          bigint comment '轮次',
    crt_cnt        bigint comment '当前人数',
    act_sts        bigint comment '活动状态',
    prz_id         bigint comment '奖品id',
    gs_id          bigint comment '活动对应的goods_id',
    mkt_prc        bigint comment '价格',
    max_cnt        bigint comment '活动最大人数',
    act_itv        bigint comment '活动持续时间',
    act_cst_type   bigint comment '购买兑奖券的’货币‘类型',
    act_cst_val    bigint comment '购买兑奖券的’货币‘数值',
    act_grp_cfg_id bigint comment '成团的配置id',
    is_rapid       bigint comment '是否是快速夺宝活动配置',
    need_cnt       bigint comment '成团需要的人数'
) COMMENT '一元夺宝活动事实表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwd.dwd_fact_vova_luk_grp_act;
create table dwd.dwd_fact_vova_luk_grp_act
(
    datasource   string comment '数据站点',
    act_id       bigint comment '活动id',
    grp_id       bigint comment 'group id',
    grp_sts      bigint comment 'group status',
    grp_mbr_id   bigint comment 'group member id',
    byr_id       bigint comment 'buyer id',
    type         bigint comment '0 普通 1 加注',
    grp_role     bigint comment 'buyer_id角色',
    grp_mbr_sts  bigint comment 'group member status',
    ord_id       bigint comment '真实 order_id',
    vtl_ord_id   bigint comment '活动 order id',
    gs_cnt       bigint comment '商品数量',
    rwd_cpn_code bigint comment '奖励cpn_code',
    prz_id       bigint comment 'prize id',
    gs_id        bigint comment 'goods_id',
    mkt_prz      bigint comment '商品价格',
    win_sts      bigint comment '0: 不中奖， 1：选为中奖订单， 2：已中奖',
    lty_src      bigint comment '开奖类型，0手动选中中奖，1是半自动中奖，2是新加的自动中奖逻辑'
) COMMENT '一元夺宝活动成团事实表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
-- 一元夺宝活动成团事实表

