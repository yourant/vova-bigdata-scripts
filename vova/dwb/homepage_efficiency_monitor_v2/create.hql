【报表需求】[7445]“首页效率监控体系”新增字段和表格
https://zt.gitvv.com/index.php?m=task&f=view&taskID=28216
任务描述
PRD：https://docs.google.com/spreadsheets/d/19tVHh9CUMUdUMXkPy9Z5GxJuAKqVPYEVc6n_wqMB-Ag/edit?usp=sharing

打点文档：https://docs.google.com/spreadsheets/d/1swMq3nW4MxSWf0HfdpLxKyKoGLGSJtz5VT-W0ilacjs/edit#gid=891143247

首页效率监控体系-V2
需求方及需求号: 马聪, 7445
创建时间及开发人员：2020-12-26,陈凯
修改需求方及需求号:
修改人及修改时间:

CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_homepage_total_efficiency_v2(
  region_code              string        COMMENT '国家',
  platform                 string        COMMENT '平台',
  app_version              string        COMMENT '版本号',
  is_new                   string        COMMENT '是否新用户',
  module_name              string        COMMENT '模块名称',
  element_position         string        COMMENT '子模块位置',
  element_name             string        COMMENT '活动名称',
  entry_impre_uv           bigint        COMMENT '入口曝光uv',
  entry_impre_pv           bigint        COMMENT '入口曝光pv',
  entry_clk_uv             bigint        COMMENT '入口点击uv',
  entry_clk_pv             bigint        COMMENT '入口点击pv',
  homepage_clk_pv          bigint        COMMENT '首页点击pv',
  activity_gmv             decimal(16,4) COMMENT '单频道gmv',
  all_station_gmv          decimal(16,4) COMMENT '全站gmv',
  activity_goods_number    bigint        COMMENT '单频道销量',
  all_station_goods_number bigint        COMMENT '全站销量',
  activity_pv              bigint        COMMENT '频道内PV',
  activity_uv              bigint        COMMENT '频道内UV',
  activity_dv              bigint        COMMENT '频道内DV',
  activity_first_pay_uv    bigint        COMMENT '单频道首次支付成功用户数',
  activity_pay_uv          bigint        COMMENT '单频道支付成功用户数'
) COMMENT '首页入口效率监控V2报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_homepage_total_efficiency_v2/"
;

select *
from dwb.dwb_vova_homepage_total_efficiency_v2
where pt='2021-01-10'
  and region_code='all'
  and platform = 'all'
  and app_version  = 'all'
  and is_new = 'all'
  and element_position = 'all'
  and element_name = 'all'
;


select *
from rpt.rpt_homepage_total_efficiency_v2
where pt='2021-01-10'
  and region_code='all'
  and platform = 'all'
  and app_version  = 'all'
  and is_new = 'all'
  and element_position = 'all'
  and element_name = 'all'
;