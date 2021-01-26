DROP TABLE IF EXISTS dwb.dwb_vova_homepage_total_index;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_homepage_total_index
(
cur_date string COMMENT 'd_事件发生日期',
country string COMMENT 'd_国家',
platform string COMMENT 'd_平台',
app_version string COMMENT 'd_版本号',
channel_en string COMMENT 'd_渠道',
is_activate_user string COMMENT 'd_是否新用户',
homepage_expre_uv string COMMENT 'i_首页曝光uv',
homepage_expre_pv string COMMENT 'i_首页曝光pv',
homepage_clk_uv string COMMENT 'i_首页点击uv',
homepage_clk_pv string COMMENT 'i_首页点击pv',	
avg_clk string COMMENT 'i_首页平均点击次数',	
gmv string COMMENT 'i_首页营收',
direct_gmv string COMMENT 'i_首页直接营收',	
clk_value string COMMENT 'i_首页点击价值', 
leave_rate string COMMENT 'i_首页跳失率'
) COMMENT '首页整体指标' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



DROP TABLE IF EXISTS dwb.dwb_vova_homepage_total_efficiency;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_homepage_total_efficiency
(
cur_date string COMMENT 'd_事件发生日期',
country string COMMENT 'd_国家',
platform string COMMENT 'd_平台',
app_version string COMMENT 'd_版本号',
channel_en string COMMENT 'd_渠道',
is_activate_user string COMMENT 'd_是否新用户',
element_name string COMMENT 'i_活动名称',
page_code string COMMENT 'i_page_code',
active_expre_uv string COMMENT 'i_活动入口曝光uv',
active_expre_pv string COMMENT 'i_活动入口曝光pv',
homepage_clk_uv string COMMENT 'i_活动入口点击uv',	
homepage_clk_pv string COMMENT 'i_活动入口点击pv',	
ctr string COMMENT 'i_ctr',
active_clk_value_uv string COMMENT 'i_单活动点击价值uv',	
active_clk_value_pv string COMMENT 'i_单活动点击价值pv', 
click_mix string COMMENT 'i_click_mix',
gmv_mix string COMMENT 'i_gmv_mix',
unit_mix string COMMENT 'i_unit_mix',
gmv_change_rate string COMMENT 'i_转化效率（GMV）',
unit_change_rate string COMMENT 'i_转化效率（Unit）',
gmv string COMMENT 'i_gmv',
dv_mix string COMMENT 'i_dv_mix',
avg_pv string COMMENT 'i_人均PV',
avg_dv string COMMENT 'i_人均DV',
new_user_change string COMMENT 'i_大盘新客转化'
) COMMENT '首页入口效率' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;
	
