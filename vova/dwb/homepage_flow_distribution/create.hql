[5371]首页各资源位流量分配报表
https://zt.gitvv.com/index.php?m=task&f=view&taskID=21448

任务描述
https://docs.qq.com/sheet/DVWp1b21CUndyblVS?tab=9xuwn0

日期
国家
平台
渠道
激活时间
排序  取element_position，all/1/2/3/4/...该筛选项对报表中已有表格不起作用。

首页各资源位流量分配报表

drop table dwb.dwb_vova_homepage_flow_distribution;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_homepage_flow_distribution (
datasource                    string   COMMENT 'd_datasource',
region_code                   string   COMMENT 'd_国家/地区',
platform                      string   COMMENT 'd_平台',
main_channel                  string   COMMENT 'd_主渠道',
is_new                        string   COMMENT 'd_是否新激活',
element_position              string   COMMENT 'd_element_position',

searchtab_impr_uv                    string  COMMENT 'i_首页搜索框曝光UV',
top_navigation_impr_uv               string  COMMENT 'i_顶部分类曝光UV',
banner_impr_uv                       string  COMMENT 'i_轮播曝光UV',
hp_multi_entrance_impr_uv            string  COMMENT 'i_金刚位曝光UV',
hp_activity_entrance_impr_uv         string  COMMENT 'i_腰通曝光UV',
hpmulti_gentrance_impr_uv            string  COMMENT 'i_多促销活动曝光UV',
text_content_popup_impr_uv           string  COMMENT 'i_图文弹窗曝光UV',
pic_content_popup_impr_uv            string  COMMENT 'i_纯图弹窗曝光UV',
searchtab_click_uv                   string  COMMENT 'i_首页搜索框点击UV',
top_navigation_click_uv              string  COMMENT 'i_顶部分类点击UV',
banner_click_uv                      string  COMMENT 'i_轮播点击UV',
hp_multi_entrance_click_uv           string  COMMENT 'i_金刚位点击UV',
hp_activity_entrance_click_uv        string  COMMENT 'i_腰通点击UV',
hpmulti_gentrance_click_uv           string  COMMENT 'i_多促销活动点点击UV',
positive_button_click_uv             string  COMMENT 'i_图文弹窗点击UV',
pic_content_popup_but_click_uv       string  COMMENT 'i_纯图弹窗点击UV',
hp_coupon_shop_now_click_uv          string  COMMENT 'i_无门槛优惠券弹窗点击UV',
new_user_7day_impr_uv                string  COMMENT 'i_新手专区商品曝光UV',
flash_sale_hp_entrance_impr_uv       string  COMMENT 'i_闪购商品曝光UV',
recently_viewed_hp_entrance_impr_uv  string  COMMENT 'i_近期浏览商品曝光UV',
new_user_7day_click_uv               string  COMMENT 'i_新手专区商品点击UV',
flash_sale_hp_entrance_click_uv      string  COMMENT 'i_闪购商品点击UV',
recently_viewed_hp_entrance_click_uv string  COMMENT 'i_近期浏览商品点击UV',
homepage_coupon_popup_uv             string  COMMENT 'i_无门槛优惠券弹窗曝光UV',
homepage_uv                          string  COMMENT 'i_首页dau'
) COMMENT '首页流量分配UV' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_homepage_flow_distribution/"
;
