DROP TABLE IF EXISTS dwb.dwb_vova_loss_user_loss;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_loss_user_loss
(
cur_date string COMMENT 'd_日期',
region_code string COMMENT 'd_国家',
platform string COMMENT 'd_平台',
is_new_user string COMMENT 'd_是否新用户',
is_activate_user string COMMENT 'd_是否激活用户',
uv_30m string COMMENT 'i_uv_30m',
uv_60m string COMMENT 'i_uv_60m',
uv_2h string COMMENT 'i_uv_2h',
uv_3h string COMMENT 'i_uv_3h',
uv_1d string COMMENT 'i_uv_1d',
uv_3d string COMMENT 'i_uv_3d',
uv_7d string COMMENT 'i_uv_7d',
uv_15d string COMMENT 'i_uv_15d',
uv_30d string COMMENT 'i_uv_30d',
c_30m_uv string COMMENT 'i_c_30m_uv',
c_60m_uv string COMMENT 'i_c_60m_uv',
c_3h_uv string COMMENT 'i_c_3h_uv',
c_6h_uv string COMMENT 'i_c_6h_uv',
c_24h_uv string COMMENT 'i_c_24h_uv',
c_3d_uv string COMMENT 'i_c_3d_uv',
c_7d_uv string COMMENT 'i_c_7d_uv',
c_15d_uv string COMMENT 'i_c_15d_uv',
c_30d_uv string COMMENT 'i_c_30d_uv'
) COMMENT '流失用户' PARTITIONED BY (pt STRING);

DROP TABLE IF EXISTS dwb.dwb_vova_loss_user_collect;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_loss_user_collect
(
cur_date string COMMENT 'd_日期',
region_code string COMMENT 'd_国家',
platform string COMMENT 'd_平台',
is_new_user string COMMENT 'd_是否新用户',
is_activate_user string COMMENT 'd_是否激活用户',
dau string COMMENT 'i_dau',
date_10 string COMMENT 'i_date_10',
date_11 string COMMENT 'i_date_11',
date_12 string COMMENT 'i_date_12',
date_13 string COMMENT 'i_date_13',
date_14 string COMMENT 'i_date_14',
date_15 string COMMENT 'i_date_15',
date_16 string COMMENT 'i_date_16',
date_17 string COMMENT 'i_date_17'
) COMMENT '流失用户' PARTITIONED BY (pt STRING);
