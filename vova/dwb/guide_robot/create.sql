drop table dwb.dwb_vova_shopping_guide_robot;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_shopping_guide_robot
(
    cur_date        string  COMMENT 'd_日期',
    list            string  COMMENT 'd_list',
    expre_pv        bigint  COMMENT 'i_expre_pv',
    expre_uv        bigint  COMMENT 'i_expre_uv',
    clk_pv          bigint  COMMENT 'i_clk_pv',
    clk_uv          bigint  COMMENT 'i_clk_uv',
    cart_uv         bigint  COMMENT 'i_cart_uv',
    pay_uv          bigint  COMMENT 'i_pay_uv',
    gmv          double  COMMENT 'i_gmv',
    enter_expre_pv  bigint  COMMENT 'i_enter_expre_pv',
    enter_expre_uv  bigint  COMMENT 'i_enter_expre_uv',
    enter_clk_pv    bigint  COMMENT 'i_enter_clk_pv',
    enter_clk_uv    bigint  COMMENT 'i_enter_clk_uv',
    button_expre_uv bigint  COMMENT 'i_button_expre_uv',
    button_clk_uv   bigint  COMMENT 'i_button_clk_uv',
    session_expre_uv   bigint  COMMENT 'i_进入会话人数'
) COMMENT 'dwb_vova_shopping_guide_robot' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
