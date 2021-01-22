drop table if exists dwb.dwb_vova_take_photo_for_buy;
create external table if  not exists dwb.dwb_vova_take_photo_for_buy (
    `platform`                      string        COMMENT  '平台',
    `app_version`                   string        COMMENT 'app版本',
    `in_button_exp_uv`              int           COMMENT '拍照购入口按钮曝光uv',
    `zip_photo_exp_cnt`             int           COMMENT '识别缩略图曝光量（position为1的缩略图曝光次数）',
    `sv_page_uv`                    int           COMMENT '拍照购页面uv',
    `sv_page_result_pv`             int           COMMENT '拍照购结果页pv',
    `button_clk_uv`                 int           COMMENT '拍照按钮点击uv',
    `in_button_clk_uv`              int           COMMENT '拍照购入口按钮点击uv',
    `img_clk_uv`                    int           COMMENT '相册按钮点击uv',
    `zip_img_clk_uv`                int           COMMENT '相册缩略图点击uv',
    `goods_exp_uv`                  int           COMMENT '商品曝光uv',
    `goods_exp_result_dis_cnt`      int           COMMENT '结果页商品曝光次数（position为1的缩略图曝光次数）',
    `goods_exp_result_cnt`          int           COMMENT '商品曝光量',
    `goods_clk_cnt`                 int           COMMENT '商品点击量',
    `goods_clk_uv`                  int           COMMENT '商品点击uv',
    `add_cat_uv`                    int           COMMENT '加车uv',
    `order_uv`                      int           COMMENT '下单uv',
    `gmv`                           decimal(13,2)           COMMENT 'gmv'
)COMMENT '拍照购报表' PARTITIONED BY (pt string) STORED AS PARQUETFILE;