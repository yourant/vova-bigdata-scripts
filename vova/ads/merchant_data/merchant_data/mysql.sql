SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for merchant_data
-- ----------------------------
DROP TABLE IF EXISTS `merchant_data`;
CREATE TABLE `merchant_data`  (
                                  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
                                  `mct_id` int(11) NOT NULL COMMENT '商品所属商家',
                                  `first_cat_id` int(11) NOT NULL COMMENT '商品一级类目id，-1表示全部',
                                  `count_date` date NOT NULL COMMENT '统计日期',
                                  `mct_gvm` decimal(15, 2) NOT NULL COMMENT '销售额',
                                  `goods_order_number` int(11) NOT NULL COMMENT '子订单量',
                                  `mct_gvm_shipped` decimal(15, 2) NOT NULL COMMENT '已发货销售额',
                                  `goods_order_number_shipped` int(11) NOT NULL COMMENT '已发货子订单量',
                                  `price` decimal(15, 2) NOT NULL COMMENT '笔单价',
                                  `goods_sold_rate` decimal(15, 4) NOT NULL COMMENT '商品动销率',
                                  `goods_new_sold_rate` decimal(15, 4) NOT NULL COMMENT '新品动销率',
                                  `add_cart_cnt` int(11) NOT NULL COMMENT '加购商品数',
                                  `cart_rate` decimal(15, 4) NOT NULL COMMENT '加购转化率',
                                  `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
                                  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
                                  PRIMARY KEY (`id`) USING BTREE,
                                  UNIQUE INDEX `uk_mct_fcat_date`(`mct_id`, `first_cat_id`, `count_date`) USING BTREE COMMENT '一个商家在一天内的一个品类统计数据唯一'
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '店铺数据' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;