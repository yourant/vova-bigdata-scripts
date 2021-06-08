SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for comments_favourable
-- ----------------------------
DROP TABLE IF EXISTS `comments_favourable`;
CREATE TABLE `comments_favourable`  (
                                  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
                                  `goods_id` bigint(20) NOT NULL COMMENT '商品ID',
                                  `buyer_id` bigint(20) NOT NULL COMMENT 'BUYER ID',
                                  `comment_id` bigint(20) NOT NULL COMMENT '评价ID',
                                  `mct_id` int(11) NOT NULL COMMENT '商品所属商家',
                                  `first_cat_id` int(11) NOT NULL COMMENT '商品一级类目id，0表示全部',
                                  `rank` int(11) NOT NULL COMMENT '排名',
                                  `order_type` int(1) NOT NULL COMMENT '0：销量排序 1:CTR排序',
                                  `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
                                  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
                                  PRIMARY KEY (`id`) USING BTREE,
                                  UNIQUE INDEX `uk_cmt_fav`(`goods_id`,`buyer_id`, `comment_id`, `mct_id`,`first_cat_id`,`order_type`) USING BTREE COMMENT '一个排序方法下同一个商家的同一个品类的同一个产品的评价唯一'
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '好评数据' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;