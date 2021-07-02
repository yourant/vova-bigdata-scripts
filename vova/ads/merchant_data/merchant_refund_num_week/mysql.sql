CREATE TABLE `merchant_refund_ratio_week`  (
                                               `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
                                               `mct_id` int(11) NOT NULL COMMENT '商品所属商家',
                                               `country` char(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '国家code,\"00\"表示全部',
                                               `shipping_type` tinyint(4) NOT NULL COMMENT '物流渠道,[全部，平邮，非平邮，集运] 按序对应0123',
                                               `count_date` date NOT NULL COMMENT '统计日期，每周一日期',
                                               `refund_num` int(11) NOT NULL COMMENT '退款总数',
                                               `item_dont_fit` int(11) NOT NULL DEFAULT 0 COMMENT '不同原因退款数量，下同',
                                               `poor_quality` int(11) NOT NULL,
                                               `item_not_as_described` int(11) NOT NULL,
                                               `defective_item` int(11) NOT NULL,
                                               `shipment_late` int(11) NOT NULL,
                                               `wrong_product` int(11) NOT NULL,
                                               `wrong_quantity` int(11) NOT NULL,
                                               `not_receive` int(11) NOT NULL,
                                               `others` int(11) NOT NULL,
                                               `empty_package` int(11) NOT NULL,
                                               `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
                                               `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
                                               PRIMARY KEY (`id`) USING BTREE,
                                               UNIQUE INDEX `uk`(`mct_id`, `country`, `shipping_type`, `count_date`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '退款比率-按周' ROW_FORMAT = Dynamic;