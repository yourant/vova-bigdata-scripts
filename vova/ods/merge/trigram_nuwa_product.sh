#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
drop table if exists ods.trigram_nuwa_product;
create table if not exists ods.trigram_nuwa_product as
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_0
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_1
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_2
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_3
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_4
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_5
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_6
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_7
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_8
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_9
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_10
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_11
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_12
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_13
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_14
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_15
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_16
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_17
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_18
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_19
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_20
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_21
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_22
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_23
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_24
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_25
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_26
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_27
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_28
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_29
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_30
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_31
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_32
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_33
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_34
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_35
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_36
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_37
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_38
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_39
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_40
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_41
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_42
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_43
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_44
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_45
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_46
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_47
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_48
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_49
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_50
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_51
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_52
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_53
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_54
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_55
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_56
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_57
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_58
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_59
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_60
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_61
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_62
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_63
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_64
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_65
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_66
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_67
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_68
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_69
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_70
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_71
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_72
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_73
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_74
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_75
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_76
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_77
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_78
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_79
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_80
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_81
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_82
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_83
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_84
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_85
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_86
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_87
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_88
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_89
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_90
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_91
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_92
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_93
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_94
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_95
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_96
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_97
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_98
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_99
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_100
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_101
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_102
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_103
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_104
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_105
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_106
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_107
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_108
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_109
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_110
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_111
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_112
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_113
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_114
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_115
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_116
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_117
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_118
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_119
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_120
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_121
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_122
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_123
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_124
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_125
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_126
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_127
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_128
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_129
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_130
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_131
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_132
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_133
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_134
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_135
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_136
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_137
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_138
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_139
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_140
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_141
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_142
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_143
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_144
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_145
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_146
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_147
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_148
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_149
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_150
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_151
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_152
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_153
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_154
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_155
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_156
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_157
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_158
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_159
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_160
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_161
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_162
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_163
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_164
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_165
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_166
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_167
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_168
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_169
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_170
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_171
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_172
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_173
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_174
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_175
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_176
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_177
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_178
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_179
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_180
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_181
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_182
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_183
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_184
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_185
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_186
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_187
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_188
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_189
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_190
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_191
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_192
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_193
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_194
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_195
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_196
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_197
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_198
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_199
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_200
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_201
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_202
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_203
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_204
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_205
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_206
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_207
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_208
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_209
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_210
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_211
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_212
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_213
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_214
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_215
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_216
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_217
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_218
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_219
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_220
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_221
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_222
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_223
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_224
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_225
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_226
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_227
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_228
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_229
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_230
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_231
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_232
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_233
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_234
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_235
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_236
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_237
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_238
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_239
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_240
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_241
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_242
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_243
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_244
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_245
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_246
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_247
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_248
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_249
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_250
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_251
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_252
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_253
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_254
union
select product_id, commodity_id, cat_id
from ods.trigram_nuwa_product_255
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.dynamicAllocation.maxExecutors=100"   --conf "spark.dynamicAllocation.minExecutors=20"  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=trigram_nuwa_product" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
