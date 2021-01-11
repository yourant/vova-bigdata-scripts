#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
insert overwrite table  ods_gyl_gnw.ods_gyl_product
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_0
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_1
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_2
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_3
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_4
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_5
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_6
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_7
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_8
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_9
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_10
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_11
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_12
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_13
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_14
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_15
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_16
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_17
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_18
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_19
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_20
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_21
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_22
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_23
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_24
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_25
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_26
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_27
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_28
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_29
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_30
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_31
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_32
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_33
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_34
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_35
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_36
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_37
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_38
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_39
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_40
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_41
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_42
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_43
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_44
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_45
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_46
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_47
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_48
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_49
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_50
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_51
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_52
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_53
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_54
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_55
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_56
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_57
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_58
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_59
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_60
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_61
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_62
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_63
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_64
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_65
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_66
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_67
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_68
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_69
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_70
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_71
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_72
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_73
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_74
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_75
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_76
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_77
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_78
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_79
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_80
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_81
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_82
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_83
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_84
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_85
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_86
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_87
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_88
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_89
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_90
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_91
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_92
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_93
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_94
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_95
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_96
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_97
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_98
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_99
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_100
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_101
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_102
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_103
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_104
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_105
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_106
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_107
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_108
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_109
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_110
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_111
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_112
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_113
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_114
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_115
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_116
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_117
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_118
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_119
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_120
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_121
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_122
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_123
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_124
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_125
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_126
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_127
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_128
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_129
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_130
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_131
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_132
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_133
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_134
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_135
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_136
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_137
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_138
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_139
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_140
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_141
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_142
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_143
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_144
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_145
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_146
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_147
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_148
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_149
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_150
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_151
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_152
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_153
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_154
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_155
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_156
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_157
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_158
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_159
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_160
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_161
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_162
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_163
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_164
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_165
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_166
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_167
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_168
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_169
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_170
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_171
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_172
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_173
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_174
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_175
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_176
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_177
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_178
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_179
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_180
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_181
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_182
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_183
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_184
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_185
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_186
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_187
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_188
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_189
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_190
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_191
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_192
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_193
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_194
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_195
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_196
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_197
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_198
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_199
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_200
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_201
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_202
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_203
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_204
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_205
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_206
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_207
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_208
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_209
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_210
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_211
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_212
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_213
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_214
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_215
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_216
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_217
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_218
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_219
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_220
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_221
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_222
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_223
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_224
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_225
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_226
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_227
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_228
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_229
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_230
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_231
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_232
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_233
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_234
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_235
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_236
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_237
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_238
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_239
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_240
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_241
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_242
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_243
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_244
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_245
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_246
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_247
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_248
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_249
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_250
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_251
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_252
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_253
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_254
union all
select product_id, commodity_id, cat_id
from ods_gyl_gnw.ods_gyl_product_255
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.dynamicAllocation.maxExecutors=100"   --conf "spark.dynamicAllocation.minExecutors=20"  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=trigram_nuwa_product" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
