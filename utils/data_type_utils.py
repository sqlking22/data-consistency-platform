#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:14
# @Author  : hejun

import pandas as pd
import numpy as np
from typing import Tuple


def unify_data_types(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """统一两个DataFrame的数据类型"""
    # 确保列名一致
    common_cols = set(df1.columns) & set(df2.columns)
    df1 = df1[list(common_cols)]
    df2 = df2[list(common_cols)]

    # 逐列统一类型
    for col in common_cols:
        # 获取两端的非空值类型
        dtype1 = df1[col].dropna().dtype
        dtype2 = df2[col].dropna().dtype

        # 优先转换为更通用的类型
        if np.issubdtype(dtype1, np.number) and np.issubdtype(dtype2, np.number):
            target_type = np.float64 if np.issubdtype(dtype1, np.float64) or np.issubdtype(dtype2,
                                                                                           np.float64) else np.int64
            df1[col] = df1[col].astype(target_type)
            df2[col] = df2[col].astype(target_type)
        elif np.issubdtype(dtype1, np.datetime64) or np.issubdtype(dtype2, np.datetime64):
            df1[col] = pd.to_datetime(df1[col])
            df2[col] = pd.to_datetime(df2[col])
        else:
            # 统一为字符串类型
            df1[col] = df1[col].astype(str)
            df2[col] = df2[col].astype(str)

    return df1, df2