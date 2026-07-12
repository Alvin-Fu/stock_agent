# -*- coding: utf-8 -*-
"""pytest 公共配置：把项目根目录加进 sys.path，任何位置执行 pytest 都能 import"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
