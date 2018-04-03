# -*- encoding: utf-8 -*-
# pyspark_xgboost v0.1.0
# pyspark xgboost
# Copyright © 2018, Julian Qian.
# See /LICENSE for licensing information.
# This file was adapted from Chris Warrick’s Python Project Template.

"""
Adapt Qt resources to Python version.

:Copyright: © 2018, Julian Qian.
:License: BSD (see /LICENSE).
"""

__all__ = ()

import sys

if sys.version_info[0] == 2:
    import pyspark-xgboost.ui.resources2  # NOQA
elif sys.version_info[0] == 3:
    import pyspark-xgboost.ui.resources3  # NOQA
else:
    print('FATAL: python version does not match `2` nor `3`')
    sys.exit(0)
