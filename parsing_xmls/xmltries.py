#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 16:00:37 2019

@author: orangebacked
"""

import pandas as pd
import numpy as np
import xmltodict
import re


with open ("mistake1.xml", "r") as f:
    doc = f.read()

doc = str(a)[2:-1]
doc = re.sub(r'''xmlns:schemaLocation=(.*?)Module:2''','',doc).replace(':','')
doc = xmltodict.parse(doc)
a = dict(doc)

str(a)
