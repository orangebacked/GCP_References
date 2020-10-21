#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 13:34:19 2020

@author: orangebacked
"""

import xmltodict
import glob
import os
import shutil

totalcount = 0
badcpunt = 0 
nwe_path = "/home/orangebacked/Downloads/newss_xmls/"
for path_file in glob.glob("/home/orangebacked/Downloads/xmlss/*.xml"):
    totalcount +=1
    with open(path_file) as xxml:
        name = os.path.basename(path_file)
        try:
            a = xxml.read()
            doc = str(a)
            doc = doc.replace(":","")
            doc = xmltodict.parse(doc)
            
        except:
            newest_path_file = nwe_path + name
            shutil.move(path_file, newest_path_file)
            badcpunt += 1 
            #print("not", " total count:", totalcount, "bad count:", badcpunt, path_file)
            
            q