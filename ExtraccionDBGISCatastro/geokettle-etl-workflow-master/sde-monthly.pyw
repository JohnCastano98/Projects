##############################################################
##
##  Weekly postgres ETL jobs - SDE data
##
##############################################################

import os

geokettlepath = "C:\\Users\\SoporteTI\\Documents\\geokette"
scriptpath = "C:\\etl\\"

# Change to geokettle directory
os.chdir(geokettlepath)

# roads
os.system("kitchen.bat /file:" + scriptpath + "sde-monthly.kjb /level:Basic /logfile:" + scriptpath + "sde-monthly.kjb.log")

