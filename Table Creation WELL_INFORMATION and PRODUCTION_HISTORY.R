options(scipen = 999)
library(RODBC); library(tidyverse); 

# Construct the query
q <- "SELECT [WELL_KEY]
      ,[WELLHEAD_UWI]
      ,[API_FM]
      ,[AREA_NAME]
      ,[FIELD_NAME]
      ,[WELL_NAME]
      ,[WELLHEAD_LATITUDE]
      ,[WELLHEAD_LONGITUDE]
      ,[SANDFACE_LATITUDE]
      ,[SANDFACE_LONGITUDE]
      ,[RESERVOIR_NAME]
      ,[OPERATOR]
      ,[TOP PERF (MD)]
      ,[BOTTOM PERF (MD)]
      ,[State]
      ,[County_Parrish]
  FROM [HarmonyDaily].[dbo].[WELL_INFORMATION] 
  where area_name like '%Kenai%'"

# Open up the db.
str <- 'driver={SQL Server};server=enertia03;database=HarmonyDaily;trusted_connection=true'

# query
cn <- RODBC::odbcDriverConnect(str)
  wi <- RODBC::sqlQuery(cn, q)
RODBC::odbcClose(cn)

# for now only work with the first 5 wells
wi <- head(wi, 200)

# construct well key
keys <- paste("'", wi$WELL_KEY,"'", collapse = ',', sep = '')

# cut the completion code (3 chars) off of the wi (well info) WELL_KEY 
# as well. 
wi$WELL_KEY_short <- substr(wi$WELL_KEY, 1, 12)
wi$select <- with( wi, as.numeric(substr(WELL_KEY, 13,15))*100)

# 99 is default for pre-hak completions. make it zero where found.
wi$select <- with(wi, ifelse(select > 50, 0, select))

# get the name of the last completion created
wi$WELL_KEY <- wi$WELL_KEY_short
wi <- subset(wi, select = -WELL_KEY_short)

wi <- wi %>% group_by(WELL_KEY) %>% slice_max(select, n = 1)
wi <- wi[!duplicated(wi$WELL_KEY), ]

# Construct the query
q <- paste0("SELECT [WELL_KEY]
      ,[DATE_TIME]
      ,[GAS_PRODUCTION_VOLUME]
      ,[OIL_PRODUCTION_VOLUME]
      ,[WATER_PRODUCTION_VOLUME]
      ,[WATER_INJECTION_VOLUME]
      ,[GAS_INJECTION_VOLUME]
      ,[TUBING_PRESSURE]
      ,[CASING_PRESSURE]
      ,[GAS_LIFT_INJECTION_VOLUME]
  FROM [HarmonyDaily].[dbo].[PRODUCTION_HISTORY] ",
            "where [WELL_KEY] IN (", keys, ")")

# query
cn <- RODBC::odbcDriverConnect(str)
  prd <- RODBC::sqlQuery(cn, q)
RODBC::odbcClose(cn)

# last 3 characters in WELL_KEY identify completion code.
# rmv completion code for parent well.  agg all production up to the 
# parent level
prd$WELL_KEY <- substr(prd$WELL_KEY, 1, 12)
prd$DATE_TIME <- as.Date(prd$DATE_TIME)

# prd_1 <- prd %>% group_by(Parent, DATE_TIME) %>% summarise(
#   GAS_PRODUCTION_VOLUME = sum(GAS_PRODUCTION_VOLUME, na.rm = T),
#   OIL_PRODUCTION_VOLUME = sum(OIL_PRODUCTION_VOLUME, na.rm = T),
#   WATER_PRODUCTION_VOLUME = sum(WATER_PRODUCTION_VOLUME, na.rm = T),
#   WATER_INJECTION_VOLUME = sum(WATER_INJECTION_VOLUME, na.rm = T),
#   GAS_INJECTION_VOLUME = sum(GAS_INJECTION_VOLUME, na.rm = T),
#   TUBING_PRESSURE = max(TUBING_PRESSURE, na.rm = T),
#   CASING_PRESSURE = max(CASING_PRESSURE, na.rm = T),
#   GAS_LIFT_INJECTION_VOLUME = sum(GAS_LIFT_INJECTION_VOLUME, na.rm = T)
# )

# update Harmony db in Gas_Forecasting_Sandbox

str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'

cn <- RODBC::odbcDriverConnect(str)
  RODBC::sqlDrop(cn, sqtable = 'WELL_INFORMATION')
  RODBC::sqlSave(cn,
           wi,
           rownames = F,
           append = F,
           tablename = 'WELL_INFORMATION')
RODBC::odbcClose(cn)

cn <- RODBC::odbcDriverConnect(str)
RODBC::sqlClear(cn, sqtable = 'PRODUCTION_HISTORY')
RODBC::sqlSave(cn,
               prd,
               rownames = F,
               append = T,
               tablename = 'PRODUCTION_HISTORY')
RODBC::odbcClose(cn)
