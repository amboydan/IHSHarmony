options(scipen = 999)
library(RODBC); library(tidyverse); 
setwd("O:/Alaska/Depts/Kenai/Gas Storage & Forecast/Production Forecast/Harmony Production")

# Construct the query
wellInformation <- function() {
  q <- "SELECT [WELL_KEY] as ENERTIA_CODE,
        LEFT(REPLACE([WELLHEAD_UWI], '-', ''), 12) AS WELL_KEY,
        [STATE],
        [COUNTY_PARRISH],
        [OPERATOR],
        [AREA_NAME],
        [FIELD_NAME],
        [WELL_NAME],
        [WELLHEAD_LATITUDE],
        [WELLHEAD_LONGITUDE],
        [SANDFACE_LATITUDE],
        [SANDFACE_LONGITUDE]
      FROM [HarmonyDaily].[dbo].[WELL_INFORMATION] 
      where area_name in ('Cook Inlet Offshore - CIO', 'Kenai - KEN')
      order by AREA_NAME, FIELD_NAME"
  
  # Open up the db.
  str <- 'driver={SQL Server};server=enertia03;database=HarmonyDaily;trusted_connection=true'
  
  # query
  cn <- RODBC::odbcDriverConnect(str)
    wi <- RODBC::sqlQuery(cn, q)
  RODBC::odbcClose(cn)
  
  # Working on a conversion file that has sub layers the team would like 
  # to incorporate.  Maybe in the future can incorporate other attributes
  # from Petra, Petrel, etc.
  # 2023-01-16 DRT: added column 'PrimaryFluid' to csv without testing. ???
  
  other_attr <- read.csv('harmony_conversion_import.csv', header = T)
  wi <- left_join(wi, other_attr, by = 'ENERTIA_CODE')
  
  # cut the completion code (3 chars) off of the wi (well info) WELL_KEY 
  # as well. 
  wi$ENERTIA_CODE_short <- substr(wi$ENERTIA_CODE, 1, 12)
  wi$select <- with( wi, as.numeric(substr(ENERTIA_CODE, 13,15))*100)
  
  # 99 is default for pre-hak completions. make it zero where found.
  wi$select <- with(wi, ifelse(select > 50, 0, select))
  
  # get the name of the last completion created
  wi$ENERTIA_CODE <- wi$ENERTIA_CODE_short
  wi <- subset(wi, select = -ENERTIA_CODE_short)
  
  wi <- wi %>% group_by(ENERTIA_CODE) %>% slice_max(select, n = 1)
  wi <- wi[!duplicated(wi$ENERTIA_CODE), ]
  wi <- as.data.frame(
    subset(wi, select = -select)
    )
  
  # Save to Gas_Forecasting_Sandbox
  str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
  
  cn <- RODBC::odbcDriverConnect(str)
  RODBC::sqlClear(cn, sqtable = 'WELL_INFORMATION')
  RODBC::sqlSave(cn,
                 wi,
                 rownames = F,
                 append = T,
                 tablename = 'WELL_INFORMATION')
  RODBC::odbcClose(cn)
  
  return(wi)
}

wi <- wellInformation()

updateProduction <- function(){
  # Clear the production history table before adding
  str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
  
  # cn <- RODBC::odbcDriverConnect(str)
  #   RODBC::sqlClear(cn, sqtable = 'PRODUCTION_HISTORY')
  # RODBC::odbcClose(cn)
  
  # Construct the query for production data. There are 3.5 million rows of 
  # production data if we load all at once.  Will read and load in batches.
  
  for(i in 1:11){#nrow(wi)) {

    print(paste0(i, '  ', wi$WELL[i]))
    q_i <- paste0("SELECT max(cast([DATE_TIME] as date)) ",
                  "FROM PRODUCTION_HISTORY ",
                  "where WELL_KEY = ", wi$WELL_KEY[i], " ")
    
    str <- paste0('driver={SQL Server};',
                  'server=ancsql04;',
                  'database=Gas_Forecasting_Sandbox;',
                  'trusted_connection=true')
    
    cn <- RODBC::odbcDriverConnect(str)
      mxDate <- RODBC::sqlQuery(cn, q_i)
    RODBC::odbcClose(cn)
    
    if(is.na(mxDate)){
      mxDate <- as.Date('1900-01-01')
    } else if(is.character(mxDate)) {
      mxDate <- as.Date('1900-01-01')
    } else {
      mxDate <- as.Date(mxDate[1,1], "%Y-%m-%d")  
    }
    
    if(mxDate+2 != Sys.Date()) {

      q <- paste0("SELECT [DATE_TIME] as DATE_TIME, ", 
          "cast('",wi$WELL_KEY[i],"' as char) as WELL_KEY, '",
          wi$ENERTIA_CODE[i],"' as ENERTIA_CODE, ",
          "sum([GAS_PRODUCTION_VOLUME]) as [GAS_PRODUCTION_VOLUME],
          sum([OIL_PRODUCTION_VOLUME]) as [OIL_PRODUCTION_VOLUME],
          sum([WATER_PRODUCTION_VOLUME]) as [WATER_PRODUCTION_VOLUME],
          sum([WATER_INJECTION_VOLUME]) as [WATER_INJECTION_VOLUME],
          sum([GAS_INJECTION_VOLUME]) as [GAS_INJECTION_VOLUME],
    	    sum([GAS_LIFT_INJECTION_VOLUME]) as [GAS_LIFT_INJECTION_VOLUME],
          max([TUBING_PRESSURE]) as [TUBING_PRESSURE],
          max([CASING_PRESSURE]) as [CASING_PRESSURE]
          
          FROM [HarmonyDaily].[dbo].[PRODUCTION_HISTORY] 
          where [WELL_KEY] like '", wi$ENERTIA_CODE[i], "%' ",
          "and cast([DATE_TIME] as date) < (select cast(GETDATE()-1 as Date)) ",
          "and [DATE_TIME] > '", mxDate,"' ",
    	    "group by DATE_TIME order by DATE_TIME")
      
      str <- paste0('driver={SQL Server};',
                    'server=enertia03;',
                    'database=HarmonyDaily;',
                    'trusted_connection=true')
      
      # query 
      cn <- RODBC::odbcDriverConnect(str)
        prd <- RODBC::sqlQuery(cn, q)
      RODBC::odbcClose(cn)
      
      # is the df empty
      if(dim(prd)[[1]] == 0) next
      
      # Where Csg and Tbg pressures are NA make 0
      prd$CASING_PRESSURE[is.na(prd$CASING_PRESSURE)] <- 0
      prd$TUBING_PRESSURE[is.na(prd$TUBING_PRESSURE)] <- 0
      
      str <- paste0('driver={SQL Server};',
                    'server=ancsql04;',
                    'database=Gas_Forecasting_Sandbox;',
                    'trusted_connection=true')
      
      cn <- RODBC::odbcDriverConnect(str)
      RODBC::sqlSave(cn,
                     prd,
                     rownames = F,
                     append = T,
                     tablename = 'PRODUCTION_HISTORY')
      RODBC::odbcClose(cn)
    }
  }
}
updateProduction()