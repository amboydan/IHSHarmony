library(RODBC); library(tidyverse); library(DescTools)
options(scipen = 999)

setwd('O:/Alaska/Depts/Kenai/OptEng/drt/projects/IHSHarmony')

# Construct the query
well_info <- function() {
  q <- "SELECT [WELL_KEY],
        LEFT(REPLACE([WELLHEAD_UWI], '-', ''), 12) AS API_FM,
        [STATE] as [PROVINCE_STATE],
        [COUNTY_PARRISH],
        [AREA_NAME],
        [FIELD_NAME],
        [WELL_NAME]
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
  
  other_attr <- read.csv('O:/Alaska/Depts/Kenai/OptEng/drt/projects/IHSHarmony/harmony_conversion_import.csv', header = T)
  wi <- plyr::join(wi, other_attr, type = 'left',  by = 'WELL_KEY')
  
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
  wi <- as.data.frame(
    subset(wi, select = -select))
  
  # Rename 'AREA_NAME' of all Storage Wells
  wi <- wi %>% 
    mutate(AREA_NAME = ifelse(FIELD_NAME %like% '%Storage%',
                              'Kenai Gas Storage', AREA_NAME))
  
  # Get other factors from sde geodatabase.
  q_geo <- "SELECT btm.API as API_FM,
      btm.HISTOPER AS ORIGINAL_OPERATOR,
      btm.SYMCODE as PRIMARY_FLUID,
      btm.SYMCODE as WELL_STATUS,
      btm.FMATTD as POOL_NAME,
      cast(btm.ELEV_KB_ as numeric) as KB_ELEVATION,
      cast(btm.TD_ as numeric) as TOTAL_DEPTH_MD,
      cast(btm.SPUD_DATE_ as date) as SPUD_DATE,
      btm.Shape.STX as SANDFACE_LONGITUDE,
      btm.Shape.STY as SANDFACE_LATITUDE,
      tp.Shape.STX as WELLHEAD_LONGITUDE,
      tp.Shape.STY as WELLHEAD_LATITUDE
      FROM [sdeHAK].[dbo].[AK_PETRAWELLS_BOTTOM_NAD27] as btm
      left join sdeHAK.dbo.AK_PETRAWELLS_SURFACE_NAD27 as tp
      on tp.API = btm.API
      where btm.state like '%ak%'"
  
  # 
  str <- 'driver={SQL Server};server=AW2GISSDEP1;database=sdeHAK;trusted_connection=true'
  
  cn <- RODBC::odbcDriverConnect(str)
    geo_db <- RODBC::sqlQuery(cn, q_geo)
  RODBC::odbcClose(cn)
  geo_db$API_FM <- as.character(geo_db$API_FM)
  
  # join wi to geo_db
  well_info <- left_join(wi, geo_db, by = "API_FM")
  
  # put well_info column names in the correct order
  q_names <- "select * from information_schema.columns
              where table_name = 'WELL_INFORMATION'"
  
  str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
  
  cn <- RODBC::odbcDriverConnect(str)
    tbl_names <- RODBC::sqlQuery(cn, q_names)
  RODBC::odbcClose(cn)
  well_info <- well_info[ , tbl_names$COLUMN_NAME]
  
 # Save to Gas_Forecasting_Sandbox
  str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
  
  cn <- RODBC::odbcDriverConnect(str)
  RODBC::sqlDrop(cn, sqtable = 'WELL_INFORMATION')
  RODBC::sqlSave(cn,
                 well_info,
                 rownames = F,
                 append = T,
                 tablename = 'WELL_INFORMATION')
  RODBC::odbcClose(cn)
  
  return(well_info)
}

write.csv(well_info(), paste0('../IHSHarmony_dump/well_information_', 
                              gsub(':', "-", Sys.time()), 
                              '.csv'), row.names = F)
