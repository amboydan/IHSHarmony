options(scipen = 999)
library(RODBC); library(tidyverse); 

# Construct the query
importDeclines <- function() {
  q <- "SELECT left(prop.ENERTIA_CODE, 12) as WELL_KEY,
              [SECTION],
              [SEQUENCE],
              [QUALIFIER],
              [KEYWORD],
              [EXPRESSION]
        FROM Ariesadmin.AC_PROPERTY as prop
        inner join
        ARIES_DECLINE_ANALYSES as ada
        on prop.propnum = ada.WELL_KEY
        where prop.AREA in ('KEN', 'CIO') and
        ada.QUALIFIER in ('PLAN23','NS0722', 'NS0122', 
        'NS0721', 'NS0121', 'NS0720', 'NS0120', 'NS0719',
        'NS0119','NS0718', 'NS0118') and
        prop.RSV_CAT = '1PDP'"
  
  # Open up the db.
  str <- 'driver={SQL Server};server=extsql01;database=Aries;trusted_connection=true'
  
  # query
  cn <- RODBC::odbcDriverConnect(str)
    dca <- RODBC::sqlQuery(cn, q)
  RODBC::odbcClose(cn)
  
  return(dca)
}
dca <- importDeclines()

exportDeclines <- function(dca) {
  # Save to Gas_Forecasting_Sandbox
  str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
  
  cn <- RODBC::odbcDriverConnect(str)
  RODBC::sqlDrop(cn, sqtable = 'ARIES_DECLINE_ANALYSES')
  RODBC::sqlSave(cn,
                 dca,
                 rownames = F,
                 append = F,
                 tablename = 'ARIES_DECLINE_ANALYSES')
  RODBC::odbcClose(cn)
}
exportDeclines(dca)