source('harmony_kenai_well_information.R')

wi <- well_info()

str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'

cn <- RODBC::odbcDriverConnect(str)
  RODBC::sqlDrop(cn, sqtable = 'PRODUCTION_HISTORY')
RODBC::odbcClose(cn)

updateProduction <- function(wi){

  
  for(i in 1:nrow(wi)) {
    
    print(paste0(round(i/nrow(wi),3)*100, '%  ', wi$WELL_NAME[i]))
    
    q <- paste0("SELECT '", wi$WELL_KEY[i], "' as WELL_KEY, 
          cast([DATE_TIME] as date) as DATE_TIME, 
          sum([GAS_PRODUCTION_VOLUME]) as [GAS_PRODUCTION_VOLUME],
          sum([OIL_PRODUCTION_VOLUME]) as [OIL_PRODUCTION_VOLUME],
          sum([WATER_PRODUCTION_VOLUME]) as [WATER_PRODUCTION_VOLUME],
          sum([WATER_INJECTION_VOLUME]) as [WATER_INJECTION_VOLUME],
          sum([GAS_INJECTION_VOLUME]) as [GAS_INJECTION_VOLUME],
    	    sum([GAS_LIFT_INJECTION_VOLUME]) as [GAS_LIFT_INJECTION_VOLUME],
          max([TUBING_PRESSURE]) as [TUBING_PRESSURE],
          max([CASING_PRESSURE]) as [CASING_PRESSURE]

          FROM [HarmonyDaily].[dbo].[PRODUCTION_HISTORY]
          where [WELL_KEY] like '", wi$WELL_KEY[i], "%' ",
          "and cast([DATE_TIME] as date) < (select cast(GETDATE()-1 as Date)) ",
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
                     varTypes = c(DATE_TIME = 'datetime',
                                  SPUD_DATE = 'datetime'),
                     tablename = 'PRODUCTION_HISTORY')
      RODBC::odbcClose(cn)
    }
}
updateProduction(wi)