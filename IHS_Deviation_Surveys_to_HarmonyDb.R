library(httr); library(RCurl); library(RODBC)

usn <- '3a86fd97-1ed2-4c5d-9801-c2f76a746e41'
pwd <- 'pRzYdqG3VhCNT3yn'

# Open up the db.
str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'

q <- "SELECT distinct [WELL_KEY], [WELL_NAME], [API_FM], [SPUD_DATE]
      FROM [Gas_Forecasting_Sandbox].[dbo].[WELL_INFORMATION] 
      where area_name in ('Cook Inlet Offshore - CIO', 'Kenai - KEN')"
      
# query
cn <- RODBC::odbcDriverConnect(str)
  api_all <- RODBC::sqlQuery(cn, q)
RODBC::odbcClose(cn)

count_empty = 0
count_total = 0
for(i in 1:nrow(api_all)) {
  api <- paste0(api_all[i,'API_FM'],'00')
  
  get_survey <- function(api, usn, pwd) {
    url <- paste0(
      'https://energydataservices.ihsenergy.com',
      '/rest/data/v3//northamerica/well/retrieve/',
      'well_directional_survey_station?',
      '$filter=uwi=', api, '&',
      '$select=station_md_uscust,station_tvd_uscust'
    )
    
    usn_pwd_64 <- paste0('Basic ', base64(paste0(usn,':',pwd)[[1]]))
    get_response <- GET(url, 
                        add_headers(
                          Authorization = usn_pwd_64))
    
    cntnt <- content(get_response)
    if(is.null(cntnt$elements)) {
      dt <- 'elements do not exist'
    } else {
      ele <- content(get_response)$elements
      ele_count <- length(ele)
      if(ele_count == 0) {
        dt <- 'no data'
      } else {
        dt <- data.frame('DEVIATION_MD' = NA, 'DEVIATION_TVD' = NA)
        for(i in 1:ele_count) {
          dt[i,1] <- ele[[i]][1]
          dt[i,2] <- ele[[i]][2]
        }
        dt <- dt[order(dt[[1]]), ]
      }
    }
    
    return(dt)
  }
  df <- get_survey(api, usn, pwd)
  is_df <- is.data.frame(df)
  if(is_df == 'TRUE') {
    str <- 'driver={SQL Server};server=ancsql04;database=Gas_Forecasting_Sandbox;trusted_connection=true'
    
    spud_date <- api_all[i, 'SPUD_DATE']
    if(is.na(spud_date)) spud_date <- Sys.Date()
    
    q_row <- paste0("INSERT INTO CWB_CONFIG (WELL_KEY, WB_CONFIG_ID, DATE_TIME, CONFIG_NAME) ",
                    "VALUES ('", api_all[i, 'WELL_KEY'], "', 1, '", spud_date,
                    "', 'Initial'")
    cn <- RODBC::odbcDriverConnect(str)
      RODBC::sqlQuery(cn, q_row)
    RODBC::odbcClose(cn)
    
    for(row in 1:nrow(df)) {
      thisRow = df[row, ]
      
      q_row <- paste0('insert into CWB_CONFIG_DETAILS ',
                      '(WELL_KEY, DATA_GROUP_TYPE, DEVIATION_MD, DEVIATION_TVD, ',
                      'SECTION_TYPE, WB_CONFIG_ID) VALUES ',
                      "('", api_all[i, 'WELL_KEY'], "', 4, ", thisRow$DEVIATION_MD,
                      ", ", thisRow$DEVIATION_TVD, ", 1, 1)")
      cn <- RODBC::odbcDriverConnect(str)
        RODBC::sqlQuery(cn, q_row)
      RODBC::odbcClose(cn)
      
    }
  } else {
    count_empty = count_empty + 1
  }
count_total = count_total + 1
print(paste("Well: ", api_all[i, 'WELL_NAME'],", Total: ", count_total, ", Empty: ", count_empty, 
            ", Ratio: ", round(count_empty/count_total, 2),
            ", Total: ", nrow(api_all)))
}
  


