library(dplyr)
library(datapkg)
library(acs)
library(stringr)
library(reshape2)
library(data.table)
library(tidyr)
source('./scripts/acsHelpers.R')

##################################################################
#
# Processing Script for Total Households by Town
# Created by Jenna Daly
# On 11/27/2017
#
##################################################################


#Get state data
geography=geo.make(state=09)
yearlist=c(2010:2017)
span = 5
col.names="pretty" 
key="ed0e58d2538fb239f51e01643745e83f380582d7"
options(scipen=999)

state_data <- data.table()
for (i in seq_along(yearlist)) {
  endyear = yearlist[i]
  data <- acs.fetch(geography=geography, endyear=endyear, span=span, 
                         table.number="B10063", col.names=col.names, key=key)
  Sys.sleep(3)
  year <- data@endyear
  print(paste("Processing: ", year))
  year <- paste(year-4, year, sep="-")
  geo <- data@geography
  geo$NAME <- NULL
  households <- acsSum(data, 1, "Total Households")
  numbers <- data.table(
             geo,
             estimate(households),
             year,
             `Measure Type` = rep_len("Number", nrow(data)),
             Variable = rep_len("Total Households", nrow(data)))
  numbers.moe <- data.table(
                 geo, 
                 standard.error(households) * 1.645,
                 year,
                 `Measure Type` = rep_len("Number", nrow(data)),
                 Variable = rep_len("Margins of Error", nrow(data)))
  names <- c("FIPS",            
              "Value", 
              "Year", 
              "Measure Type", 
              "Variable")
  setnames(numbers, names)
  setnames(numbers.moe, names)
  
  state_data <- rbind(state_data, numbers, numbers.moe)
}

#Get town data
geography=geo.make(state=09, county="*", county.subdivision = "*")

town_data <- data.table()
for (i in seq_along(yearlist)) {
  endyear = yearlist[i]
  data <- acs.fetch(geography=geography, endyear=endyear, span=span, 
                         table.number="B10063", col.names=col.names, key=key)
  year <- data@endyear
  print(paste("Processing: ", year))
  year <- paste(year-4, year, sep="-")
  geo <- data@geography
  geo$county <- sprintf("%02d", geo$county)
  geo$county <- gsub("^", "090", geo$county)
  geo$FIPS <- paste0(geo$county, geo$countysubdivision)
  geo$state <- NULL
  geo$NAME <- NULL
  geo$countysubdivision <- NULL
  geo$county <- NULL  
  households <- acsSum(data, 1, "Total Households")
  numbers <- data.table(
             geo,
             estimate(households),
             year,
             `Measure Type` = rep_len("Number", nrow(data)),
             Variable = rep_len("Total Households", nrow(data)))
  numbers.moe <- data.table(
                 geo, 
                 standard.error(households) * 1.645,
                 year,
                 `Measure Type` = rep_len("Number", nrow(data)),
                 Variable = rep_len("Margins of Error", nrow(data)))
  names <- c("FIPS",            
             "Value", 
             "Year", 
             "Measure Type", 
             "Variable")
  setnames(numbers, names)
  setnames(numbers.moe, names)
  town_data <- rbind(town_data, numbers, numbers.moe)
}

households <- rbind(state_data, town_data)

#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
towns <- (town_fips_dp$data[[1]])

households <- merge(households, towns, by = "FIPS", all.y=T)

households <- households %>% 
  select(Town, FIPS, Year, `Measure Type`, Variable, Value) %>%
  arrange(Town, FIPS, Year, `Measure Type`, desc(Variable))

write.table (
  households,
  file.path(getwd(), "data", "total-households-town-2017.csv"),
  sep = ",",
  row.names = F,
  na = "-6666"
)




