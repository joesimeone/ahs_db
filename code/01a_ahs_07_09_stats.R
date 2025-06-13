library(duckdb)
library(tidyverse)
library(glue)


con <- dbConnect(duckdb(), 'duckdb/ahs.db')

dbListTables(con)


# dbGetQuery(con, "SELECT column_name, data_type
#                  FROM information_schema.columns
#                  WHERE table_schema = 'metro' AND table_name = 'test'")

fields <- dbGetQuery(con, "PRAGMA table_info('national.natl_2013')")
natl_smsa <- dbGetQuery(con, 'SELECT DISTINCT SMSA FROM national.natl_2009')


tbls <-
  c('national.natl_2007', 'national.natl_2009')


room_ac_query <-
  "
SELECT 
	SMSA, NUNIT2, BUILT, AIR, 
	COUNT(AIR) as raw_room_ac,
	sum(WGT90GEO) as wgt_room_ac
FROM
	{.x}
WHERE 
	SMSA in ('''6160''', '''6200''', '''5000''')
GROUP BY 
	SMSA, NUNIT2, BUILT, AIR"


central_ac_query <-
  "
SELECT 
	SMSA, NUNIT2, OARSYS, BUILT,
	COUNT(OARSYS) as raw_cent_ac,
	sum(WGT90GEO) as wgt_cent_ac
FROM
	{.x}
WHERE 
	SMSA in ('''6160''', '''6200''', '''5000''')
GROUP BY 
	SMSA, NUNIT2, BUILT, OARSYS"


# See raw data real quick -------------------------------------------------

raw_ac_vars <-
  map(
    tbls,
    ~ dbGetQuery(
      con,
      glue(
        "SELECT SMSA, NUNIT2, BUILT, AIR, AIRSYS, WGT90GEO 
                            FROM {.x}
                            WHERE SMSA in ('''6160''', '''6200''', '''5000''')"
      )
    )
  )


# Derive 2007, 2009 counts for ac vars ------------------------------------
room_ac_counts <-
  map(tbls, ~ dbGetQuery(con, sql(glue(room_ac_query))))

cent_ac_counts <-
  map(tbls, ~ dbGetQuery(con, sql(glue(central_ac_query))))


# Use raw data and to combine categories ----------------------------------
ac_combo_dat <-
  map(
    raw_ac_vars,
    ~ .x %>%
      mutate(
        AIR = parse_number(AIR),
        AIRSYS = parse_number(AIRSYS),
        ac_combo = case_when(
          AIR == 1 & AIRSYS == 1 ~ "Both",
          AIR == 2 & AIRSYS == 2 ~ 'No Room or Central',
          AIR == 1 & AIRSYS == 2 ~ 'Room Only',
          AIR == 2 & AIRSYS == 1 ~ 'Central Only',
          TRUE ~ "Sleep More"
        )
      ) %>%
      mutate(
        aircond_status = if_else(
          ac_combo %in% c('Room Only', 'Central Only', 'Both'),
          "AC",
          "No AC"
        )
      )
  )


## Making things a little more readable
ac_combo_dat <-
  map(
    ac_combo_dat,
    ~ .x %>%
      mutate(
        smsa_name = case_when(
          SMSA == "'6160'" ~ "Philadelphia, PA-NJ",
          SMSA == "'5000'" ~ "Miami-Hialeah, FL",
          SMSA == "'6200'" ~ "Phoenix, AZ",
          TRUE ~ "RUDE"
        ),
        bld_type = case_when(
          NUNIT2 == "'1'" ~ "Detached",
          NUNIT2 == "'2'" ~ "Attached",
          NUNIT2 == "'3'" ~ "Building 2 or more apartments",
          NUNIT2 == "'4'" ~ "Mobile Home",
          TRUE ~ "Sleep more big guy"
        ),
        ybl_broad = case_when(
          BUILT < 1950 ~ "Before 1950",
          between(BUILT, 1950, 1980) ~ "1950-1980",
          TRUE ~ "After 1980"
        )
      )
  )


count_by_group <- function(count_grp, pct_grp) {
  smsa_stats <-
    ac_bld_stats <-
      map(
        ac_combo_dat,
        ~ .x %>%
          filter(NUNIT2 != "'4'") %>%
          summarise(
            n = n(),
            wgt_n = sum(WGT90GEO),
            .by = all_of(count_grp)
          ) %>%
          mutate(
            pct = n / sum(n),
            wgt = wgt_n / sum(wgt_n),
            .by = all_of(pct_grp)
          )
      )

  return(smsa_stats)
}

count_grps <-
  list(
    c("smsa_name", "aircond_status"),
    c("smsa_name", "bld_type", "ybl_broad"),
    c("smsa_name", "bld_type", "ybl_broad", "aircond_status")
  )

pct_grps <-
  list(
    c("smsa_name"),
    c("smsa_name"),
    c("smsa_name", "bld_type", "ybl_broad")
  )


topline_stats <-
  map2(count_grps, pct_grps, ~ count_by_group(.x, .y))

# ac_smsa_stats <-
#   ac_bld_stats <-
#     map(
#       ac_combo_dat,
#       ~ .x %>%
#         filter(NUNIT2 != "'4'") %>%
#         summarise(
#           n_ac = n(),
#           wgt_n_ac = sum(WGT90GEO),
#           .by = c(smsa_name, aircond_status)
#         ) %>%
#         arrange(smsa_name, aircond_status) %>%
#         mutate(
#           pct = n_ac / sum(n_ac),
#           wgt_pct = wgt_n_ac / sum(wgt_n_ac),
#           .by = c(smsa_name)
#         )
#     )
#
#
# bld_stats <-
#   ac_stats <-
#     map(
#       ac_combo_dat,
#       ~ .x %>%
#         filter(NUNIT2 != "'4'") %>%
#         summarise(
#           n_bld = n(),
#           wgt_n_bld = sum(WGT90GEO),
#           .by = c(smsa_name, bld_type)
#         ) %>%
#         arrange(smsa_name, bld_type)
#     )
#
#
# ac_bld_stats <-
#   map(
#     ac_combo_dat,
#     ~ .x %>%
#       filter(NUNIT2 != "'4'") %>%
#       summarise(
#         n_ac = n(),
#         wgt_n_ac = sum(WGT90GEO),
#         .by = c(smsa_name, bld_type, aircond_status)
#       ) %>%
#       arrange(smsa_name, bld_type, aircond_status) %>%
#       mutate(
#         pct = n_ac / sum(n_ac),
#         wgt_pct = wgt_n_ac / sum(wgt_n_ac),
#         .by = c(smsa_name, bld_type)
#       )
#   )
