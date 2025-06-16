library(duckdb)
library(tidyverse)
library(glue)


con <- dbConnect(duckdb(), 'duckdb/ahs.db')

dbListTables(con)


# dbGetQuery(con, "SELECT column_name, data_type
#                  FROM information_schema.columns
#                  WHERE table_schema = 'metro' AND table_name = 'test'")

fields <- dbGetQuery(con, "PRAGMA table_info('national.natl_2015')")
natl_cbsa <- dbGetQuery(
  con,
  'SELECT DISTINCT OMB13CBSA FROM national.natl_2015'
)


ac_query <-
  "
SELECT 
	OMB13CBSA, BLD, YRBUILT, ACPRIMARY,
	COUNT(ACPRIMARY) as raw_ac_primary,
	sum(SP1WEIGHT) as wgt_ac_primary
FROM
	national.natl_2015
WHERE 
	OMB13CBSA in ('''33100''', '''37980''', '''38060''')
GROUP BY 
	OMB13CBSA, BLD, YRBUILT, ACPRIMARY"


# See raw data real quick -------------------------------------------------

raw_ac_vars <-
  dbGetQuery(
    con,
    glue(
      "SELECT OMB13CBSA, BLD, YRBUILT, ACPRIMARY, SP1WEIGHT 
                            FROM national.natl_2015
                            WHERE OMB13CBSA in ('''33100''', '''37980''', '''38060''')"
    )
  )


# Derive 2007, 2009 counts for ac vars ------------------------------------
ac_counts <-
  dbGetQuery(con, sql(glue(ac_query)))


vars_to_strip <-
  c("ACPRIMARY", "OMB13CBSA", "BLD", "YRBUILT")
# Use raw data and to combine categories ----------------------------------
test <-
  raw_ac_vars %>%
  mutate(
    across(all_of(vars_to_strip), ~ str_remove_all(., "[''']"))
  )


ac_combo_dat <-
  raw_ac_vars %>%
  mutate(
    across(all_of(vars_to_strip), ~ str_remove_all(., "[''']"))
  ) %>%
  mutate(
    ac_status = if_else(ACPRIMARY == "12", "No AC", "AC"),
    cbsa_name = case_when(
      OMB13CBSA == "33100" ~ "Miami",
      OMB13CBSA == "37980" ~ "Philly",
      OMB13CBSA == "38060" ~ "Phoenix",
      TRUE ~ "RUDE"
    ),
    bld_type = case_when(
      BLD %in% c("06", "07", "08, 09") ~ "Apt 5+ Units",
      BLD == "02" ~ "Single Fam. Detached",
      BLD == "03" ~ "Single Fam. Attached",
      TRUE ~ "Irrelevant"
    ),
    ybl_broad = parse_number(YRBUILT),
    ybl_broad = case_when(
      ybl_broad < 1940 ~ "Before 1950",
      between(ybl_broad, 1950, 1979) ~ "1950-1980",
      TRUE ~ "After 1980"
    )
  )


count_by_group <- function(count_grp, pct_grp) {
  cbsa_stats <-
    ac_bld_stats <-
      ac_combo_dat %>%
      filter(BLD != "Irrelevant") %>%
      summarise(
        n = n(),
        wgt_n = sum(SP1WEIGHT),
        .by = all_of(count_grp)
      ) %>%
      mutate(
        pct = n / sum(n),
        wgt = wgt_n / sum(wgt_n),
        .by = all_of(pct_grp)
      )

  return(cbsa_stats)
}

count_grps <-
  list(
    c("cbsa_name", "ac_status"),
    c("cbsa_name", "bld_type", "ybl_broad"),
    c("cbsa_name", "bld_type", "ybl_broad", "ac_status")
  )

pct_grps <-
  list(
    c("cbsa_name"),
    c("cbsa_name"),
    c("cbsa_name", "bld_type", "ybl_broad")
  )


topline_stats <-
  map2(count_grps, pct_grps, ~ count_by_group(.x, .y))


## Hard code out of dbConnection in case I forget
dbDisconnect(con)

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
