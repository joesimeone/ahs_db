library(duckdb)
library(tidyverse)
library(glue)
library(srvyr)


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
	sum(WEIGHT) as wgt_ac_primary
FROM
	national.natl_2015
WHERE 
	OMB13CBSA in ('''33100''', '''37980''', '''38060''')
GROUP BY 
	OMB13CBSA, BLD, YRBUILT, ACPRIMARY"


# See raw data real quick -------------------------------------------------
rep_weights <-
  paste("REPWEIGHT", 1:160, sep = "", collapse = ",")

raw_ac_vars <-
  dbGetQuery(
    con,
    glue(
      "SELECT OMB13CBSA, BLD, YRBUILT, ACPRIMARY, WEIGHT, {rep_weights} 
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
        wgt_n = sum(WEIGHT),
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


## ---------------------------------------------------------------------------=
# Incorporate survey SEs ----
## ---------------------------------------------------------------------------=

# Create svr object -------------------------------------------------------
## Ratio noted in AHS how to use weights documentation
ahs_fay_num <- 4 / 160


ahs_svr <-
  raw_ac_vars %>%
  as_survey_rep(
    weights = WEIGHT,
    repweights = REPWEIGHT1:REPWEIGHT160,
    type = "Fay",
    rho = ahs_fay_num
  )


# Re-do case_when post svr just in case -----------------------------------

## Rather than use objects already coded, we're going to code everything again
## to make sure grouping and SEs are being calculated correctly... Not sure if nec, but hey

ahs_svr <-
  ahs_svr %>%
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
  ) %>%
  filter(bld_type != "Irrelevant")


# Function for counts and proportions -------------------------------------

calc_svr_counts <- function(link_vars) {
  ahs_counts <-
    ahs_svr %>%
    survey_count(across(all_of({{ link_vars }})), vartype = c("se", "ci"))

  ahs_props <-
    ahs_svr %>%
    group_by(across(all_of({{ link_vars }}))) %>%
    summarise(
      pct = survey_prop(vartype = c("se", "ci"), prop_method = c("beta"))
    )

  ahs_combo <-
    ahs_counts %>%
    left_join(ahs_props, by = link_vars)

  return(ahs_combo)
}

link_grp_ahs <-
  map(count_grps, ~ calc_svr_counts(.x))


## ----------------------------------------------------------------------------=
# Write object for further analysis ----
## ----------------------------------------------------------------------------=
heat_study_path <- "C:/Users/js5466/OneDrive - Drexel University/r_master/new_projects/HEAT-Housing"
vali_path <- fs::path(heat_study_path, "validation", "ahs")

saveRDS(topline_stats, fs::path(vali_path, "ahs15_topline_estimates.rds"))
saveRDS(link_grp_ahs, fs::path(vali_path, "ahs15_estimates.rds"))


## Hard code out of dbConnection in case I forget
dbDisconnect(con)
