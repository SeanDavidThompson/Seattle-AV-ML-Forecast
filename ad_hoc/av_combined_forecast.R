# =============================================================================
# av_combined_forecast.R
# Combine ML forecasts (res + condo), aggregate commercial, new construction,
# and personal property into a single AV forecast table.
#
# Strategy:
#   1. Sum all components (appraised AV) to get total appraised forecast
#   2. Compute YoY growth rates from the appraised total
#   3. Apply those growth rates to the certified AV base ($303B for 2026)
#      to produce the final forecast anchored to the certified number
#
# Inputs:
#   1. ML residential forecast  — from av_fcst_summary(prop_scope="res")
#   2. ML condo forecast        — from av_fcst_summary(prop_scope="condo")
#   3. Aggregate commercial     — from 04_commercial_av_forecast.R (appraised)
#   4. New construction         — from 05_new_construction_forecast.R
#   5. Personal property        — $9.07B base (2025), grown at res growth rates
#
# Output: one table, 3 scenario columns, rows = tax_yr 2026-2031
# =============================================================================

library(tidyverse)
library(here)

# ============================================================================
# 1. ML RESIDENTIAL FORECAST (from av_fcst_summary export or console output)
#    Replace with your actual numbers or read from CSV:
#    ml_res <- read_csv("path/to/av_fcst_summary_res_20260402.csv")
# ============================================================================
ml_res <- tribble(
  ~tax_yr, ~baseline,    ~optimistic,  ~pessimistic,
  2026,    203220000000, 203030000000, 203030000000,
  2027,    212530000000, 212520000000, 211480000000,
  2028,    222030000000, 221590000000, 220400000000,
  2029,    232000000000, 231240000000, 228480000000,
  2030,    243080000000, 241990000000, 239380000000,
  2031,    254650000000, 254160000000, 250080000000
)

# ============================================================================
# 2. ML CONDO FORECAST (from av_fcst_summary export)
#    Replace with actual numbers once the condo run finishes.
# ============================================================================
ml_condo <- tribble(
  ~tax_yr, ~baseline,    ~optimistic,  ~pessimistic,
  2026,    36330000000,  36330000000,  36330000000,
  2027,    37000000000,  37000000000,  37000000000,
  2028,    37770000000,  37770000000,  38310000000,
  2029,    38570000000,  38570000000,  39100000000,
  2030,    39450000000,  39450000000,  39970000000,
  2031,    40390000000,  40400000000,  40910000000
)

# ============================================================================
# 3. AGGREGATE COMMERCIAL FORECAST (from 04_commercial_av_forecast.R)
#    Appraised AV, prop_type C only, correct Seattle levy codes
#    Read from CSV or paste output directly:
# ============================================================================
com_csv_path <- here("data", "wrangled",
                     list.files(here("data", "wrangled"),
                                pattern = "commercial_av_forecast_.*\\.csv$") %>%
                       sort() %>% tail(1))

if (file.exists(com_csv_path)) {
  old_com <- read_csv(com_csv_path, show_col_types = FALSE) %>%
    rename(tax_yr = tax_year) %>%
    mutate(tax_yr = as.integer(tax_yr))
  message("Loaded commercial forecast from: ", basename(com_csv_path))
} else {
  # Fallback: hardcode from latest run (appraised, bill_yr 2026 base)
  message("Commercial CSV not found, using hardcoded values")
  old_com <- tribble(
    ~tax_yr, ~baseline,       ~optimistic,     ~pessimistic,
    2026,    96231797691,     96231797691,     96231797691,
    2027,    94972679909,     95934997886,     94010361932,
    2028,    98025368770,     99977968330,     96092015569,
    2029,    101420122602,    104440123064,    98458894556,
    2030,    106086214162,    110289558682,    102004158316,
    2031,    110085469106,    115550167562,    104829485688
  )
}

# ============================================================================
# 4. NEW CONSTRUCTION FORECAST (from 05_new_construction_forecast.R)
#    Read from CSV or hardcode from latest run:
# ============================================================================
nc_csv_path <- here(list.files(here(),
                               pattern = "OERF_New_Construction_Forecast_.*\\.csv$") %>%
                      sort() %>% tail(1))

if (file.exists(nc_csv_path)) {
  new_con <- read_csv(nc_csv_path, show_col_types = FALSE) %>%
    mutate(tax_year = as.integer(tax_year)) %>%
    rename(tax_yr = tax_year)
  message("Loaded NC forecast from: ", basename(nc_csv_path))
} else {
  message("NC CSV not found, using hardcoded values")
  new_con <- tribble(
    ~tax_yr, ~baseline,     ~optimistic,    ~pessimistic,
    2026,    4140241033,    4140241033,     4140241033,
    2027,    4002698604,    4012995397,     3943989209,
    2028,    4104599456,    4201618982,     3786944067,
    2029,    4165047383,    4367429554,     3623377121,
    2030,    4353555622,    4623307825,     3740179921,
    2031,    4598617484,    4928219290,     4065658508
  )
}

# Filter to common years
forecast_years <- 2026:2031
new_con <- new_con %>% filter(tax_yr %in% forecast_years)
old_com <- old_com %>% filter(tax_yr %in% forecast_years)

# ============================================================================
# 5. PERSONAL PROPERTY
#    Base: $9,073,883,825 (2025 from KC 25citypersprop report, Seattle row)
#    Growth: match residential YoY growth rates per scenario
# ============================================================================
pp_base_2026 <- 9073883825  # Update if 2026 certified PP is available

# Compute residential YoY growth rates
res_gyy <- ml_res %>%
  arrange(tax_yr) %>%
  mutate(
    gyy_bas = baseline / lag(baseline) - 1,
    gyy_opt = optimistic / lag(optimistic) - 1,
    gyy_pes = pessimistic / lag(pessimistic) - 1
  )

# Build personal property forecast using res growth rates
pp_forecast <- tibble(tax_yr = forecast_years)
pp_forecast$baseline    <- NA_real_
pp_forecast$optimistic  <- NA_real_
pp_forecast$pessimistic <- NA_real_
pp_forecast$baseline[1]    <- pp_base_2026
pp_forecast$optimistic[1]  <- pp_base_2026
pp_forecast$pessimistic[1] <- pp_base_2026

for (i in 2:nrow(pp_forecast)) {
  yr <- pp_forecast$tax_yr[i]
  gyy_row <- res_gyy %>% filter(tax_yr == yr)
  if (nrow(gyy_row) == 0) next
  pp_forecast$baseline[i]    <- pp_forecast$baseline[i-1]    * (1 + gyy_row$gyy_bas)
  pp_forecast$optimistic[i]  <- pp_forecast$optimistic[i-1]  * (1 + gyy_row$gyy_opt)
  pp_forecast$pessimistic[i] <- pp_forecast$pessimistic[i-1] * (1 + gyy_row$gyy_pes)
}

# ============================================================================
# COMBINE ALL COMPONENTS (APPRAISED TOTAL)
# ============================================================================
combine <- function(col) {
  ml_res[[col]][ml_res$tax_yr %in% forecast_years] +
    ml_condo[[col]][ml_condo$tax_yr %in% forecast_years] +
    old_com[[col]] +
    new_con[[col]] +
    pp_forecast[[col]]
}

appraised_total <- tibble(
  tax_yr      = forecast_years,
  baseline    = combine("baseline"),
  optimistic  = combine("optimistic"),
  pessimistic = combine("pessimistic")
)

# Compute YoY growth rates from appraised totals
appraised_total <- appraised_total %>%
  mutate(
    gyy_bas = baseline / lag(baseline) - 1,
    gyy_opt = optimistic / lag(optimistic) - 1,
    gyy_pes = pessimistic / lag(pessimistic) - 1
  )

# ============================================================================
# DISPLAY APPRAISED TOTAL (for verification)
# ============================================================================
cat("\n=== Appraised AV Total (All Components) ===\n\n")

display_appr <- appraised_total %>%
  mutate(
    across(c(baseline, optimistic, pessimistic),
           ~ scales::dollar(.x, scale = 1e-9, suffix = "B", accuracy = 0.01)),
    across(starts_with("gyy_"),
           ~ scales::percent(.x, accuracy = 0.1))
  )
print(display_appr, n = Inf)

# Component breakdown for 2026 (verification)
cat("\n=== 2026 Component Breakdown (Appraised) ===\n")
cat(sprintf("  Residential (ML):   %s\n", scales::dollar(ml_res$baseline[ml_res$tax_yr == 2026],    scale = 1e-9, suffix = "B")))
cat(sprintf("  Condo (ML):         %s\n", scales::dollar(ml_condo$baseline[ml_condo$tax_yr == 2026],  scale = 1e-9, suffix = "B")))
cat(sprintf("  Commercial (agg):   %s\n", scales::dollar(old_com$baseline[old_com$tax_yr == 2026],   scale = 1e-9, suffix = "B")))
cat(sprintf("  New Construction:   %s\n", scales::dollar(new_con$baseline[new_con$tax_yr == 2026],   scale = 1e-9, suffix = "B")))
cat(sprintf("  Personal Property:  %s\n", scales::dollar(pp_forecast$baseline[1],                    scale = 1e-9, suffix = "B")))
cat(sprintf("  ─────────────────────────\n"))
cat(sprintf("  TOTAL 2026 (appr):  %s\n", scales::dollar(appraised_total$baseline[1],                scale = 1e-9, suffix = "B")))

# ============================================================================
# IMPUTE: APPLY GROWTH RATES TO CERTIFIED BASE
# ============================================================================
# The appraised total won't match the certified number exactly (different AV
# concepts, exemptions, rounding). Instead, we use the appraised forecast's
# YoY growth rates and apply them to the certified base going forward.

cert_av_2026 <- 306466960900  # Update with actual certified 2026 AV

cat(sprintf("\n  Certified 2026 AV:  %s\n", scales::dollar(cert_av_2026, scale = 1e-9, suffix = "B")))
cat(sprintf("  Appr/Cert ratio:    %.4f\n", appraised_total$baseline[1] / cert_av_2026))

# Build certified-base forecast using appraised growth rates
certified_forecast <- tibble(tax_yr = forecast_years) %>%
  mutate(
    baseline    = NA_real_,
    optimistic  = NA_real_,
    pessimistic = NA_real_
  )

# 2026 = certified (identical across scenarios)
certified_forecast$baseline[1]    <- cert_av_2026
certified_forecast$optimistic[1]  <- cert_av_2026
certified_forecast$pessimistic[1] <- cert_av_2026

# 2027+ = compound forward using appraised growth rates
for (i in 2:nrow(certified_forecast)) {
  gyy_row <- appraised_total %>% filter(tax_yr == certified_forecast$tax_yr[i])
  if (nrow(gyy_row) == 0 || is.na(gyy_row$gyy_bas)) next

  certified_forecast$baseline[i]    <-
    certified_forecast$baseline[i-1]    * (1 + gyy_row$gyy_bas)
  certified_forecast$optimistic[i]  <-
    certified_forecast$optimistic[i-1]  * (1 + gyy_row$gyy_opt)
  certified_forecast$pessimistic[i] <-
    certified_forecast$pessimistic[i-1] * (1 + gyy_row$gyy_pes)
}

# Add growth rates for display
certified_forecast <- certified_forecast %>%
  mutate(
    gyy_bas = baseline / lag(baseline) - 1,
    gyy_opt = optimistic / lag(optimistic) - 1,
    gyy_pes = pessimistic / lag(pessimistic) - 1
  )

# ============================================================================
# DISPLAY CERTIFIED-BASE FORECAST
# ============================================================================
cat("\n=== FINAL AV Forecast (Certified Base) ===\n\n")

display_cert <- certified_forecast %>%
  mutate(
    across(c(baseline, optimistic, pessimistic),
           ~ scales::dollar(.x, scale = 1e-9, suffix = "B", accuracy = 0.01)),
    across(starts_with("gyy_"),
           ~ scales::percent(.x, accuracy = 0.1))
  )
print(display_cert, n = Inf)

# ============================================================================
# EXPORT
# ============================================================================
# Appraised total (for reference)
write_csv(
  appraised_total %>% select(tax_yr, baseline, optimistic, pessimistic),
  here(str_c("OERF_AV_Appraised_Total_", Sys.Date(), ".csv"))
)

# Certified-base forecast (the one you use)
write_csv(
  certified_forecast %>% select(tax_yr, baseline, optimistic, pessimistic),
  here(str_c("OERF_AV_Forecast_", Sys.Date(), ".csv"))
)

cat(sprintf("\nExported appraised total and certified-base forecast.\n"))
