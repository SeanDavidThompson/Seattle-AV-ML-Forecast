# Seattle-AV-ML-Forecast

A parcel-level machine learning pipeline that forecasts Seattle's assessed value (AV) tax base from 2026 through 2031. This repository contains the code supporting the City of Seattle Office of Economic and Revenue Forecasts (OERF) April 2026 forecast.

> **Note on scope:** This is an earlier version of the pipeline, from before the commercial track was split into valuation-based subgroups (Major Office, Commercial Waterfront, Industrial Specialty, etc.). The commercial results here come from a single pooled LightGBM model and, in production, were cross-checked against a CoStar-based aggregate approach. The current production pipeline uses subgroup-specific models. See [Known limitations](#known-limitations) for detail.

## What it does

Given the King County Assessor (KCA) parcel panel, a set of economic scenario inputs, NWMLS housing market forecasts, CoStar commercial rents, and building permit data, the pipeline produces forecasted total assessed value for Seattle through 2031 under three macro scenarios (baseline, optimistic, pessimistic).

The forecast is parcel-level: each of roughly 250,000 Seattle parcels is individually predicted year by year, then aggregated. Three property tracks run in parallel:

- **Residential** (single-family, `prop_type == "R"`) — LightGBM on land and improvement values separately
- **Condominium** — LightGBM with condo-specific features (unit counts, building-level attributes)
- **Commercial** (`prop_type == "C"`) — LightGBM with features derived from CoStar quarterly rent and vacancy data

A fourth component, new construction, is forecasted separately in `scripts/05_new_construction_forecast.R` using permit data.

## Method in one paragraph

For each property track, the pipeline trains two LightGBM models: a *level* model that predicts the log of assessed value, and a *delta* model that predicts the year-over-year log change. These are combined in a sequential recursive forecast — the 2026 prediction feeds into the features used for 2027, and so on through 2031. Scenario differentiation enters through the economic panel (NWMLS median prices, inventory, closed sales; BLS/BEA macro inputs) and through CoStar quarterly aggregates for commercial. The final output is anchored to the certified 2026 total AV (~$303B) via share imputation, so forecast growth rates are applied to the certified base rather than to the model's own implied 2026 total.

## Repository structure

```
Seattle-AV-ML-Forecast/
├── main_ml.R                           # Orchestration: run_main_ml(), prep_scenario_caches(), av_fcst_summary()
├── av-forecast-2026.Rproj              # RStudio project file
│
├── scripts/
│   ├── 04_commercial_av_forecast.R     # Aggregate CoStar-based commercial forecast (cross-check)
│   ├── 05_new_construction_forecast.R  # New construction AV from permits
│   └── ml/
│       ├── 00_init.R                   # Packages, constants, LightGBM helpers, lookup decoding
│       ├── 01_import_{res,condo,comm}.R          # KCA CSV ingestion per track
│       ├── 02_transfrm{,_condo,_comm}.R          # Feature engineering per track
│       ├── 03_model_{land,impr}.R                # Residential land & improvement LightGBM
│       ├── 03_model_condo_{land,impr}.R          # Condo land & improvement LightGBM
│       ├── 03_model_comm_{land,impr}.R           # Commercial land & improvement LightGBM
│       ├── 04_retrofitting_values.R              # Fill missing historical AVs with model estimates
│       ├── 05_eval_holdout_2025.R                # Backtest on the 2025 holdout year
│       ├── 05_extend_panel_2026_2031{,_comm,_condo}.R  # Build the forecast-period feature panel
│       ├── 06_forecast_av_2026_2031_sequential{,_comm}.R  # Recursive year-by-year forecast
│       └── xx_*.R                                # Feeder scripts for economic data, NWMLS, CoStar, permits, change history
│
├── ad_hoc/
│   └── av_combined_forecast.R          # Combines res + condo + commercial + new construction into final table
│
└── data/
    ├── kca/2026-03-27/                 # KCA parcel extract (not tracked; see below)
    ├── costar/                         # CoStar quarterly commercial rents
    ├── nwmls/                          # NWMLS housing forecast (baseline/optimistic/pessimistic)
    ├── oerf/                           # OERF economic forecast input
    ├── permits/                        # Building permits for new construction
    ├── wrangled/                       # Intermediate cleaned inputs
    ├── cache/                          # Cached panels and intermediate RDS files
    ├── model/                          # Saved LightGBM models (timestamped)
    └── outputs/                        # Final forecast CSVs
```

## How to run it

### 1. Prerequisites

- R 4.3 or newer
- The following packages: `tidyverse`, `data.table`, `lightgbm`, `caret`, `here`, `lubridate`, `zoo`, `slider`, `sf`, `arrow`, `readxl`, `janitor`, `doParallel`, `foreach`, `scales`, `forcats`
- Roughly 16 GB of RAM (the residential panel is large)

### 2. Get the data

The King County Assessor parcel extract is not tracked in this repository. Download the full ZIP from the KCA data download page:

https://info.kingcounty.gov/assessor/datadownload/default.aspx

Unzip it into `data/kca/2026-03-27/` (or whatever extract date you are using — see `CFG$kca_date_data_extracted` in `main_ml.R`). At minimum the pipeline expects `EXTR_Parcel.csv`, `EXTR_ResBldg.csv`, `EXTR_CommBldg.csv`, `EXTR_CondoUnit.csv`, `EXTR_LookUp.csv`, and the change history files.

The NWMLS, CoStar, OERF economic forecast, and permits files in `data/` are the specific vintages used for the April 2026 run. CoStar data carries licensing restrictions and is included here only for reproducibility of the historical run — if you fork this repo, replace with your own licensed extract.

### 3. Run the pipeline

```r
# From the project root, open av-forecast-2026.Rproj in RStudio, then:
source("main_ml.R")

# First run (rebuilds everything, takes 2-3 hours on a workstation):
run_main_ml(
  prop_scope = "all",         # "res" | "com" | "condo" | "both" | "all"
  scenario   = "baseline",
  replicate  = TRUE           # TRUE = rebuild all caches
)

# Subsequent scenario runs (reuses cached panels):
prep_scenario_caches("optimistic")
run_main_ml(scenario = "optimistic", extend_replicate = TRUE, forecast_only = TRUE)

prep_scenario_caches("pessimistic")
run_main_ml(scenario = "pessimistic", extend_replicate = TRUE, forecast_only = TRUE)

# Summarise across all three scenarios:
av_fcst_summary(prop_scope = "all", export_csv = TRUE)
```

The orchestrator uses replicate flags (`panel_replicate`, `model_replicate`, `retrofit_replicate`, `extend_replicate`, `forecast_only`) so you can re-run individual stages without rebuilding upstream. Set `CFG` at the top of `main_ml.R` to change defaults.

## Pipeline stages

1. **Import** (`01_import_*.R`) — Read KCA parcel, building, condo, and commercial CSVs; filter to Seattle levy codes (`0010, 0011, 0013, 0014, 0016, 0025, 0030, 0032`).
2. **Transform** (`02_transfrm*.R`) — Feature engineering per track: log transforms, lag variables, categorical encoding, neighborhood and area fixed effects.
3. **Panel assembly** (`xx_*.R`) — Join parcel history with change records, economic forecasts, NWMLS, CoStar, permits.
4. **Model** (`03_model_*.R`) — Train level and delta LightGBM models per track with 5-fold CV and early stopping. Hyperparameters live in `train_lgbm_log_model()` in `00_init.R`.
5. **Retrofit** (`04_retrofitting_values.R`) — For parcels with missing historical AV components, impute using the trained model so the historical panel is complete before forecasting.
6. **Holdout evaluation** (`05_eval_holdout_2025.R`) — Backtest against the 2025 holdout year to produce RMSE and MAPE at parcel and aggregate levels.
7. **Extend panel** (`05_extend_panel_2026_2031*.R`) — Build feature rows for forecast years using scenario-specific economic, NWMLS, and CoStar inputs.
8. **Sequential forecast** (`06_forecast_av_2026_2031_sequential*.R`) — Recursively predict year by year, feeding each year's prediction back into the next year's lag features.

## Key design decisions

- **Appraised AV as the consistent concept.** The target is `appr_land_val + appr_imps_val`, not taxable AV. This avoids having to model exemption churn separately.
- **Share imputation to the certified base.** Forecast growth rates are applied to the certified 2026 total ($303B), not to whatever total the model's absolute predictions happen to imply. This keeps the forecast anchored to a known public number.
- **Level + delta ensemble.** Training both a level and a delta model lets the pipeline blend long-horizon level anchoring with short-horizon change dynamics. In practice the delta model dominates at short horizons and the level model keeps the multi-year trajectory from drifting.
- **Rolling-year CV folds.** `make_rolling_year_folds()` in `00_init.R` cuts folds by tax year instead of randomly, so validation is always on years the model has not seen — closer to the actual forecasting task than random CV would be.
- **Three macro scenarios.** Scenario differentiation enters through the economic forecast inputs and NWMLS housing forecasts, not through separately tuned models. The same model weights are applied to three different feature futures.

## Known limitations

- **Pre-subgroup commercial model.** The commercial LightGBM here pools all commercial property into one model. In production this was found to be unreliable (the ML commercial panel had a 134% growth rate bug in an earlier iteration, and even after fixing it the pooled model struggled to separate income-valued from cost-valued property). The production April 2026 forecast used a CoStar-based aggregate growth-rate approach for commercial as a stopgap; a subgroup ML commercial model (Major Office / Area 280, Commercial Waterfront / Area 12, Industrial Specialty / Area 540) is the next iteration.
- **Land value retrofitting.** The `appr_land_val_filled` column on the ML panel contains model-imputed values for parcels with missing history, even for years where KCA published a certified value. Correlation with raw KCA certified land values for those parcels is about 0.43. This is acceptable for aggregate forecasts but means parcel-level predictions should not be compared directly to KCA published values at the individual parcel. Use `appr_land_val` (unfilled) for parcel-level comparisons where coverage permits; correlation is 0.86–0.91 there.
- **Residual autocorrelation in NWMLS inputs.** Upstream NWMLS closed-sales models (feeding the commercial and residential panels) have a persistent Breusch-Godfrey lag-2 serial correlation issue attributable to the ~60-day contract-to-close pipeline cycle. Parameter estimates are unbiased but standard errors are understated.

## Outputs

Final forecast tables are written to `data/outputs/` as timestamped CSVs. The relevant ones for the April 2026 forecast are:

- `OERF_AV_Appraised_Total_<date>.csv` — Parcel-level appraised AV forecast, aggregated by scenario
- `OERF_AV_Forecast_<date>.csv` — Final forecast anchored to the certified base
- `OERF_New_Construction_Forecast_<date>.csv` — New construction component

## Author

Sean Thompson, Economist, City of Seattle Office of Economic and Revenue Forecasts.

## License

See [LICENSE](LICENSE). King County Assessor data is a public record; CoStar data is proprietary and is not licensed for redistribution under this repository.
