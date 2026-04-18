# =============================================================================
# main_ml.R  —  Master orchestration script
# =============================================================================

# ---- Top-level config -------------------------------------------------------
CFG <- list(
  kca_date_data_extracted = "2026-03-27",
  seed                    = 123,
  
  replicate             = FALSE,
  panel_replicate       = TRUE,  # TRUE = rebuild panels; FALSE = load from cache
  model_replicate       = TRUE,
  retrofit_replicate    = TRUE,
  diagnostics_replicate = FALSE,
  extend_replicate      = TRUE,
  forecast_only         = FALSE,  # TRUE = load cached extended panels and jump to Step 6

  import_raw_changes    = FALSE,  # TRUE = re-read 65M-row change CSVs
  
  prop_scope = "condo",
  # "res" | "com" | "condo" | "both" (res+com) | "all"
  scenario   = "baseline",

  # Forecast horizon — update these each year instead of touching sourced scripts
  forecast_start = 2026,  # first year the models predict forward
  forecast_end   = 2031,  # last year of the forecast horizon
  
  cache_dir  = here::here("data", "cache"),
  model_dir  = here::here("data", "model"),
  output_dir = here::here("data", "outputs")
)

# =============================================================================
# run_main_ml()
# =============================================================================
run_main_ml <- function(replicate             = CFG$replicate,
                        panel_replicate       = CFG$panel_replicate,
                        model_replicate       = CFG$model_replicate,
                        retrofit_replicate    = CFG$retrofit_replicate,
                        diagnostics_replicate = CFG$diagnostics_replicate,
                        extend_replicate      = CFG$extend_replicate,
                        forecast_only         = CFG$forecast_only,
                        import_raw_changes    = CFG$import_raw_changes,
                        kca_date_data_extracted = CFG$kca_date_data_extracted,
                        scenario              = CFG$scenario,
                        prop_scope            = CFG$prop_scope,
                        seed                  = CFG$seed,
                        forecast_start        = CFG$forecast_start,
                        forecast_end          = CFG$forecast_end,
                        cache_dir             = CFG$cache_dir,
                        model_dir             = CFG$model_dir,
                        output_dir            = CFG$output_dir) {
  # ---- Validate ---------------------------------------------------------------
  valid_scenarios <- c("baseline", "optimistic", "pessimistic")
  if (!scenario %in% valid_scenarios)
    stop("scenario must be one of: ",
         paste(valid_scenarios, collapse = ", "))
  
  valid_scopes <- c("res", "com", "condo", "both", "all")
  if (!prop_scope %in% valid_scopes)
    stop("prop_scope must be one of: ",
         paste(valid_scopes, collapse = ", "))

  if (!is.numeric(forecast_start) || !is.numeric(forecast_end) ||
      forecast_start >= forecast_end)
    stop("forecast_start must be a year < forecast_end")
  forecast_start <- as.integer(forecast_start)
  forecast_end   <- as.integer(forecast_end)
  
  run_res   <- prop_scope %in% c("res", "both", "all")
  run_com   <- prop_scope %in% c("com", "both", "all")
  run_condo <- prop_scope %in% c("condo", "all")
  
  # ---- Header -----------------------------------------------------------------
  start_time <- Sys.time()
  message("==============================================")
  message("run_main_ml() started at: ",
          format(start_time, "%Y-%m-%d %H:%M:%S"))
  message(
    "replicate=", replicate,
    " | panel_replicate=", panel_replicate,
    " | model_replicate=", model_replicate,
    " | retrofit=", retrofit_replicate,
    " | diagnostics=", diagnostics_replicate,
    " | extend=", extend_replicate,
    " | forecast_only=", forecast_only
  )
  message("scenario = ", scenario, " | prop_scope = ", prop_scope)
  message("forecast = ", forecast_start, "-", forecast_end)
  message("kca_date = ", kca_date_data_extracted, " | seed = ", seed)
  message("import_raw_changes = ", import_raw_changes)
  message("==============================================")
  
  # Must be defined before first use (sources 00_init.R immediately below)
  source_global <- function(path) source(path, local = .GlobalEnv)

  # ---- Globals for sourced scripts --------------------------------------------
  assign("kca_date_data_extracted", kca_date_data_extracted, envir = .GlobalEnv)
  assign("retrofit_replicate",      retrofit_replicate,      envir = .GlobalEnv)
  assign("forecast_start",          forecast_start,          envir = .GlobalEnv)
  assign("forecast_end",            forecast_end,            envir = .GlobalEnv)
  assign("diagnostics_replicate",   diagnostics_replicate,   envir = .GlobalEnv)
  assign("scenario",                scenario,                envir = .GlobalEnv)
  assign("cache_dir",               cache_dir,               envir = .GlobalEnv)
  assign("model_dir",               model_dir,               envir = .GlobalEnv)
  assign("output_dir",              output_dir,              envir = .GlobalEnv)
  set.seed(seed)

  source_global(here::here("scripts", "ml", "00_init.R"))
  
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(model_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # ============================================================================
  # HELPERS
  # ============================================================================
  
  cache_path <- function(name)
    file.path(cache_dir, paste0(name, ".rds"))
  
  cache_save <- function(obj_name) {
    if (!exists(obj_name, envir = .GlobalEnv))
      return(invisible(NULL))
    obj   <- get(obj_name, envir = .GlobalEnv)
    sz_mb <- round(as.numeric(object.size(obj)) / 1024^2, 1)
    saveRDS(obj, cache_path(obj_name))
    message("  \U1f4be cached: ", obj_name, " (", sz_mb, " MB)")
  }
  
  cache_load <- function(obj_name) {
    p <- cache_path(obj_name)
    if (!file.exists(p))
      stop("Missing cache file: ",
           p,
           "\nRun with replicate=TRUE once to rebuild caches.")
    assign(obj_name, readRDS(p), envir = .GlobalEnv)
    message("  \u2705 loaded: ", obj_name)
  }
  
  # Remove named objects from GlobalEnv after caching/consumption.
  # Logs total MB freed and calls gc() once.
  drop_if_exists <- function(...) {
    nms      <- c(...)
    freed_mb <- 0
    dropped  <- character(0)
    for (nm in nms) {
      if (exists(nm, envir = .GlobalEnv)) {
        freed_mb <- freed_mb +
          as.numeric(object.size(get(nm, envir = .GlobalEnv))) / 1024^2
        rm(list = nm, envir = .GlobalEnv)
        dropped <- c(dropped, nm)
      }
    }
    if (length(dropped) > 0) {
      gc(verbose = FALSE)
      message("  \U1f9f9 freed: ",
              paste(dropped, collapse = ", "),
              "  (",
              round(freed_mb, 1),
              " MB)")
    }
    invisible(NULL)
  }
  
  latest_model_file <- function(prefix) {
    pat   <- paste0("^", prefix, ".*\\.rds$")
    files <- list.files(model_dir, pattern = pat, full.names = TRUE)
    if (length(files) == 0)
      return(NULL)
    files[which.max(file.info(files)$mtime)]
  }
  
  load_model <- function(obj_name, prefix, required = TRUE) {
    if (exists(obj_name, envir = .GlobalEnv))
      return(invisible(NULL))
    f <- latest_model_file(prefix)
    if (is.null(f)) {
      if (required)
        stop(
          "No model file found for: ",
          obj_name,
          "\nRun with model_replicate=TRUE once to train models."
        )
      message("  \u2139\ufe0f  optional model not found (skipping): ",
              obj_name)
      return(invisible(NULL))
    }
    assign(obj_name, readRDS(f), envir = .GlobalEnv)
    message("  \u2705 loaded: ", obj_name, " (", basename(f), ")")
  }
  
  load_train_frame <- function(obj_name, file_name) {
    if (exists(obj_name, envir = .GlobalEnv))
      return(invisible(NULL))
    p <- file.path(cache_dir, file_name)
    if (!file.exists(p)) {
      message(
        "  \u2139\ufe0f  training frame not found: ",
        file_name,
        "  (run model_replicate=TRUE to generate)"
      )
      return(invisible(NULL))
    }
    assign(obj_name, readRDS(p), envir = .GlobalEnv)
    message("  \u2705 loaded training frame: ", obj_name)
  }
  
  expose_cv <- function(cv_name, model_name, feat_name) {
    if (!exists(cv_name, envir = .GlobalEnv))
      return(invisible(NULL))
    cv <- get(cv_name, envir = .GlobalEnv)
    assign(model_name, cv$model, envir = .GlobalEnv)
    assign(feat_name, cv$x_cols, envir = .GlobalEnv)
  }
  # ============================================================================
  # STEP 1  —  ETL + Transforms
  # ============================================================================
  
  # --------------------------------------------------------------------------
  # 1a. Residential
  # ============================================================================
  # STEP 1  —  ETL + Transforms
  # (skipped entirely when panel_replicate=FALSE — panels loaded from cache in Step 2)
  # (skipped entirely when forecast_only=TRUE — extended panels loaded from cache in Step 5b)
  # ============================================================================
  if (forecast_only) {
    message("\n--- Steps 1-5b: Skipped (forecast_only=TRUE — loading cached extended panels) ---")
  } else if (panel_replicate) {

    # --------------------------------------------------------------------------
    # 1a. Residential
    # --------------------------------------------------------------------------
    if (run_res) {
      message("\n--- Step 1a: ETL + transforms (residential) ---")
      if (replicate) {
        source_global(here::here("scripts", "ml", "01_import_res.R"))
        source_global(here::here("scripts", "ml", "02_transfrm.R"))
        cache_save("parcel_res_full")

        if (import_raw_changes) {
          message("  Importing raw changes history (import_raw_changes = TRUE) ...")
          source_global(here::here("scripts", "ml", "xx_tracking_changes.R"))
          cache_save("changes_long_tbl")
        } else {
          message("  Skipping raw changes import (import_raw_changes = FALSE) ...")
          cache_load("changes_long_tbl")
        }

        source_global(here::here("scripts", "ml", "xx_av_history.R"))
        cache_save("av_history_cln")

      } else {
        for (nm in c("parcel_res_full", "changes_long_tbl", "av_history_cln"))
          cache_load(nm)
      }
    }

    # --------------------------------------------------------------------------
    # 1b. Commercial
    # --------------------------------------------------------------------------
    # parcel_comm_full is also required by xx_combine_res_comm_condo_panel.R
    # (for the small-MF cross-track dedup), so load it whenever run_condo=TRUE
    # even if run_com=FALSE.
    if (run_com || run_condo) {
      message("\n--- Step 1b: ETL + transforms (commercial) ---")
      if (replicate && run_com) {
        source_global(here::here("scripts", "ml", "01_import_comm.R"))
        source_global(here::here("scripts", "ml", "02_transfrm_comm.R"))
        cache_save("parcel_comm_full")
      } else {
        cache_load("parcel_comm_full")
      }
    }

    # --------------------------------------------------------------------------
    # 1c. Condo
    # --------------------------------------------------------------------------
    # Also ensure parcel_res_full and av_history_cln are available when
    # run_condo=TRUE but run_res=FALSE — both are required by
    # xx_combine_res_comm_condo_panel.R for the residential backbone build.
    if (run_condo && !run_res) {
      if (!exists("parcel_res_full", envir = .GlobalEnv))
        cache_load("parcel_res_full")
      if (!exists("changes_long_tbl", envir = .GlobalEnv))
        cache_load("changes_long_tbl")
      if (!exists("av_history_cln", envir = .GlobalEnv))
        cache_load("av_history_cln")
    }

    if (run_condo) {
      message("\n--- Step 1c: ETL + transforms (condo) ---")
      if (replicate) {
        source_global(here::here("scripts", "ml", "01_import_condo.R"))
        source_global(here::here("scripts", "ml", "02_transfrm_condo.R"))
        cache_save("parcel_condo_full")
        cache_save("condo_complex_cln")
      } else {
        cache_load("parcel_condo_full")
        cache_load("condo_complex_cln")
      }
    }

  } else {
    message("\n--- Step 1: Skipped (panel_replicate=FALSE — panels load from cache in Step 2) ---")
  }

  if (!forecast_only) {

  # ============================================================================
  # STEP 2  —  Panel Assembly
  # ============================================================================
  message("\n--- Step 2: Panel assembly (scenario = ", scenario, ") ---")

  if (panel_replicate) {
    # --------------------------------------------------------------------------
    # panel_replicate = TRUE: rebuild panels from scratch
    # --------------------------------------------------------------------------

    # 2a. Residential backbone — always built; chg_dt / av_dt consumed here
    source_global(here::here("scripts", "ml", "xx_combine_parcel_history_changes.R"))
    gc(verbose = FALSE)
    source_global(here::here("scripts", "ml", "xx_permits_to_panel.R"))
    source_global(here::here("scripts", "ml", "xx_nwmls_to_panel.R"))
    source_global(here::here("scripts", "ml", "xx_econ_to_panel.R"))

    # 2b. Stack property types and attach market signals
    if (run_condo) {
      source_global(here::here("scripts", "ml", "xx_combine_res_comm_condo_panel.R"))
      # → creates panel_tbl_res, panel_tbl_com, panel_tbl_condo, panel_tbl_all
      if (run_com)
        source_global(here::here("scripts", "ml", "xx_costar_to_panel.R"))
      source_global(here::here("scripts", "ml", "xx_nwmls_condo_to_panel.R"))
      cache_save("panel_tbl_res")
      cache_save("panel_tbl_com")
      cache_save("panel_tbl_condo")
      cache_save("panel_tbl_all")
    } else if (run_com) {
      source_global(here::here("scripts", "ml", "xx_combine_res_comm_panel.R"))
      source_global(here::here("scripts", "ml", "xx_costar_to_panel.R"))
      cache_save("panel_tbl_res")
      cache_save("panel_tbl_com")
      cache_save("panel_tbl_all")
    } else {
      if (exists("panel_tbl", envir = .GlobalEnv)) {
        assign("panel_tbl_res", get("panel_tbl", envir=.GlobalEnv), envir = .GlobalEnv)
        cache_save("panel_tbl_res")
      } else if (exists("panel_tbl_res", envir = .GlobalEnv)) {
        cache_save("panel_tbl_res")
      } else {
        stop("Panel assembly did not create panel_tbl or panel_tbl_res.")
      }
    }

    # Raw parcel tables and panel-assembly intermediates fully consumed above.
    drop_if_exists(
      "parcel_res_full", "parcel_comm_full", "parcel_condo_full",
      "condo_complex_cln", "changes_long_tbl", "av_history_cln",
      "chg_dt", "av_dt", "panel_tbl", "panel_tbl_all"
    )
    # panel_tbl_com/condo already cached to disk — drop so they do not sit
    # idle (~2 GB) during the residential retrofit. Reloaded before Steps 4b/4c.
    drop_if_exists("panel_tbl_com", "panel_tbl_condo")
    gc(verbose = FALSE)

  } else {
    # --------------------------------------------------------------------------
    # panel_replicate = FALSE: load pre-built panels from cache
    # --------------------------------------------------------------------------
    message("  panel_replicate=FALSE — loading panels from cache ...")

    # Always load residential panel
    if (run_res) {
      if (!exists("panel_tbl_res", envir = .GlobalEnv)) cache_load("panel_tbl_res")
    }
    if (run_com) {
      if (!exists("panel_tbl_com", envir = .GlobalEnv)) cache_load("panel_tbl_com")
    }
    if (run_condo) {
      if (!exists("panel_tbl_condo", envir = .GlobalEnv)) cache_load("panel_tbl_condo")
    }

    # Drop any ETL intermediates that may have been loaded in Step 1
    drop_if_exists(
      "parcel_res_full", "parcel_comm_full", "parcel_condo_full",
      "condo_complex_cln", "changes_long_tbl", "av_history_cln",
      "chg_dt", "av_dt", "panel_tbl", "panel_tbl_all"
    )
    # Drop com/condo panels — already cached; not needed until Steps 4b/4c
    drop_if_exists("panel_tbl_com", "panel_tbl_condo")
    gc(verbose = FALSE)
  }

  } # end !forecast_only (Step 2)

  # ============================================================================
  # STEP 3  —  Model Training or Loading
  # (runs regardless of forecast_only — models always needed for Step 6)
  # ============================================================================
  message("\n--- Step 3: Models ---")
  
  # --------------------------------------------------------------------------
  # 3a. RESIDENTIAL  —  03_model_land.R  +  03_model_impr.R
  # --------------------------------------------------------------------------
  if (run_res) {
    message("\n  [Residential models]")
    if (model_replicate) {
      message("  Training residential LightGBM models ...")
      cl <- init_parallel()
      on.exit({
        try(stopCluster(cl), silent = TRUE)
        try(registerDoSEQ(), silent = TRUE)
      }, add = TRUE)
      source_global(here::here("scripts", "ml", "03_model_land.R"))
      source_global(here::here("scripts", "ml", "03_model_impr.R"))
      message("  \u2705 Residential models trained and saved.")
      
      # Training frames written to cache_dir by the model scripts; drop copies
      drop_if_exists(
        "model_data_land_delta_model",
        "model_data_land_model",
        "model_data_impr_delta_model",
        "model_data_impr_level_model"
      )
      
    } else {
      message("  Loading cached residential models ...")
      load_model("lgb_land_delta_cv", "lgb_land_delta_cv")
      load_model("dv_land_delta", "dv_land_delta")
      load_model("lgb_impr_delta_cv", "lgb_impr_delta_cv")
      load_model("dv_impr_delta", "dv_impr_delta")
      load_model("lgb_impr_level_cv", "lgb_impr_level_cv")
      load_model("dv_impr_level", "dv_impr_level")
      load_model("lgb_land_level_cv", "lgb_land_level_cv", required = FALSE)
      load_model("dv_land_level", "dv_land_level", required = FALSE)
    }
    
    expose_cv("lgb_land_delta_cv",
              "lgb_land_delta_model",
              "lgb_land_delta_features")
    expose_cv("lgb_impr_delta_cv",
              "lgb_impr_delta_model",
              "lgb_impr_delta_features")
    expose_cv("lgb_impr_level_cv",
              "lgb_impr_level_model",
              "lgb_impr_level_features")
    expose_cv("lgb_land_level_cv",
              "lgb_land_level_model",
              "lgb_land_level_features")
    
    load_train_frame("model_data_land_delta_model",
                     "model_data_land_delta_model.rds")
    load_train_frame("model_data_land_model", "model_data_land_model.rds")
    load_train_frame("model_data_impr_delta_model",
                     "model_data_impr_delta_model.rds")
    load_train_frame("model_data_impr_level_model",
                     "model_data_impr_level_model.rds")
  }
  
  # --------------------------------------------------------------------------
  # 3b. COMMERCIAL  —  03_model_comm_land.R  +  03_model_comm_impr.R
  #
  # Both scripts use make_rolling_year_folds() for chronological CV.
  #   _land  →  log(appr_land_val)   delta (primary) + level (fallback)
  #   _impr  →  log(appr_imps_val)   delta (primary) + level (fallback)
  # --------------------------------------------------------------------------
  if (run_com) {
    message("\n  [Commercial models]")
    if (model_replicate) {
      message("  Training commercial LightGBM models (land + impr) ...")
      source_global(here::here("scripts", "ml", "03_model_comm_land.R"))
      source_global(here::here("scripts", "ml", "03_model_comm_impr.R"))
      message("  \u2705 Commercial models trained and saved.")
      # Expose aliases used by 06_forecast_av_2026_2031_sequential_comm.R
      # (forecast script predates land/impr split; expects short model names)
      if (exists("lgb_com_land_delta_cv", envir = .GlobalEnv))
        assign("lgb_com_delta_cv", get("lgb_com_land_delta_cv", envir = .GlobalEnv), envir = .GlobalEnv)
      if (exists("lgb_com_land_level_cv", envir = .GlobalEnv))
        assign("lgb_com_level_cv", get("lgb_com_land_level_cv", envir = .GlobalEnv), envir = .GlobalEnv)
      
      drop_if_exists(
        "model_data_comm_land_delta",
        "model_data_comm_land_level",
        "model_data_comm_impr_delta",
        "model_data_comm_impr_level"
      )
      
    } else {
      message("  Loading cached commercial models ...")
      # Land
      load_model("lgb_com_land_delta_cv", "lgb_comm_land_delta_cv")
      load_model("dv_com_land_delta", "dv_comm_land_delta")
      load_model("lgb_com_land_level_cv",
                 "lgb_comm_land_level_cv",
                 required = FALSE)
      load_model("dv_com_land_level", "dv_comm_land_level", required = FALSE)
      # Improvement
      load_model("lgb_com_impr_delta_cv", "lgb_comm_impr_delta_cv")
      load_model("dv_com_impr_delta", "dv_comm_impr_delta")
      load_model("lgb_com_impr_level_cv",
                 "lgb_comm_impr_level_cv",
                 required = FALSE)
      load_model("dv_com_impr_level", "dv_comm_impr_level", required = FALSE)
    }
    
    expose_cv(
      "lgb_com_land_delta_cv",
      "lgb_com_land_delta_model",
      "lgb_com_land_delta_features"
    )
    expose_cv(
      "lgb_com_land_level_cv",
      "lgb_com_land_level_model",
      "lgb_com_land_level_features"
    )
    expose_cv(
      "lgb_com_impr_delta_cv",
      "lgb_com_impr_delta_model",
      "lgb_com_impr_delta_features"
    )
    expose_cv(
      "lgb_com_impr_level_cv",
      "lgb_com_impr_level_model",
      "lgb_com_impr_level_features"
    )
    
    load_train_frame("model_data_comm_land_delta",
                     "model_data_comm_land_delta.rds")
    load_train_frame("model_data_comm_land_level",
                     "model_data_comm_land_level.rds")
    load_train_frame("model_data_comm_impr_delta",
                     "model_data_comm_impr_delta.rds")
    load_train_frame("model_data_comm_impr_level",
                     "model_data_comm_impr_level.rds")

    # Short-name aliases for 06_forecast_av_2026_2031_sequential_comm.R
    if (exists("lgb_com_land_delta_cv", envir = .GlobalEnv))
      assign("lgb_com_delta_cv", get("lgb_com_land_delta_cv", envir = .GlobalEnv), envir = .GlobalEnv)
    if (exists("lgb_com_land_level_cv", envir = .GlobalEnv))
      assign("lgb_com_level_cv", get("lgb_com_land_level_cv", envir = .GlobalEnv), envir = .GlobalEnv)
  }
  
  # --------------------------------------------------------------------------
  # 3c. CONDO  —  03_model_condo_land.R  +  03_model_condo_impr.R
  #
  # Both scripts use make_rolling_year_folds() for chronological CV.
  #   _land  →  log(appr_land_val) per unit   delta (primary) + level (fallback)
  #   _impr  →  log(appr_imps_val) per unit   delta (primary) + level (fallback)
  # --------------------------------------------------------------------------
  if (run_condo) {
    message("\n  [Condo models]")
    if (model_replicate) {
      message("  Training condo LightGBM models (land + impr) ...")
      source_global(here::here("scripts", "ml", "03_model_condo_land.R"))
      source_global(here::here("scripts", "ml", "03_model_condo_impr.R"))
      message("  \u2705 Condo models trained and saved.")
      # Expose aliases used by 06_forecast_av_2026_2031_sequential_condo.R
      if (exists("lgb_condo_land_delta_cv", envir = .GlobalEnv))
        assign("lgb_condo_delta_cv", get("lgb_condo_land_delta_cv", envir = .GlobalEnv), envir = .GlobalEnv)
      if (exists("lgb_condo_land_level_cv", envir = .GlobalEnv))
        assign("lgb_condo_level_cv", get("lgb_condo_land_level_cv", envir = .GlobalEnv), envir = .GlobalEnv)
      
      drop_if_exists(
        "model_data_condo_land_delta",
        "model_data_condo_land_level",
        "model_data_condo_impr_delta",
        "model_data_condo_impr_level"
      )
      
    } else {
      message("  Loading cached condo models ...")
      # Land
      load_model("lgb_condo_land_delta_cv", "lgb_condo_land_delta_cv")
      load_model("dv_condo_land_delta", "dv_condo_land_delta")
      load_model("lgb_condo_land_level_cv",
                 "lgb_condo_land_level_cv",
                 required = FALSE)
      load_model("dv_condo_land_level",
                 "dv_condo_land_level",
                 required = FALSE)
      # Improvement
      load_model("lgb_condo_impr_delta_cv", "lgb_condo_impr_delta_cv")
      load_model("dv_condo_impr_delta", "dv_condo_impr_delta")
      load_model("lgb_condo_impr_level_cv",
                 "lgb_condo_impr_level_cv",
                 required = FALSE)
      load_model("dv_condo_impr_level",
                 "dv_condo_impr_level",
                 required = FALSE)
    }
    
    expose_cv(
      "lgb_condo_land_delta_cv",
      "lgb_condo_land_delta_model",
      "lgb_condo_land_delta_features"
    )
    expose_cv(
      "lgb_condo_land_level_cv",
      "lgb_condo_land_level_model",
      "lgb_condo_land_level_features"
    )
    expose_cv(
      "lgb_condo_impr_delta_cv",
      "lgb_condo_impr_delta_model",
      "lgb_condo_impr_delta_features"
    )
    expose_cv(
      "lgb_condo_impr_level_cv",
      "lgb_condo_impr_level_model",
      "lgb_condo_impr_level_features"
    )
    
    load_train_frame("model_data_condo_land_delta",
                     "model_data_condo_land_delta.rds")
    load_train_frame("model_data_condo_land_level",
                     "model_data_condo_land_level.rds")
    load_train_frame("model_data_condo_impr_delta",
                     "model_data_condo_impr_delta.rds")
    load_train_frame("model_data_condo_impr_level",
                     "model_data_condo_impr_level.rds")

    # Short-name aliases for 06_forecast_av_2026_2031_sequential_condo.R
    if (exists("lgb_condo_land_delta_cv", envir = .GlobalEnv))
      assign("lgb_condo_delta_cv", get("lgb_condo_land_delta_cv", envir = .GlobalEnv), envir = .GlobalEnv)
    if (exists("lgb_condo_land_level_cv", envir = .GlobalEnv))
      assign("lgb_condo_level_cv", get("lgb_condo_land_level_cv", envir = .GlobalEnv), envir = .GlobalEnv)
  }
  
  # panel_tbl (res backbone created by xx_combine_parcel*) no longer needed
  drop_if_exists("panel_tbl")

  if (!forecast_only) {

  # ============================================================================
  # STEP 4  —  Retrofit Historical AV
  # ============================================================================
  
  # --------------------------------------------------------------------------
  # 4a. Residential  (full residual-correction retrofit via 04_retrofitting_values.R)
  # --------------------------------------------------------------------------
  if (run_res) {
    message("\n--- Step 4a: Retrofit historical AV (residential) ---")
    retro_cache_res <- file.path(cache_dir, "panel_tbl_retro_res.rds")
    
    if (retrofit_replicate || !file.exists(retro_cache_res)) {
      assign("panel_tbl", panel_tbl_res, envir = .GlobalEnv)
      # Drop panel_tbl_res immediately — retrofit only needs panel_tbl.
      # Keeping both alive simultaneously adds ~2.5 GB at peak.
      drop_if_exists("panel_tbl_res")
      source_global(here::here("scripts", "ml", "04_retrofitting_values.R"))
      if (exists("panel_tbl_retro", envir = .GlobalEnv)) {
        assign("panel_tbl_retro_res", panel_tbl_retro, envir = .GlobalEnv)
        saveRDS(panel_tbl_retro_res, retro_cache_res)
        message("  \U1f4be cached: panel_tbl_retro_res")
      }
    } else {
      message("  retrofit_replicate=FALSE and cache exists — loading.")
      assign("panel_tbl_retro_res",
             readRDS(retro_cache_res),
             envir = .GlobalEnv)
      message("  \u2705 loaded: panel_tbl_retro_res")
    }
    
    # panel_tbl_res and the intermediate panel_tbl_retro are now superseded
    # by panel_tbl_retro_res.
    drop_if_exists("panel_tbl_res", "panel_tbl_retro", "panel_tbl")
  }
  
  # --------------------------------------------------------------------------
  # 4b. Commercial  (forward/backward na.locf fill)
  # --------------------------------------------------------------------------
  if (run_com) {
    message("\n--- Step 4b: Retrofit historical AV (commercial) ---")
    retro_cache_com <- file.path(cache_dir, "panel_tbl_retro_com.rds")
    # Reload panel_tbl_com from cache (was dropped after Step 2 to save memory)
    if (!exists("panel_tbl_com", envir = .GlobalEnv))
      cache_load("panel_tbl_com")
    
    if (retrofit_replicate || !file.exists(retro_cache_com)) {
      panel_retro_com <- copy(panel_tbl_com)
      setDT(panel_retro_com)
      setkeyv(panel_retro_com, c("parcel_id", "tax_yr"))

      # If log AV columns are all-NA (dash-format parcel_id mismatch in panel build),
      # re-join from av_history_cln with normalized IDs before locf fills.
      if ("log_appr_land_val" %in% names(panel_retro_com) &&
          all(is.na(panel_retro_com$log_appr_land_val))) {
        message("  log_appr_land_val all-NA in commercial retro panel — re-joining AV ...")
        av_cache_path <- file.path(cache_dir, "av_history_cln.rds")
        if (!exists("av_history_cln", envir = .GlobalEnv) && file.exists(av_cache_path))
          assign("av_history_cln", readRDS(av_cache_path), envir = .GlobalEnv)
        if (exists("av_history_cln", envir = .GlobalEnv)) {
          av_fix <- data.table::as.data.table(av_history_cln)
          av_fix[, parcel_id := gsub("-", "", parcel_id)]
          av_fix <- av_fix[parcel_id %in% unique(panel_retro_com$parcel_id),
                           .(parcel_id, tax_yr, appr_land_val, appr_imps_val)]
          panel_retro_com[av_fix, on = .(parcel_id, tax_yr),
                          `:=`(appr_land_val = i.appr_land_val,
                               appr_imps_val = i.appr_imps_val)]
          panel_retro_com[appr_land_val > 0, log_appr_land_val := log(appr_land_val)]
          panel_retro_com[appr_imps_val > 0, log_appr_imps_val := log(appr_imps_val)]
          panel_retro_com[, total_assessed :=
            fifelse(is.na(appr_land_val), 0, appr_land_val) +
            fifelse(is.na(appr_imps_val), 0, appr_imps_val)]
          panel_retro_com[total_assessed > 0, log_total_assessed := log(total_assessed)]
          rm(av_fix)
          message("    AV re-join complete for commercial retro panel.")
        }
        # av_history_cln is on disk — drop from memory now it is consumed
        drop_if_exists("av_history_cln")
      }

      for (col in c("log_appr_land_val",
                    "log_appr_imps_val",
                    "log_total_assessed")) {
        if (col %in% names(panel_retro_com))
          panel_retro_com[, (col) := zoo::na.locf(zoo::na.locf(get(col), na.rm = FALSE),
                                                  fromLast = TRUE,
                                                  na.rm = FALSE), by = parcel_id]
      }
      
      assign("panel_tbl_retro_com", panel_retro_com, envir = .GlobalEnv)
      saveRDS(panel_retro_com, retro_cache_com)
      message("  \U1f4be cached: panel_tbl_retro_com")
      rm(panel_retro_com)
      
    } else {
      message("  retrofit_replicate=FALSE and cache exists — loading.")
      assign("panel_tbl_retro_com",
             readRDS(retro_cache_com),
             envir = .GlobalEnv)
      message("  \u2705 loaded: panel_tbl_retro_com")
    }
    
    drop_if_exists("panel_tbl_com")
  }
  
  # --------------------------------------------------------------------------
  # 4c. Condo  (forward/backward na.locf fill)
  # --------------------------------------------------------------------------
  if (run_condo) {
    message("\n--- Step 4c: Retrofit historical AV (condo) ---")
    retro_cache_condo <- file.path(cache_dir, "panel_tbl_retro_condo.rds")
    # Reload panel_tbl_condo from cache (was dropped after Step 2 to save memory)
    if (!exists("panel_tbl_condo", envir = .GlobalEnv))
      cache_load("panel_tbl_condo")
    
    if (retrofit_replicate || !file.exists(retro_cache_condo)) {
      panel_retro_condo <- copy(panel_tbl_condo)
      setDT(panel_retro_condo)
      setkeyv(panel_retro_condo, c("parcel_id", "tax_yr"))

      # Re-join AV if all-NA (dash-format mismatch in panel build)
      if ("log_appr_land_val" %in% names(panel_retro_condo) &&
          all(is.na(panel_retro_condo$log_appr_land_val))) {
        message("  log_appr_land_val all-NA in condo retro panel — re-joining AV ...")
        av_cache_path <- file.path(cache_dir, "av_history_cln.rds")
        if (!exists("av_history_cln", envir = .GlobalEnv) && file.exists(av_cache_path))
          assign("av_history_cln", readRDS(av_cache_path), envir = .GlobalEnv)
        if (exists("av_history_cln", envir = .GlobalEnv)) {
          av_fix <- data.table::as.data.table(av_history_cln)
          av_fix[, parcel_id := gsub("-", "", parcel_id)]
          av_fix <- av_fix[parcel_id %in% unique(panel_retro_condo$parcel_id),
                           .(parcel_id, tax_yr, appr_land_val, appr_imps_val)]
          panel_retro_condo[av_fix, on = .(parcel_id, tax_yr),
                            `:=`(appr_land_val = i.appr_land_val,
                                 appr_imps_val = i.appr_imps_val)]
          panel_retro_condo[appr_land_val > 0, log_appr_land_val := log(appr_land_val)]
          panel_retro_condo[appr_imps_val > 0, log_appr_imps_val := log(appr_imps_val)]
          panel_retro_condo[, total_assessed :=
            fifelse(is.na(appr_land_val), 0, appr_land_val) +
            fifelse(is.na(appr_imps_val), 0, appr_imps_val)]
          panel_retro_condo[total_assessed > 0, log_total_assessed := log(total_assessed)]
          rm(av_fix)
          message("    AV re-join complete for condo retro panel.")
        }
        # av_history_cln is on disk — drop from memory now it is consumed
        drop_if_exists("av_history_cln")
      }

      for (col in c("log_appr_land_val",
                    "log_appr_imps_val",
                    "log_total_assessed")) {
        if (col %in% names(panel_retro_condo))
          panel_retro_condo[, (col) := zoo::na.locf(zoo::na.locf(get(col), na.rm = FALSE),
                                                    fromLast = TRUE,
                                                    na.rm = FALSE), by = parcel_id]
      }
      
      assign("panel_tbl_retro_condo", panel_retro_condo, envir = .GlobalEnv)
      saveRDS(panel_retro_condo, retro_cache_condo)
      message("  \U1f4be cached: panel_tbl_retro_condo")
      rm(panel_retro_condo)
      
    } else {
      assign("panel_tbl_retro_condo",
             readRDS(retro_cache_condo),
             envir = .GlobalEnv)
      message("  \u2705 loaded: panel_tbl_retro_condo")
    }
    
    drop_if_exists("panel_tbl_condo")
  }
  
  # panel_tbl_retro_com and panel_tbl_retro_condo are already cached to disk.
  # Drop them now so they do not consume ~2 GB during Step 5a eval and the
  # residential extend (Step 5b). They are reloaded just before their
  # respective extend scripts.
  drop_if_exists("panel_tbl_retro_com", "panel_tbl_retro_condo")
  gc(verbose = FALSE)
  
  # ============================================================================
  # STEP 5a  —  Holdout Diagnostics
  # ============================================================================
  message("\n--- Step 5a: Holdout diagnostics ---")
  eval_script <- here::here("scripts", "ml", "05_eval_holdout_2025.R")

  if (!file.exists(eval_script)) {
    message("  05_eval_holdout_2025.R not found — skipping.")
  } else if (!diagnostics_replicate) {
    # Cache-first: load pre-computed metrics without running the eval script
    out_dir_eval <- file.path(output_dir, "eval_2025")
    metrics_land_path <- file.path(out_dir_eval,
      paste0("metrics_land_2025_", kca_date_data_extracted, ".csv"))
    if (file.exists(metrics_land_path)) {
      message("  diagnostics_replicate=FALSE — loading cached eval metrics ...")
      metrics_files <- list(
        file.path(out_dir_eval, paste0("metrics_land_2025_",       kca_date_data_extracted, ".csv")),
        file.path(out_dir_eval, paste0("metrics_impr_2025_",       kca_date_data_extracted, ".csv")),
        file.path(out_dir_eval, paste0("metrics_com_land_2025_",   kca_date_data_extracted, ".csv")),
        file.path(out_dir_eval, paste0("metrics_com_impr_2025_",   kca_date_data_extracted, ".csv")),
        file.path(out_dir_eval, paste0("metrics_condo_land_2025_", kca_date_data_extracted, ".csv")),
        file.path(out_dir_eval, paste0("metrics_condo_impr_2025_", kca_date_data_extracted, ".csv"))
      )
      metrics_all <- dplyr::bind_rows(
        lapply(metrics_files[sapply(metrics_files, file.exists)],
               function(p) readr::read_csv(p, show_col_types = FALSE))
      )
      assign("metrics_all", metrics_all, envir = .GlobalEnv)
      message("  === 2025 Holdout Eval — All Tracks (cached) ===")
      print(metrics_all %>%
              dplyr::select(Track, Target, Model, RMSE_log, MAE_log, MAPE, WAPE) %>%
              dplyr::arrange(Track, Target))
    } else {
      message("  No cached eval metrics found — run with diagnostics_replicate=TRUE to generate.")
    }
  } else if (run_res) {
    assign("panel_tbl", panel_tbl_retro_res, envir = .GlobalEnv)
    source_global(eval_script)
  }

  } # end !forecast_only (Steps 4 + 5a)

  gc(verbose = FALSE)

  # ============================================================================
  # STEP 5b  —  Extend Panel 2026-2031  (or load cached extended panels)
  # ============================================================================
  # When forecast_only=TRUE: skip extend scripts, load cached extended panels.
  # When forecast_only=FALSE: run extend scripts (respecting extend_replicate).
  # ============================================================================
  message(paste0("\n--- Step 5b: Extend panel ", forecast_start, "-", forecast_end, " (scenario = ", scenario, ") ---"))

  if (forecast_only) {
    # --------------------------------------------------------------------------
    # forecast_only = TRUE: load cached extended panels directly
    # --------------------------------------------------------------------------
    message("  forecast_only=TRUE — loading cached extended panels ...")

    load_ext <- function(track_label, obj_name, cache_name) {
      if (exists(obj_name, envir = .GlobalEnv)) {
        message("  \u2705 already in memory: ", obj_name)
        return(invisible(NULL))
      }
      p <- file.path(cache_dir, cache_name)
      if (!file.exists(p))
        stop("forecast_only=TRUE but extended panel cache not found: ", p,
             "\nRun with extend_replicate=TRUE once to build it.")
      assign(obj_name, readRDS(p), envir = .GlobalEnv)
      message("  \u2705 loaded extended panel (", track_label, "): ", cache_name)
    }

    if (run_res)
      load_ext("res",
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_res"),
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_res.rds"))
    if (run_com)
      load_ext("com",
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com"),
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com.rds"))
    if (run_condo)
      load_ext("condo",
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_condo"),
               paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_condo.rds"))

  } else {
    # --------------------------------------------------------------------------
    # forecast_only = FALSE: run extend scripts (normal path)
    # --------------------------------------------------------------------------

    # Residential extend
    if (run_res) {
      ext_cache_res <- file.path(cache_dir,
                                 paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_res.rds"))
      if (extend_replicate || !file.exists(ext_cache_res)) {
        assign("panel_tbl_retro", panel_tbl_retro_res, envir = .GlobalEnv)
        source_global(here::here("scripts", "ml", "05_extend_panel_2026_2031.R"))
        ext_obj <- paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario)
        if (exists(ext_obj, envir = .GlobalEnv)) {
          panel_ext_res <- get(ext_obj, envir = .GlobalEnv)
          saveRDS(panel_ext_res, ext_cache_res)
          assign(paste0(ext_obj, "_res"), panel_ext_res, envir = .GlobalEnv)
          message("  \U1f4be cached: ", basename(ext_cache_res))
          rm(panel_ext_res)
          drop_if_exists(ext_obj)
        }
      } else {
        message("  Res extend cache exists — skipping.")
        message("  \u2705 using: ", basename(ext_cache_res))
      }
      drop_if_exists("panel_tbl_retro_res", "panel_tbl_retro")
    }

    # Commercial extend
    if (run_com) {
      ext_cache_com <- file.path(cache_dir,
                                 paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com.rds"))
      if (extend_replicate || !file.exists(ext_cache_com)) {
        # Reload retro_com from cache (was dropped after Step 4 to save memory)
        if (!exists("panel_tbl_retro_com", envir = .GlobalEnv))
          assign("panel_tbl_retro_com", readRDS(file.path(cache_dir, "panel_tbl_retro_com.rds")), envir = .GlobalEnv)
        assign("panel_tbl_retro", panel_tbl_retro_com, envir = .GlobalEnv)
        source_global(here::here("scripts", "ml", "05_extend_panel_2026_2031_comm.R"))
        ext_obj_com <- paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com")
        if (exists(ext_obj_com, envir = .GlobalEnv)) {
          saveRDS(get(ext_obj_com, envir = .GlobalEnv), ext_cache_com)
          message("  \U1f4be cached: ", basename(ext_cache_com))
        }
      } else {
        message("  Com extend cache exists — skipping.")
        message("  \u2705 using: ", basename(ext_cache_com))
      }
      drop_if_exists("panel_tbl_retro_com", "panel_tbl_retro")
    }

    # Free the commercial extended panel before loading condo retro panel —
    # keeping both alive simultaneously causes OOM on memory-constrained runs.
    # The commercial extended panel is already cached to disk above and will be
    # reloaded in Step 6 (forecast) via load_ext or the forecast script itself.
    if (run_com && run_condo) {
      drop_if_exists(paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com"))
      gc(verbose = FALSE)
    }

    # Condo extend
    if (run_condo) {
      ext_cache_condo <- file.path(cache_dir,
                                   paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_condo.rds"))
      if (extend_replicate || !file.exists(ext_cache_condo)) {
        # Reload retro_condo from cache (was dropped after Step 4 to save memory)
        if (!exists("panel_tbl_retro_condo", envir = .GlobalEnv))
          assign("panel_tbl_retro_condo", readRDS(file.path(cache_dir, "panel_tbl_retro_condo.rds")), envir = .GlobalEnv)
        assign("panel_tbl_retro", panel_tbl_retro_condo, envir = .GlobalEnv)
        source_global(here::here("scripts", "ml", "05_extend_panel_2026_2031_condo.R"))
        ext_obj_condo <- paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_condo")
        if (exists(ext_obj_condo, envir = .GlobalEnv)) {
          saveRDS(get(ext_obj_condo, envir = .GlobalEnv), ext_cache_condo)
          message("  \U1f4be cached: ", basename(ext_cache_condo))
        }
      } else {
        message("  Condo extend cache exists — skipping.")
      }
      drop_if_exists("panel_tbl_retro_condo", "panel_tbl_retro")
    }

  } # end forecast_only / normal extend branch

  gc(verbose = FALSE)

  # ============================================================================
  # STEP 6  —  Sequential Forecast 2026-2031
  # ============================================================================
  message(paste0("\n--- Step 6: Sequential forecast ", forecast_start, "-", forecast_end, " ---"))
  
  if (run_res) {
    message("  [Residential forecast]")
    source_global(here::here("scripts", "ml", "06_forecast_av_2026_2031_sequential.R"))
  }
  
  if (run_com) {
    # Reload commercial extended panel if it was dropped between Steps 5b com/condo
    ext_obj_com_name <- paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com")
    if (!exists(ext_obj_com_name, envir = .GlobalEnv)) {
      ext_com_path <- file.path(cache_dir, paste0(ext_obj_com_name, ".rds"))
      if (file.exists(ext_com_path)) {
        assign(ext_obj_com_name, readRDS(ext_com_path), envir = .GlobalEnv)
        message("  \u2705 reloaded: ", ext_obj_com_name)
      }
    }
    message("  [Commercial forecast]")
    source_global(here::here(
      "scripts",
      "ml",
      "06_forecast_av_2026_2031_sequential_comm.R"
    ))
  }
  
  if (run_condo) {
    message("  [Condo forecast]")
    source_global(here::here(
      "scripts",
      "ml",
      "06_forecast_av_2026_2031_sequential_condo.R"
    ))
  }
  
  # Extended input panels consumed by forecast scripts above
  drop_if_exists(
    paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_res"),
    paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_com"),
    paste0("panel_tbl_", forecast_start, "_", forecast_end, "_inputs_", scenario, "_condo")
  )
  
  # ---- Footer -----------------------------------------------------------------
  elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 2)
  message("\n==============================================")
  message("run_main_ml() finished — elapsed: ", elapsed, " min")
  message("  prop_scope = ", prop_scope, " | scenario = ", scenario)
  message("==============================================")
  
  invisible(
    list(
      panel_tbl_forecasted_res   = if (exists("panel_tbl_forecasted_res", envir = .GlobalEnv))
        get("panel_tbl_forecasted_res", envir = .GlobalEnv)
      else
        NULL,
      panel_tbl_forecasted_com   = if (exists("panel_tbl_forecasted_com", envir = .GlobalEnv))
        get("panel_tbl_forecasted_com", envir = .GlobalEnv)
      else
        NULL,
      panel_tbl_forecasted_condo = if (exists("panel_tbl_forecasted_condo", envir = .GlobalEnv))
        get("panel_tbl_forecasted_condo", envir = .GlobalEnv)
      else
        NULL,
      metrics_all                = if (exists("metrics_all", envir = .GlobalEnv))
        get("metrics_all", envir = .GlobalEnv)
      else
        NULL,
      rmse_land                  = if (exists("rmse_comparison_land", envir = .GlobalEnv))
        get("rmse_comparison_land", envir = .GlobalEnv)
      else
        NULL,
      rmse_impr                  = if (exists("rmse_comparison_impr", envir = .GlobalEnv))
        get("rmse_comparison_impr", envir = .GlobalEnv)
      else
        NULL
    )
  )
}
run_main_ml()

# =============================================================================
# prep_scenario_caches()
# =============================================================================
# Generates the scenario-specific econ and NWMLS forecast cache files needed
# by the extend scripts (05_extend_panel_2026_2031*.R) without rebuilding the
# full panel.  Run this once per new scenario before calling run_main_ml() with
# that scenario and extend_replicate=TRUE.
#
# Requirements:
#   - panel_tbl_res.rds must exist in cache_dir  (panel_replicate=TRUE once)
#   - The NWMLS Excel file for the scenario must be in data/nwmls/
#   - The OERF econ forecast CSV for the scenario must be in data/econ/
#
# Usage:
#   prep_scenario_caches("optimistic")
#   prep_scenario_caches("pessimistic")
#   # Then run the forecast:
#   run_main_ml(scenario="optimistic", extend_replicate=TRUE, forecast_only=FALSE, ...)
# =============================================================================
prep_scenario_caches <- function(
    scenario,
    kca_date_data_extracted = if (exists("CFG", envir = .GlobalEnv))
                                CFG$kca_date_data_extracted
                              else
                                get0("kca_date_data_extracted", envir = .GlobalEnv,
                                     ifnotfound = stop(
                                       "kca_date_data_extracted not found. ",
                                       "Either source main_ml.R first or pass it explicitly:\n",
                                       "  prep_scenario_caches('optimistic', kca_date_data_extracted='2026-03-27')"
                                     )),
    cache_dir  = if (exists("CFG", envir = .GlobalEnv)) CFG$cache_dir
                 else here::here("data", "cache"),
    model_dir  = if (exists("CFG", envir = .GlobalEnv)) CFG$model_dir
                 else here::here("data", "model"),
    output_dir = if (exists("CFG", envir = .GlobalEnv)) CFG$output_dir
                 else here::here("data", "outputs")
) {
  valid_scenarios <- c("baseline", "optimistic", "pessimistic")
  if (!scenario %in% valid_scenarios)
    stop("scenario must be one of: ", paste(valid_scenarios, collapse = ", "))

  message("==============================================")
  message("prep_scenario_caches() — scenario: ", scenario)
  message("==============================================")

  source_global <- function(path) source(path, local = .GlobalEnv)

  # Push all required globals for sourced scripts before calling 00_init.R
  assign("scenario",               scenario,               envir = .GlobalEnv)
  assign("kca_date_data_extracted", kca_date_data_extracted, envir = .GlobalEnv)
  assign("cache_dir",              cache_dir,              envir = .GlobalEnv)
  assign("model_dir",              model_dir,              envir = .GlobalEnv)
  assign("output_dir",             output_dir,             envir = .GlobalEnv)

  source_global(here::here("scripts", "ml", "00_init.R"))

  # ---- Check whether caches already exist ----------------------------------
  econ_path  <- file.path(cache_dir,
                          paste0("econ_fcst_2026_2031_",  scenario, ".rds"))
  nwmls_path <- file.path(cache_dir,
                          paste0("nwmls_fcst_2026_2031_", scenario, ".rds"))
  nwmls_condo_path <- file.path(cache_dir,
                          paste0("nwmls_condo_fcst_2026_2031_", scenario, ".rds"))

  if (file.exists(econ_path) && file.exists(nwmls_path) &&
      file.exists(nwmls_condo_path)) {
    message("  All scenario caches already exist — nothing to do.")
    message("  econ  : ", basename(econ_path))
    message("  nwmls : ", basename(nwmls_path))
    message("  condo : ", basename(nwmls_condo_path))
    return(invisible(NULL))
  }

  # ---- Load residential panel from cache (needed by join scripts) ----------
  panel_cache <- file.path(cache_dir, "panel_tbl_res.rds")
  if (!file.exists(panel_cache))
    stop("panel_tbl_res.rds not found in cache_dir.\n",
         "Run run_main_ml(panel_replicate=TRUE) once first.")

  if (!exists("panel_tbl", envir = .GlobalEnv)) {
    message("  Loading panel_tbl_res from cache ...")
    assign("panel_tbl", readRDS(panel_cache), envir = .GlobalEnv)
    message("  \u2705 loaded panel_tbl_res")
    loaded_panel <- TRUE
  } else {
    loaded_panel <- FALSE
  }

  # ---- Run the forecast-input scripts --------------------------------------
  if (!file.exists(econ_path)) {
    message("  Running xx_econ_to_panel.R (", scenario, ") ...")
    source_global(here::here("scripts", "ml", "xx_econ_to_panel.R"))
  } else {
    message("  \u2705 econ cache exists: ", basename(econ_path))
  }

  if (!file.exists(nwmls_path)) {
    message("  Running xx_nwmls_to_panel.R (", scenario, ") ...")
    source_global(here::here("scripts", "ml", "xx_nwmls_to_panel.R"))
  } else {
    message("  \u2705 nwmls cache exists: ", basename(nwmls_path))
  }

  if (!file.exists(nwmls_condo_path)) {
    message("  Running xx_nwmls_condo_to_panel.R (", scenario, ") ...")
    nwmls_condo_script <- here::here("scripts", "ml", "xx_nwmls_condo_to_panel.R")
    if (file.exists(nwmls_condo_script)) {
      # xx_nwmls_condo_to_panel.R requires panel_tbl_condo — load if needed
      condo_cache <- file.path(cache_dir, "panel_tbl_condo.rds")
      loaded_condo <- FALSE
      if (!exists("panel_tbl_condo", envir = .GlobalEnv) && file.exists(condo_cache)) {
        message("  Loading panel_tbl_condo from cache ...")
        assign("panel_tbl_condo", readRDS(condo_cache), envir = .GlobalEnv)
        loaded_condo <- TRUE
      } else if (!exists("panel_tbl_condo", envir = .GlobalEnv)) {
        message("  \u26a0\ufe0f  panel_tbl_condo not found — skipping condo NWMLS cache.")
        loaded_condo <- FALSE
        nwmls_condo_script <- NULL
      }
      if (!is.null(nwmls_condo_script)) {
        source_global(nwmls_condo_script)
        if (loaded_condo) {
          rm("panel_tbl_condo", envir = .GlobalEnv)
          gc(verbose = FALSE)
        }
      }
    } else {
      message("  \u2139\ufe0f  xx_nwmls_condo_to_panel.R not found — skipping condo NWMLS cache.")
    }
  } else {
    message("  \u2705 nwmls condo cache exists: ", basename(nwmls_condo_path))
  }

  # ---- Also cache CoStar for this scenario (commercial extend needs it) ----
  costar_path <- file.path(cache_dir,
                           paste0("costar_fcst_1990_2033_", scenario, ".rds"))
  if (!file.exists(costar_path)) {
    message("  Running xx_costar_to_panel.R (", scenario, ") ...")
    costar_script <- here::here("scripts", "ml", "xx_costar_to_panel.R")
    if (file.exists(costar_script)) {
      # xx_costar_to_panel.R requires panel_tbl_com — load if needed
      com_cache <- file.path(cache_dir, "panel_tbl_com.rds")
      loaded_com <- FALSE
      if (!exists("panel_tbl_com", envir = .GlobalEnv) && file.exists(com_cache)) {
        assign("panel_tbl_com", readRDS(com_cache), envir = .GlobalEnv)
        loaded_com <- TRUE
      }
      source_global(costar_script)
      if (loaded_com) {
        rm("panel_tbl_com", envir = .GlobalEnv)
        gc(verbose = FALSE)
      }
    } else {
      message("  \u2139\ufe0f  xx_costar_to_panel.R not found — skipping CoStar cache.")
    }
  } else {
    message("  \u2705 costar cache exists: ", basename(costar_path))
  }

  # ---- Clean up panel (only drop if we loaded it here) --------------------
  if (loaded_panel) {
    rm("panel_tbl", envir = .GlobalEnv)
    gc(verbose = FALSE)
    message("  \U1f9f9 dropped panel_tbl from memory")
  }

  message("\n\u2705 prep_scenario_caches() complete for: ", scenario)
  message("  You can now run:")
  message("    run_main_ml(scenario=\'", scenario,
          "\', extend_replicate=TRUE, forecast_only=FALSE, ...)")
  invisible(NULL)
}

#prep_scenario_caches("optimistic")
#prep_scenario_caches("baseline") 
#prep_scenario_caches("pessimistic")

#run_main_ml(scenario = "baseline", extend_replicate = TRUE) 
#run_main_ml(scenario = "optimistic", extend_replicate = TRUE)
#run_main_ml(scenario = "pessimistic", extend_replicate = TRUE)
# =============================================================================
# av_fcst_summary()
# =============================================================================
# Summarises total assessed value (AV) by year (2025–2031) across all three
# forecast scenarios (baseline, optimistic, pessimistic), reading from the
# cached forecast RDS files produced by the Step 6 forecast scripts.
#
# Arguments:
#   prop_scope  — which property types to include: "res", "com", "condo",
#                 "both" (res+com), or "all" (res+com+condo). Default: "all"
#   scenarios   — character vector of scenarios to include.
#                 Default: c("baseline", "optimistic", "pessimistic")
#   years       — integer vector of tax years to include. Default: 2025:2031
#   export_csv  — if TRUE, write the summary table to a CSV file. Default: FALSE
#   csv_path    — path for the CSV export. If NULL, auto-generates a name in
#                 output_dir.
#   cache_dir   — where the forecast RDS caches live. Default: CFG$cache_dir
#   output_dir  — where CSV exports are written. Default: CFG$output_dir
#   digits      — rounding digits for dollar amounts in display. Default: 0
#
# Returns: a tibble with columns:
#   tax_yr, <scenario_1>, <scenario_2>, ..., and optionally diff_opt_base,
#   diff_pes_base (absolute $ differences from baseline)
# =============================================================================
av_fcst_summary <- function(
    prop_scope  = "all",
    scenarios   = c("baseline", "optimistic", "pessimistic"),
    years       = if (exists("CFG", envir = .GlobalEnv))
                    (CFG$forecast_start - 1):CFG$forecast_end
                  else 2025:2031,
    export_csv  = TRUE,
    csv_path    = NULL,
    cache_dir   = if (exists("CFG", envir = .GlobalEnv)) CFG$cache_dir
                  else here::here("data", "cache"),
    output_dir  = if (exists("CFG", envir = .GlobalEnv)) CFG$output_dir
                  else here::here("data", "outputs"),
    digits      = 0
) {
  # ---- Validate inputs -------------------------------------------------------
  valid_scopes    <- c("res", "com", "condo", "both", "all")
  valid_scenarios <- c("baseline", "optimistic", "pessimistic")

  if (!prop_scope %in% valid_scopes)
    stop("prop_scope must be one of: ", paste(valid_scopes, collapse = ", "))
  if (!all(scenarios %in% valid_scenarios))
    stop("scenarios must be subset of: ", paste(valid_scenarios, collapse = ", "))

  run_res   <- prop_scope %in% c("res", "both", "all")
  run_com   <- prop_scope %in% c("com", "both", "all")
  run_condo <- prop_scope %in% c("condo", "all")

  tracks <- c(
    if (run_res)   "res",
    if (run_com)   "com",
    if (run_condo) "condo"
  )

  message("==============================================")
  message("av_fcst_summary()")
  message("  prop_scope : ", prop_scope,
          "  (", paste(tracks, collapse = "+"), ")")
  message("  scenarios  : ", paste(scenarios, collapse = ", "))
  message("  years      : ", min(years), "-", max(years))
  message("==============================================")

  # ---- Helper: read one RDS and extract total AV by year -------------------
  read_track_av <- function(scenario, track) {
    # Always read from the scenario-specific RDS cache — never use in-memory
    # objects, since the GlobalEnv forecasted panels reflect only the last
    # run_main_ml() scenario and would silently return wrong values for others.
    fname <- paste0("panel_tbl_2006_2031_forecasted_", scenario, "_", track, ".rds")
    fpath <- file.path(cache_dir, fname)

    if (!file.exists(fpath)) {
      return(NULL)  # caller handles missing gracefully
    }
    dt <- data.table::as.data.table(readRDS(fpath))

    # Filter to requested years
    dt <- dt[tax_yr %in% years]

    # ---- Identify the AV columns by track ----------------------------------
    if (track == "res") {
      # Residential: historical years have appr_land_val / appr_imps_val,
      # forecast years have appr_land_val_filled / appr_imps_val_filled.
      # Use filled where available, fall back to observed.
      if ("appr_land_val_filled" %in% names(dt)) {
        dt[, av_land := fifelse(!is.na(appr_land_val_filled),
                                appr_land_val_filled, appr_land_val)]
        dt[, av_imps := fifelse(!is.na(appr_imps_val_filled),
                                appr_imps_val_filled, appr_imps_val)]
      } else {
        dt[, av_land := appr_land_val]
        dt[, av_imps := appr_imps_val]
      }
      dt[, av_total := fifelse(!is.na(av_land), av_land, 0) +
                       fifelse(!is.na(av_imps), av_imps, 0)]

    } else {
      # Commercial / Condo: 2025 (seed year) uses appr_land_val/appr_imps_val;
      # 2026+ uses pred_appr_land_val / pred_appr_imps_val.
      if ("pred_total_assessed" %in% names(dt)) {
        # For forecast years use pred_total_assessed; for seed year use observed
        dt[, av_total := data.table::fcase(
          !is.na(pred_total_assessed) & pred_total_assessed > 0,
            as.numeric(pred_total_assessed),
          !is.na(appr_land_val) | !is.na(appr_imps_val),
            fifelse(!is.na(appr_land_val), appr_land_val, 0) +
            fifelse(!is.na(appr_imps_val), appr_imps_val, 0),
          default = NA_real_
        )]
      } else if (all(c("appr_land_val", "appr_imps_val") %in% names(dt))) {
        dt[, av_total := fifelse(!is.na(appr_land_val), appr_land_val, 0) +
                         fifelse(!is.na(appr_imps_val), appr_imps_val, 0)]
      } else {
        warning("Could not identify AV columns for track '", track,
                "' scenario '", scenario, "'")
        return(NULL)
      }
    }

    # Sum total AV by year
    dt[, .(av = sum(av_total, na.rm = TRUE)), by = tax_yr]
  }

  # ---- Loop over scenarios and tracks, build combined table ----------------
  results <- list()

  for (sc in scenarios) {
    message("  Reading scenario: ", sc)
    sc_av <- NULL

    for (tr in tracks) {
      tr_av <- read_track_av(sc, tr)
      if (is.null(tr_av)) {
        message("    \u26a0\ufe0f  ", sc, "/", tr,
                " — cache not found, skipping track.")
        next
      }
      message("    \u2705  ", sc, "/", tr,
              " — ", nrow(tr_av), " years loaded")

      sc_av <- if (is.null(sc_av)) {
        tr_av
      } else {
        merge(sc_av, tr_av, by = "tax_yr", all = TRUE)[
          , .(tax_yr, av = rowSums(cbind(av.x, av.y), na.rm = TRUE))
        ]
      }
    }

    if (!is.null(sc_av))
      results[[sc]] <- sc_av[order(tax_yr)][tax_yr %in% years]
  }

  if (length(results) == 0)
    stop("No forecast data found. Check cache_dir and run run_main_ml() first.")

  # ---- Pivot to wide: one column per scenario ------------------------------
  summary_tbl <- Reduce(
    function(a, b) merge(a, b, by = "tax_yr", all = TRUE),
    mapply(
      function(sc, dt) {
        setNames(dt, c("tax_yr", sc))
      },
      names(results), results,
      SIMPLIFY = FALSE
    )
  )
  summary_tbl <- data.table::as.data.table(summary_tbl)[order(tax_yr)]

  # ---- Compute differences from baseline if baseline present ---------------
  if ("baseline" %in% names(summary_tbl) && length(scenarios) > 1) {
    for (sc in setdiff(scenarios, "baseline")) {
      if (sc %in% names(summary_tbl)) {
        diff_col <- paste0("diff_", sc, "_vs_baseline")
        summary_tbl[, (diff_col) := get(sc) - baseline]
      }
    }
  }

  # ---- Convert to tibble for display ---------------------------------------
  out <- tibble::as_tibble(summary_tbl)

  # ---- Print formatted summary ---------------------------------------------
  message("\n=== AV Forecast Summary (", prop_scope, ") ===")
  av_cols <- intersect(scenarios, names(out))
  print_tbl <- out
  for (col in av_cols) {
    print_tbl[[col]] <- scales::dollar(round(out[[col]], digits),
                                        scale  = 1e-9,
                                        suffix = "B",
                                        prefix = "$")
  }
  diff_cols <- grep("^diff_", names(out), value = TRUE)
  for (col in diff_cols) {
    print_tbl[[col]] <- scales::dollar(round(out[[col]], digits),
                                        scale  = 1e-9,
                                        suffix = "B",
                                        prefix = "$")
  }
  print(print_tbl)

  # ---- Export to CSV if requested ------------------------------------------
  if (export_csv) {
    if (is.null(csv_path)) {
      dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
      csv_path <- file.path(
        output_dir,
        paste0("av_fcst_summary_", prop_scope, "_",
               format(Sys.Date(), "%Y%m%d"), ".csv")
      )
    }
    readr::write_csv(out, csv_path)
    message("\n\U1f4be exported: ", csv_path)
  }

  invisible(out)
}
source(here::here("scripts","ml","00_init.R"))
av_fcst_summary(prop_scope = "res")
av_fcst_summary(prop_scope = "condo")
av_fcst_summary(prop_scope = "com")
