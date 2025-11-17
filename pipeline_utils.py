"""
pipeline_utils.py

Utility helpers for the pipeline: normalization-accuracy checks, enhanced numeric inference,
categorical-vs-numeric fixes, and an LLM enrichment stub (local fallback if no API key).
"""
import os
import json
import logging
from difflib import SequenceMatcher
import re
import pandas as pd

logger = logging.getLogger(__name__)


def compute_normalization_accuracy(before_df, after_df, sample_rows=None):
    """
    Compute simple normalization accuracy metrics between `before_df` and `after_df`.
    - For text columns: average similarity before->after using SequenceMatcher
    - Percent of values changed
    Returns a dict of metrics.
    """
    metrics = {}
    if before_df is None or after_df is None:
        return metrics

    # Align columns intersection
    cols = [c for c in before_df.columns if c in after_df.columns]
    if sample_rows:
        before_df = before_df.head(sample_rows)
        after_df = after_df.head(sample_rows)

    total_cells = 0
    changed_cells = 0
    text_sim_sum = 0.0
    text_cells = 0

    for c in cols:
        bcol = before_df[c].astype(str).fillna("")
        acol = after_df[c].astype(str).fillna("")
        for b, a in zip(bcol.tolist(), acol.tolist()):
            total_cells += 1
            if b != a:
                changed_cells += 1
            # If either looks like text (non-numeric), compute similarity
            if not _looks_numeric(b) and not _looks_numeric(a):
                text_cells += 1
                try:
                    text_sim_sum += SequenceMatcher(None, b, a).ratio()
                except Exception:
                    pass

    metrics["total_cells"] = total_cells
    metrics["changed_cells"] = changed_cells
    metrics["percent_changed"] = (changed_cells / float(total_cells)) if total_cells else 0.0
    metrics["avg_text_similarity"] = (text_sim_sum / float(text_cells)) if text_cells else None
    return metrics


def _looks_numeric(s: str) -> bool:
    """Helper to quickly spot numeric-looking strings."""
    if s is None:
        return False
    s = str(s).strip()
    if s == "":
        return False
    # remove common thousands separators and currency symbols
    s2 = re.sub(r"[,$€£%()]", "", s)
    s2 = s2.replace(" ", "")
    try:
        float(s2)
        return True
    except Exception:
        return False


def infer_numeric_column_enhanced(series, threshold=0.7):
    """
    Enhanced numeric inference for a pandas Series. Tries to normalize values like
    '1,234', '$1,234.00', '(123)' and percents. Returns True if >= threshold of
    non-null values parse as numeric.
    """
    if series is None or series.empty:
        return False
    total = 0
    numeric = 0
    for v in series.dropna().head(2000):
        total += 1
        s = str(v).strip()
        # handle percent
        if s.endswith('%'):
            s = s[:-1]
        # handle parentheses negative numbers
        s = s.replace('(', '-').replace(')', '')
        # remove currency symbols and commas
        s2 = re.sub(r"[^0-9eE+\-\.\,]", "", s)
        s2 = s2.replace(',', '')
        if s2 == '':
            continue
        try:
            float(s2)
            numeric += 1
        except Exception:
            pass

    if total == 0:
        return False
    return (numeric / float(total)) >= float(threshold)


def enhance_numeric_inference(report_path_or_obj, input_path=None):
    """
    Post-pass helper that attempts to enhance numeric column detection using
    a sample. `report_path_or_obj` can be a path to JSON report or the report dict.

    This function writes an updated diagnostics file next to the report.
    """
    # Load report
    if isinstance(report_path_or_obj, str) and os.path.exists(report_path_or_obj):
        with open(report_path_or_obj, 'r', encoding='utf-8') as f:
            report = json.load(f)
    elif isinstance(report_path_or_obj, dict):
        report = report_path_or_obj
    else:
        raise ValueError("report_path_or_obj must be a path or dict")

    # If input_path provided, read a small sample
    sample_df = None
    if input_path and os.path.exists(input_path):
        try:
            sample_df = pd.read_csv(input_path, nrows=2000)
        except Exception:
            try:
                sample_df = pd.read_json(input_path, lines=True, nrows=2000)
            except Exception:
                sample_df = None

    numeric_candidates = {}
    if sample_df is not None:
        for c in sample_df.columns:
            try:
                ok = infer_numeric_column_enhanced(sample_df[c])
                numeric_candidates[c] = bool(ok)
            except Exception:
                numeric_candidates[c] = False

    diagnostics = {
        "numeric_candidates": numeric_candidates,
    }

    out_path = None
    if isinstance(report_path_or_obj, str):
        out_path = report_path_or_obj.replace('.json', '.numeric_diagnostics.json')
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(diagnostics, f, indent=2)
    else:
        out_path = None

    logger.info("Enhanced numeric inference done. Saved diagnostics to %s", out_path)
    return diagnostics


def fix_categorical_numeric_detection(report_path_or_obj, input_path=None, unique_ratio_threshold=0.05):
    """
    Attempts to fix categorical-vs-numeric detection by checking unique-count ratio
    on a sample and returning suggested overrides.
    """
    # Read sample
    sample_df = None
    if input_path and os.path.exists(input_path):
        try:
            sample_df = pd.read_csv(input_path, nrows=2000)
        except Exception:
            try:
                sample_df = pd.read_json(input_path, lines=True, nrows=2000)
            except Exception:
                sample_df = None

    suggestions = {}
    if sample_df is not None:
        nrows = float(len(sample_df)) if len(sample_df) else 1.0
        for c in sample_df.columns:
            try:
                uniq = sample_df[c].nunique(dropna=True)
                ratio = uniq / nrows
                # If small unique ratio -> likely categorical
                if ratio < unique_ratio_threshold:
                    suggestions[c] = "categorical"
                else:
                    # also check numeric appearance
                    if infer_numeric_column_enhanced(sample_df[c], threshold=0.9):
                        suggestions[c] = "numeric"
            except Exception:
                pass

    out = {
        "suggestions": suggestions,
        "note": "Use these suggestions to override auto-detection in pipeline or as diagnostics."
    }

    logger.info("Categorical-vs-numeric suggestion count=%d", len(suggestions))
    return out


def llm_enrich_dataframe(cleaned_csv_path_or_df, provider="gemini", api_key=None, output_dir=None):
    """
    Basic LLM enrichment stub. If an OpenAI key and python-openai are available,
    this will attempt to call the model to enrich rows. Otherwise, a local
    heuristic enrichment will be performed:
      - token_count columns for text fields
      - suggested canonical category using mode

    Writes enriched CSV to `output_dir/enriched_data.csv` if output_dir is provided.
    """
    # Load cleaned file if a path was provided
    if isinstance(cleaned_csv_path_or_df, str):
        if not os.path.exists(cleaned_csv_path_or_df):
            raise FileNotFoundError(cleaned_csv_path_or_df)
        df = pd.read_csv(cleaned_csv_path_or_df)
    else:
        df = cleaned_csv_path_or_df

    # Prefer provider-backed enrichment if available
    llm_enriched = False
    if provider == "gemini":
        try:
            import google.generativeai as genai  # type: ignore
            if api_key:
                genai.configure(api_key=api_key)
                logger.info("Gemini provider configured. Attempting live enrichment on sample rows...")
                model = genai.GenerativeModel("gemini-pro")
                enriched = df.copy()
                
                # Enrich first 10 rows with Gemini (to avoid excessive API calls)
                sample_size = min(10, len(df))
                for idx in range(sample_size):
                    row = df.iloc[idx]
                    row_str = " | ".join([f"{col}={val}" for col, val in row.items()])
                    prompt = f"Analyze this data row and provide a single-line summary of quality issues (if any) or 'OK' if clean:\n{row_str}"
                    try:
                        response = model.generate_content(prompt, generation_config={"max_output_tokens": 100})
                        if response and response.text:
                            enriched.at[idx, "__gemini_analysis"] = response.text.strip()
                    except Exception as e:
                        logger.debug("Gemini API call failed for row %d: %s", idx, e)
                
                logger.info("Gemini enrichment complete")
                llm_enriched = True
            else:
                logger.info("Gemini provider requested but no API key provided.")
        except ImportError:
            logger.info("google.generativeai not installed; falling back to local enrichment.")
        except Exception as e:
            logger.warning("Gemini enrichment failed: %s; falling back to local enrichment.", e)

    elif provider == "openai":
        try:
            import openai  # type: ignore
            if api_key:
                openai.api_key = api_key
                logger.info("OpenAI provider configured (live calls not yet implemented).")
            logger.info("OpenAI library available. Performing local fallback enrichment.")
        except Exception:
            logger.info("OpenAI library not available; falling back to local enrichment.")

    # Local heuristic enrichment (safe, deterministic)
    if not llm_enriched:
        enriched = df.copy()
    
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            enriched[c + "__token_count"] = df[c].fillna("").astype(str).apply(lambda s: len(s.split()))

    # categorical canonical suggestion: add column with the column mode
    for c in df.columns:
        try:
            if df[c].nunique(dropna=True) < 100 and pd.api.types.is_object_dtype(df[c]):
                enriched[c + "__suggested_canonical"] = df[c].mode().iloc[0] if not df[c].mode().empty else None
        except Exception:
            pass

    # Optionally save
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "enriched_data.csv")
        enriched.to_csv(out_path, index=False)
        logger.info("Saved enriched data to %s", out_path)
        return out_path

    return enriched
