"""
benchmarking.py

Lightweight benchmarking harness for the pipeline. Measures runtime and simple
quality metrics (dedupe rate, imputation counts, normalization accuracy).

This includes placeholders/descriptions for how to compare against Databricks,
Cleanlab, and OpenRefine (these external runs are out-of-scope for this local
script and should be run in their respective environments; this script
provides a common schema for result JSON so you can compare after the fact).
"""
import os
import time
import json
import logging
from data_cleaning import run_full_cleaning_pipeline_two_pass_sqlite_batched
from pipeline_utils import compute_normalization_accuracy
import pandas as pd

logger = logging.getLogger(__name__)


def run_benchmark(input_path, output_dir="benchmark_output", sqlite_path="benchmark_state.db"):
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.perf_counter()
    cleaned_path, report_path = run_full_cleaning_pipeline_two_pass_sqlite_batched(
        path=input_path,
        output_dir=output_dir,
        sqlite_path=sqlite_path,
        chunksize=50000
    )
    t1 = time.perf_counter()

    bench = {
        "input_path": input_path,
        "output_dir": output_dir,
        "runtime_seconds": t1 - t0,
        "timestamp": time.time(),
    }

    # Read cleaning report for metrics
    report = {}
    if report_path and os.path.exists(report_path):
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
        except Exception:
            report = {}

    bench["cleaning_report_summary"] = report.get("summary", {})

    # normalization accuracy: try to compare original sample vs after sample in report
    try:
        before_sample = pd.DataFrame(report.get("samples", {}).get("before_sample", []))
        after_sample = pd.DataFrame(report.get("samples", {}).get("after_sample", []))
        if not before_sample.empty and not after_sample.empty:
            norm_metrics = compute_normalization_accuracy(before_sample, after_sample, sample_rows=200)
            bench["normalization_accuracy"] = norm_metrics
    except Exception as e:
        logger.debug("Normalization accuracy check failed: %s", e)

    # Add placeholders for external comparisons
    bench["external_comparison_instructions"] = {
        "databricks": "Run a similar cleaning workflow in Databricks and export a JSON summary with keys: runtime_seconds, cleaned_rows, rows_dropped_total, imputed_counts",
        "cleanlab": "Run Cleanlab workflows for label cleaning as needed and export precision/recall metrics",
        "openrefine": "Use OpenRefine to profile and clean the dataset; export a summary JSON with counts of edits per column",
        "note": "This harness cannot run those external services. Produce JSON outputs from those tools and place them in the output_dir for side-by-side comparison."
    }

    out_path = os.path.join(output_dir, "benchmark_report.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(bench, f, indent=2, default=str)

    logger.info("Benchmark complete. Report saved to %s", out_path)
    return out_path
