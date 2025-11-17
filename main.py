#!/usr/bin/env python3
"""
main.py - orchestrate the cleaning pipeline with CLI to control parameters.

Usage examples:
  python main.py --input vehicles.csv --output-dir out --intensity intense --debug

Defaults: intensity=intense (more aggressive sampling / checks)
"""
import os
import argparse
import logging
import time
from dotenv import load_dotenv

from data_cleaning import run_full_cleaning_pipeline_two_pass_sqlite_batched
from pipeline_utils import (
    enhance_numeric_inference,
    fix_categorical_numeric_detection,
    llm_enrich_dataframe,
)
from benchmarking import run_benchmark

logger = logging.getLogger(__name__)


INTENSITY_PRESETS = {
    "light": {
        "chunksize": 200000,
        "numeric_sample_mod": 50,
        "categorical_sample_mod": 100,
        "lsh_sample_mod": 200,
        "original_sample_mod": 50,
    },
    "medium": {
        "chunksize": 100000,
        "numeric_sample_mod": 20,
        "categorical_sample_mod": 40,
        "lsh_sample_mod": 100,
        "original_sample_mod": 20,
    },
    "intense": {
        "chunksize": 50000,
        "numeric_sample_mod": 10,
        "categorical_sample_mod": 20,
        "lsh_sample_mod": 50,
        "original_sample_mod": 10,
    },
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", required=True, help="Path to input file")
    p.add_argument("--output-dir", "-o", default="pipeline_output", help="Directory for outputs")
    p.add_argument("--sqlite", default="pipeline_state.db", help="SQLite state file")
    p.add_argument("--intensity", choices=["light","medium","intense"], default="intense")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--enhance-numeric", action="store_true", dest="enhance_numeric", help="Run enhanced numeric inference after pass1")
    p.add_argument("--fix-catnum", action="store_true", dest="fix_catnum", help="Apply improved categorical-vs-numeric correction")
    p.add_argument("--enable-llm", action="store_true", help="Enable LLM-powered enrichment (stub/local if no key)")
    p.add_argument("--llm-key", default=None, help="API key for chosen LLM provider (optional)")
    p.add_argument("--provider", choices=["gemini","openai"], default="gemini", help="LLM provider to use for enrichment")
    p.add_argument("--benchmark", action="store_true", help="Run benchmarking harness after pipeline")
    return p.parse_args()


def main():
    # Load environment variables from env/.env
    env_path = os.path.join(os.path.dirname(__file__), 'env', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    
    args = parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    preset = INTENSITY_PRESETS.get(args.intensity, INTENSITY_PRESETS["intense"])

    os.makedirs(args.output_dir, exist_ok=True)

    logger.info("Starting pipeline with intensity=%s, input=%s", args.intensity, args.input)
    t0 = time.perf_counter()

    # Run two-pass pipeline
    cleaned_path, report_path = run_full_cleaning_pipeline_two_pass_sqlite_batched(
        path=args.input,
        output_dir=args.output_dir,
        sqlite_path=args.sqlite,
        chunksize=preset["chunksize"],
        numeric_sample_mod=preset["numeric_sample_mod"],
        categorical_sample_mod=preset["categorical_sample_mod"],
        lsh_sample_mod=preset["lsh_sample_mod"],
        original_sample_mod=preset["original_sample_mod"],
    )

    t1 = time.perf_counter()
    logger.info("Pipeline run time: %.2fs", t1 - t0)

    # Optional post-processing: enhanced numeric inference
    if getattr(args, "enhance_numeric", False):
        logger.info("Running enhanced numeric inference (post-pass)")
        try:
            enhance_numeric_inference(report_path, args.input)
        except Exception as e:
            logger.error("Enhanced numeric inference failed: %s", e)

    # Optional fix for categorical-vs-numeric
    if getattr(args, "fix_catnum", False):
        logger.info("Applying categorical-vs-numeric detection fixes")
        try:
            fix_categorical_numeric_detection(report_path, args.input)
        except Exception as e:
            logger.error("Categorical-vs-numeric fix failed: %s", e)

    # Optional LLM enrichment
    if args.enable_llm:
        logger.info("Running LLM-powered enrichment (may use local heuristics if provider lib/key missing)")
        try:
            if cleaned_path and os.path.exists(cleaned_path):
                # Use env variable if no CLI arg provided
                api_key = args.llm_key
                if not api_key:
                    if args.provider == "gemini":
                        api_key = os.getenv("GEMINI_API_KEY")
                    elif args.provider == "openai":
                        api_key = os.getenv("OPENAI_API_KEY")
                llm_enrich_dataframe(cleaned_path, provider=args.provider, api_key=api_key, output_dir=args.output_dir)
            else:
                logger.warning("No cleaned file found to enrich: %s", cleaned_path)
        except Exception as e:
            logger.error("LLM enrichment failed: %s", e)

    # Benchmark if requested
    if args.benchmark:
        logger.info("Starting benchmark run")
        try:
            run_benchmark(input_path=args.input, output_dir=args.output_dir, sqlite_path=args.sqlite)
        except Exception as e:
            logger.error("Benchmarking failed: %s", e)

    logger.info("All done. Outputs in: %s", os.path.abspath(args.output_dir))


if __name__ == "__main__":
    main()