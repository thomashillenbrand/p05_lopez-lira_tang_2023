"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init
## Custom reporter: Print PyDoit Text in Green
# This is helpful because some tasks write to sterr and pollute the output in
# the console. I don't want to mute this output, because this can sometimes
# cause issues when, for example, LaTeX hangs on an error and requires
# presses on the keyboard before continuing. However, I want to be able
# to easily see the task lines printed by PyDoit. I want them to stand out
# from among all the other lines printed to the console.
from doit.reporter import ConsoleReporter
from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        # other config here...
        # "cleanforget": True, # Doit will forget about tasks that have been cleaned.
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}
init(autoreset=True)


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")
USER = config("USER")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

## Helpers for handling Jupyter Notebook tasks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    """Clear the output of a notebook"""
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    """Move a file to a folder"""
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        command = f"mv {from_path} {to_path}"
    else:
        command = f"move {from_path} {to_path}"
    return command


def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull data from external sources"""
    yield {
        "name": "crsp_stock",
        "doc": "Pull CRSP stock data from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_CRSP_stock.py",
        ],
        "targets": [
            DATA_DIR / "CRSP_stock_daily.parquet",
            DATA_DIR / "CRSP_unique_tickers.parquet"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_CRSP_stock.py"
        ],
        "clean": [],
    }
    yield {
        "name": "ravenpack",
        "doc": "Pull RavenPack data from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_ravenpack.py",
        ],
        "targets": [
            DATA_DIR / "RAVENPACK.parquet"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_ravenpack.py"
        ],
        "clean": [],
    }

def task_clean_data():
    """
    Data Cleaning steps.
      - filter to CRSP universe, apply OSA firm-day deduplication,
        remove intraday news, and other basic cleaning steps on RavenPack data
    """
    yield {
        "name": "clean_ravenpack",
        "doc": "Filter RavenPack to CRSP universe and apply OSA firm-day dedupe",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/clean_ravenpack.py",
        ],
        "targets": [DATA_DIR / "RAVENPACK_cleaned.parquet"],
        "file_dep": [
            "./src/settings.py",
            "./src/clean_ravenpack.py",
            DATA_DIR / "RAVENPACK.parquet",
            DATA_DIR / "CRSP_unique_tickers.parquet",
        ],
        "task_dep": [
            "pull:ravenpack",
            "pull:crsp_stock"
        ],
        "clean": [],
    }


def task_process():
    """Data processing steps"""
    
    yield {
        "name": "generate_batched_requests",
        "doc": "Generate JSONL file(s) of batched requests for OpenAI batch",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/generate_batched_requests.py",
        ],
        "targets": [
            DATA_DIR / "openai_headline_requests.1.jsonl",
            DATA_DIR / "id_to_row_mapping.1.json",
            DATA_DIR / "openai_headline_requests.2.jsonl",
            DATA_DIR / "id_to_row_mapping.2.json",
            DATA_DIR / "openai_headline_requests.3.jsonl",
            DATA_DIR / "id_to_row_mapping.3.json",
            DATA_DIR / "openai_headline_requests.4.jsonl",
            DATA_DIR / "id_to_row_mapping.4.json",
            DATA_DIR / "openai_headline_requests.5.jsonl",
            DATA_DIR / "id_to_row_mapping.5.json",
            DATA_DIR / "openai_headline_requests.6.jsonl",
            DATA_DIR / "id_to_row_mapping.6.json"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/generate_batched_requests.py",
            DATA_DIR / "RAVENPACK_cleaned.parquet",
        ],
        "task_dep": [
            "clean_data:clean_ravenpack",
        ],
        "clean": [],
    }

    yield {
        "name": "submit_headlines_to_openai",
        "doc": "Submit cleaned RavenPack headlines via OpenAI batch and aggregate ticker-day polarity",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/submit_headlines_to_openai.py",
        ],
        "targets": [
            DATA_DIR / "openai_headline_batch_output.1.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.1.json",
            DATA_DIR / "openai_headline_batch_output.2.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.2.json",
            DATA_DIR / "openai_headline_batch_output.3.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.3.json",
            DATA_DIR / "openai_headline_batch_output.4.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.4.json",
            DATA_DIR / "openai_headline_batch_output.5.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.5.json",
            DATA_DIR / "openai_headline_batch_output.6.jsonl",
            DATA_DIR / "openai_headline_batch_metadata.6.json"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/submit_headlines_to_openai.py",
            DATA_DIR / "openai_headline_requests.1.jsonl",
            DATA_DIR / "id_to_row_mapping.1.json",
            DATA_DIR / "openai_headline_requests.2.jsonl",
            DATA_DIR / "id_to_row_mapping.2.json",
            DATA_DIR / "openai_headline_requests.3.jsonl",
            DATA_DIR / "id_to_row_mapping.3.json",
            DATA_DIR / "openai_headline_requests.4.jsonl",
            DATA_DIR / "id_to_row_mapping.4.json",
            DATA_DIR / "openai_headline_requests.5.jsonl",
            DATA_DIR / "id_to_row_mapping.5.json",
            DATA_DIR / "openai_headline_requests.6.jsonl",
            DATA_DIR / "id_to_row_mapping.6.json",

        ],
        "task_dep": [
            "process:generate_batched_requests",
        ],
        "clean": [],
    }
    
    yield {
        "name": "create_firm_day_score",
        "doc": "Process OpenAI batch output and aggregate to daily ticker-level sentiment",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_firmday_score.py",
        ],
        "targets": [
            DATA_DIR / "daily_headline_polarity.parquet"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_firmday_score.py",
            DATA_DIR / "openai_headline_batch_output.1.jsonl",
            DATA_DIR / "openai_headline_batch_output.2.jsonl",
            DATA_DIR / "openai_headline_batch_output.3.jsonl",
            DATA_DIR / "openai_headline_batch_output.4.jsonl",
            DATA_DIR / "openai_headline_batch_output.5.jsonl",
            DATA_DIR / "openai_headline_batch_output.6.jsonl"
        ],
        "task_dep": [
            "process:submit_headlines_to_openai",
        ],
        "clean": []
    } 

    yield {
        "name": "create_portfolios",
        "doc": "Construct portfolio return series used by downstream figures and tables",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_portfolios.py",
        ],
        "targets": [
            DATA_DIR / "portfolio_daily_returns.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_portfolios.py",
            DATA_DIR / "daily_headline_polarity.parquet",
            DATA_DIR / "CRSP_stock_daily.parquet",
        ],
        "task_dep": [
            "process:create_firm_day_score",
            "pull:crsp_stock",
        ],
        "clean": [],
    }


def task_charts():
    """HW3: Generate exploratory charts (interactive HTML)"""
#     yield {
#         "name": "crsp_daily_closing_prices",
#         "actions": [
#             "ipython ./src/settings.py",
#             "ipython ./src/plot_CRSP_data.py",
#         ],
#         "targets": [OUTPUT_DIR / "crsp_daily_closing_prices.html"],
#         "file_dep": [
#             "./src/settings.py",
#             "./src/plot_CRSP_data.py",
#             DATA_DIR / "CRSP_stock_daily.parquet",
#         ],
#         "clean": True,
#     }

    yield {
        "name": "ravenpack_news_timing",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/plot_ravenpack_data.py",
        ],
        "targets": [OUTPUT_DIR / "ravenpack_overnight_intraday_proportion.html"],
        "file_dep": [
            "./src/settings.py",
            "./src/plot_ravenpack_data.py",
            DATA_DIR / "RAVENPACK.parquet",
        ],
        "clean": True,
    }

    """Generate report-facing CSVs, figures, and LaTeX table fragments"""

    yield {
        "name": "summary_statistics",
        "doc": "Create summary statistics CSVs for CRSP universe and news distribution",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_summary_statistics.py",
        ],
        "targets": [
            OUTPUT_DIR / "summary_stats_crsp_universe.csv",
            OUTPUT_DIR / "summary_stats_news_by_year.csv",
            OUTPUT_DIR / "summary_stats_news_by_timing.csv",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_summary_statistics.py",
            DATA_DIR / "CRSP_stock_daily.parquet",
            DATA_DIR / "RAVENPACK.parquet",
        ],
        "task_dep": [
            "pull:crsp_stock",
            "pull:ravenpack",
        ],
        "clean": [],
    }

    yield {
        "name": "openai_responses_proportion",
        "doc": "Create label proportion CSV from OpenAI batch outputs",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_openai_responses_table.py",
        ],
        "targets": [
            OUTPUT_DIR / "openai_output_label_proportions.csv",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_openai_responses_table.py",
            DATA_DIR / "id_to_row_mapping.1.json",
            DATA_DIR / "id_to_row_mapping.2.json",
            DATA_DIR / "id_to_row_mapping.3.json",
            DATA_DIR / "id_to_row_mapping.4.json",
            DATA_DIR / "id_to_row_mapping.5.json",
            DATA_DIR / "openai_headline_batch_output.1.jsonl",
            DATA_DIR / "openai_headline_batch_output.2.jsonl",
            DATA_DIR / "openai_headline_batch_output.3.jsonl",
            DATA_DIR / "openai_headline_batch_output.4.jsonl",
            DATA_DIR / "openai_headline_batch_output.5.jsonl",
        ],
        "clean": [],
    }

    yield {
        "name": "portfolio_size",
        "doc": "Create portfolio size diagnostic plots and CSVs",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_portfolio_size.py",
        ],
        "targets": [
            OUTPUT_DIR / "portfolio_size_diagnostics_daily.csv",
            OUTPUT_DIR / "portfolio_size_diagnostics_summary.csv",
            OUTPUT_DIR / "portfolio_size_diagnostics_all.png",
            OUTPUT_DIR / "portfolio_size_diagnostics_grid.png",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_portfolio_size.py",
            DATA_DIR / "daily_headline_polarity.parquet",
            DATA_DIR / "CRSP_stock_daily.parquet",
        ],
        "task_dep": [
            "process:create_firm_day_score",
            "pull:crsp_stock",
        ],
        "clean": [],
    }

    yield {
        "name": "table1",
        "doc": "Create Table 1 CSVs for paper and full samples",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_table1.py",
        ],
        "targets": [
            OUTPUT_DIR / "table1_overnight_paper_sample.csv",
            OUTPUT_DIR / "table1_overnight_full_sample.csv",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_table1.py",
            DATA_DIR / "daily_headline_polarity.parquet",
            DATA_DIR / "CRSP_stock_daily.parquet",
        ],
        "task_dep": [
            "process:create_firm_day_score",
            "pull:crsp_stock",
        ],
        "clean": [],
    }

    yield {
        "name": "figure5",
        "doc": "Create Figure 5 charts and exported series CSVs",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/create_figure5.py",
        ],
        "targets": [
            OUTPUT_DIR / "figure5_paper_sample.png",
            OUTPUT_DIR / "figure5_full_sample.png",
            OUTPUT_DIR / "figure5_paper_sample_series.csv",
            OUTPUT_DIR / "figure5_full_sample_series.csv",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/create_figure5.py",
            DATA_DIR / "portfolio_daily_returns.parquet",
        ],
        "task_dep": [
            "process:create_portfolios",
        ],
        "clean": [],
    }

    yield {
        "name": "pandas_to_latex",
        "doc": "Convert report CSV outputs into LaTeX table fragments",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pandas_to_latex_tables.py",
        ],
        "targets": [
            OUTPUT_DIR / "label_ratio_table.tex",
            OUTPUT_DIR / "replication_table1_paper_sample.tex",
            OUTPUT_DIR / "replication_table1_full_sample.tex",
            OUTPUT_DIR / "summary_stats_crsp_universe.tex",
            OUTPUT_DIR / "summary_stats_news_by_year.tex",
            OUTPUT_DIR / "summary_stats_news_by_timing.tex",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pandas_to_latex_tables.py",
            OUTPUT_DIR / "openai_output_label_proportions.csv",
            OUTPUT_DIR / "table1_overnight_paper_sample.csv",
            OUTPUT_DIR / "table1_overnight_full_sample.csv",
            OUTPUT_DIR / "summary_stats_crsp_universe.csv",
            OUTPUT_DIR / "summary_stats_news_by_year.csv",
            OUTPUT_DIR / "summary_stats_news_by_timing.csv",
        ],
        "task_dep": [
            "charts:summary_statistics",
            "charts:openai_responses_proportion",
            "charts:table1",
        ],
        "clean": [],
    }



###############################################################
## Task below is for LaTeX compilation
###############################################################

def task_compile_latex_docs():
    """Compile the LaTeX documents to PDFs"""

    file_dep = [
        "./reports/final_report.tex",
        "./reports/bibliography.bib",
        "./reports/jpe.bst",
        "./reports/my_article_header.sty",
        "./reports/my_common_header.sty",

        OUTPUT_DIR / "label_ratio_table.tex",
        OUTPUT_DIR / "replication_table1_paper_sample.tex",
        OUTPUT_DIR / "replication_table1_full_sample.tex",
        OUTPUT_DIR / "summary_stats_crsp_universe.tex",
        OUTPUT_DIR / "summary_stats_news_by_year.tex",
        OUTPUT_DIR / "summary_stats_news_by_timing.tex",

        OUTPUT_DIR / "figure5_paper_sample.png",
        OUTPUT_DIR / "figure5_full_sample.png",
        OUTPUT_DIR / "portfolio_size_diagnostics_all.png",
        OUTPUT_DIR / "portfolio_size_diagnostics_grid.png",
    ]

    targets = [
        "./reports/final_report.pdf",
    ]

    return {
        "actions": [
            "latexmk -xelatex -halt-on-error -cd ./reports/final_report.tex",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "task_dep": [
            "charts:pandas_to_latex",
            "charts:portfolio_size",
            "charts:figure5",
        ],
        "clean": True,
    }

# sphinx_targets = [
#     "./docs/index.html",
# ]


# def task_build_chartbook_site():
#     """Compile Sphinx Docs"""
#     file_dep = [
#         "./README.md",
#         "./chartbook.toml",
#     ]

#     return {
#         "actions": [
#             "chartbook build -f",
#         ],  # Use docs as build destination
#         "targets": sphinx_targets,
#         "file_dep": file_dep,
#         "task_dep": ["charts"],
#         "clean": True,
#     }
