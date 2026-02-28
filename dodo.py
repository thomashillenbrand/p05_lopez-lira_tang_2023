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
        "targets": [DATA_DIR / "CRSP_stock_daily.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_CRSP_stock.py"],
        "clean": [],
    }
    yield {
        "name": "ravenpack",
        "doc": "Pull RavenPack data from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_ravenpack.py",
        ],
        "targets": [DATA_DIR / "RAVENPACK.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_ravenpack.py"],
        "clean": [],
    }
    yield {
        "name": "CRSP_unique_tickers",
        "doc": "Pull unique CRSP tickers from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_crsp_unique_tickers.py",
        ],
        "targets": [DATA_DIR / "CRSP_unique_tickers.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_crsp_unique_tickers.py"],
        "clean": [],
    }

def task_process():
    """Data cleaning and processing steps"""

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
            "pull:crsp_unique_tickers",
        ],
        "clean": [],
    }

    yield {
        "name": "clean_crsp",
        "doc": "Filter CRSP to RavenPack universe based on unique tickers",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/clean_crsp.py",
        ],
        "targets": [DATA_DIR / "CRSP_clean_daily.parquet"],
        "file_dep": [
            "./src/settings.py",
            "./src/clean_crsp.py",
            DATA_DIR / "RAVENPACK_cleaned.parquet",
            DATA_DIR / "CRSP_stock_daily.parquet",
        ],
        "task_dep": [
            "pull:ravenpack",
            "process:clean_ravenpack",
            "pull:crsp_stock",
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
            DATA_DIR / "openai_headline_batch_output.jsonl"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/submit_headlines_to_openai.py",
            DATA_DIR / "RAVENPACK_cleaned.parquet",
        ],
        "task_dep": [
            "process:clean_ravenpack",
        ],
        "clean": [],
    }
    
    yield {
        "name": "process_openai_responses",
        "doc": "Process OpenAI batch output and aggregate to daily ticker-level sentiment",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/process_openai_responses.py",
        ],
        "targets": [
            DATA_DIR / "daily_headline_polarity.parquet"
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/process_openai_responses.py",
            OUTPUT_DIR / "openai_headline_batch_output.jsonl",
        ],
        "task_dep": [
            "process:submit_headlines_to_openai",
        ],
        "clean": []
    }


def task_charts():
    """HW3: Generate exploratory charts (interactive HTML)"""
    yield {
        "name": "crsp_daily_closing_prices",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/plot_CRSP_data.py",
        ],
        "targets": [OUTPUT_DIR / "crsp_daily_closing_prices.html"],
        "file_dep": [
            "./src/settings.py",
            "./src/plot_CRSP_data.py",
            DATA_DIR / "CRSP_stock_daily.parquet",
        ],
        "clean": True,
    }

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


sphinx_targets = [
    "./docs/index.html",
]


def task_build_chartbook_site():
    """Compile Sphinx Docs"""
    file_dep = [
        "./README.md",
        "./chartbook.toml",
    ]

    return {
        "actions": [
            "chartbook build -f",
        ],  # Use docs as build destination
        "targets": sphinx_targets,
        "file_dep": file_dep,
        "task_dep": ["charts"],
        "clean": True,
    }
