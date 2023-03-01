#!/usr/bin/env python
# -*- coding: utf-8; -*-

# Copyright (c) 2021, 2023 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""This module runs a Jupyter Python notebook with nbconvert and print the outputs.
This is a driver script auto-generated by Oracle ADS.

The following environment variables are used:
JOB_RUN_NOTEBOOK:
    The relative path of the jupyter Python notebook to be executed.
NOTEBOOK_EXCLUDE_TAGS:
    Optional, a list of tags serialized to JSON string.
    Notebook cells with one of the tags will be excluded from running.
NOTEBOOK_ENCODING:
    Optional, the encoding for opening the notebook.
OUTPUT_URI:
    Optional, object storage URI for saving files from the output directory.
"""
import logging
import json
import os
from typing import Optional

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor, CellExecutionError

try:
    # This is used by ADS and testing
    from .driver_utils import OCIHelper, JobRunner, set_log_level
except ImportError:
    # This is used when the script is in a job run.
    from driver_utils import OCIHelper, JobRunner, set_log_level


logger = logging.getLogger(__name__)
logger = set_log_level(logger)

class ADSExecutePreprocessor(ExecutePreprocessor):
    """Customized Execute Preprocessor for running notebook."""

    def __init__(self, exclude_tags=None, **kw):
        """Initialize the preprocessor

        Parameters
        ----------
        exclude_tags : list, optional
            A list of cell tags, notebook cells with any of these cell tag will be skipped.
            Defaults to None.
        """
        self.exclude_tags = exclude_tags
        super().__init__(**kw)

    @staticmethod
    def _print_cell_outputs(cell):
        """Prints the outputs of a notebook cell."""
        for output in cell.outputs:
            output_type = output.get("output_type")
            if output_type == "stream":
                # stream outputs includes line break already
                print(output.text, end="")
            elif output_type == "execute_result":
                # execute_result may contain text/plain
                text = output.get("data", {}).get("text/plain", [])
                # The value could be str or list of str
                if isinstance(text, list):
                    for line in text:
                        print(line)
                else:
                    print(text)

    def preprocess_cell(self, cell, resources, *args, **kwargs):
        """Runs the notebook cell and print out the outputs"""
        # Skip the cell if any of the cell tags matching an exclude tag.
        if self.exclude_tags:
            # Log an error message if there is an error reading the cell tags,
            # and continue to run the cell.
            try:
                cell_tags = cell.get("metadata", {}).get("tags", [])
                for tag in cell_tags:
                    if tag in self.exclude_tags:
                        return cell, resources
            except Exception as ex:
                logger.exception("An error occurred when reading cell tags.")
        # Run the cell
        cell, resources = super().preprocess_cell(cell, resources, *args, **kwargs)
        # Print cell output
        if hasattr(cell, "outputs"):
            # Log a message if there is an error getting the cell output,
            # and continue to run the next cell.
            try:
                self._print_cell_outputs(cell)
            except Exception as ex:
                logger.exception("An error occurred when reading cell outputs.")
        return cell, resources


def run_notebook(
    notebook_path: str,
    working_dir: Optional[str] = None,
    exclude_tags: Optional[list] = None,
) -> Optional[CellExecutionError]:
    """Runs a notebook

    Parameters
    ----------
    notebook_path : str
        The path of the notebook
    working_dir : str, optional
        The working directory for running the notebook, by default None.
        If this is None, the same directory of the notebook_path will be used.
    exclude_tags : list, optional
        Tags for excluding cells, by default None

    Returns
    -------
    CellExecutionError or None
        Exception object when there is an error in a notebook cell.
        Otherwise, None.
    """
    # Read the notebook
    encoding = os.environ.get("NOTEBOOK_ENCODING", "utf-8")
    with open(notebook_path, encoding=encoding) as f:
        nb = nbformat.read(f, as_version=4)

    # Working/Output directory
    if not working_dir:
        working_dir = os.path.dirname(notebook_path)

    # The path of the output notebook with results/plots
    notebook_filename_out = os.path.join(working_dir, os.path.basename(notebook_path))

    ep = ADSExecutePreprocessor(exclude_tags=exclude_tags, kernel_name="python")

    try:
        ep.preprocess(nb, {"metadata": {"path": working_dir}})
        ex = None
    except CellExecutionError as exc:
        msg = "Error executing the notebook.\n\n"
        logger.error(msg)
        ex = exc
    finally:
        with open(notebook_filename_out, mode="w", encoding=encoding) as f:
            nbformat.write(nb, f)
    return ex


def main() -> None:
    """Runs the driver to execute a notebook."""
    JobRunner().conda_unpack()

    notebook_file_path = os.path.join(
        os.path.dirname(__file__), os.environ.get("JOB_RUN_NOTEBOOK")
    )
    output_dir = os.path.join(os.path.dirname(__file__), "outputs")
    # Create the output directory
    os.makedirs(output_dir, exist_ok=True)
    # Exclude tags
    tags = os.environ.get("NOTEBOOK_EXCLUDE_TAGS")
    if tags:
        tags = json.loads(tags)
        logger.info("Excluding cells with any of the following tags: %s", tags)
    # Run the notebook
    ex = run_notebook(notebook_file_path, working_dir=output_dir, exclude_tags=tags)

    # Save the outputs
    OCIHelper.copy_outputs(output_dir)

    if ex:
        raise ex


if __name__ == "__main__":
    main()
