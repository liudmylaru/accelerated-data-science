#!/usr/bin/env python

# Copyright (c) 2023 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/

import os

NETWORKS_COMPARTMENT_OCID = os.environ.get(
    "NETWORKS_COMPARTMENT_OCID"
)  # ends with "kqteq"
JOBS_IN_NETWORKS_LOG_OCID = os.environ.get(
    "JOBS_IN_NETWORKS_LOG_OCID"
)  # ends with "5srca"
JOBRUNPRE_OCID = os.environ.get("JOBRUNPRE_OCID")  # ends with "rjk2a"
DC_LOG_GROUP_OCID = os.environ.get("DC_LOG_GROUP_OCID")  # ends with "r7qoq"
DC_ACCESS_LOG_OCID = os.environ.get("DC_ACCESS_LOG_OCID")  # ends with "yupl2a"
DC_PREDICT_LOG_OCID = os.environ.get("DC_PREDICT_LOG_OCID")  # ends with "5rv5q"
DC_MODEL_DEPLOYMENT_OCID = os.environ.get(
    "DC_MODEL_DEPLOYMENT_OCID"
)  # ends with "x2y5q"
