#!/usr/bin/env python
# -*- coding: utf-8; -*-

# Copyright (c) 2021, 2022 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/


import collections
import datetime
import json
import time
from typing import Dict, Union, List, Any

import oci.loggingsearch
import pandas as pd
from ads.common import auth, oci_client
from ads.common.data_serializer import InputDataSerializer
from ads.common.oci_logging import (
    LOG_RECORDS_LIMIT,
    ConsolidatedLog,
    OCILog,
)
from ads.model.common.utils import _is_json_serializable
from ads.model.deployment.common.utils import send_request
from .common import utils
from .common.utils import OCIClientManager, State
from .model_deployment_properties import ModelDeploymentProperties

DEFAULT_WAIT_TIME = 1200
DEFAULT_POLL_INTERVAL = 10
DEFAULT_WORKFLOW_STEPS = 6
DELETE_WORKFLOW_STEPS = 2
DEACTIVATE_WORKFLOW_STEPS = 2
DEFAULT_RETRYING_REQUEST_ATTEMPTS = 3
TERMINAL_STATES = [State.ACTIVE, State.FAILED, State.DELETED, State.INACTIVE]


class ModelDeploymentLogType:
    PREDICT = "predict"
    ACCESS = "access"


class LogNotConfiguredError(Exception):
    pass


class ModelDeployment:
    """
    A class used to represent a Model Deployment.

    Attributes
    ----------
    config: (dict)
        Deployment configuration parameters
    properties: (ModelDeploymentProperties)
        ModelDeploymentProperties object
    workflow_state_progress: (str)
        Workflow request id
    workflow_steps: (int)
        The number of steps in the workflow
    url: (str)
        The model deployment url endpoint
    ds_client: (DataScienceClient)
        The data science client used by model deployment
    ds_composite_client: (DataScienceCompositeClient)
        The composite data science client used by the model deployment
    workflow_req_id: (str)
        Workflow request id
    model_deployment_id: (str)
        model deployment id
    state: (State)
        Returns the deployment state of the current Model Deployment object

    Methods
    -------
    deploy(wait_for_completion, **kwargs)
        Deploy the current Model Deployment object
    delete(wait_for_completion, **kwargs)
        Deletes the current Model Deployment object
    update(wait_for_completion, **kwargs)
        Updates a model deployment
    activate(wait_for_completion, max_wait_time, poll_interval)
        Activates a model deployment
    deactivate(wait_for_completion, max_wait_time, poll_interval)
        Deactivates a model deployment
    list_workflow_logs()
        Returns a list of the steps involved in deploying a model
    """

    def __init__(
        self,
        properties: Union[ModelDeploymentProperties, Dict] = None,
        config: Dict = None,
        workflow_req_id: str = None,
        model_deployment_id: str = None,
        model_deployment_url: str = "",
        **kwargs,
    ):
        """Initializes a ModelDeployment object.

        Parameters
        ----------
        properties: (Union[ModelDeploymentProperties, Dict], optional). Defaults to None.
            Object containing deployment properties.
            The properties can be `None` when `kwargs` are used for specifying properties.
        config: (Dict, optional). Defaults to None.
            ADS auth dictionary for OCI authentication.
            This can be generated by calling `ads.common.auth.api_keys()` or `ads.common.auth.resource_principal()`.
            If this is `None` then the `ads.common.default_signer(client_kwargs)` will be used.
        workflow_req_id: (str, optional). Defaults to None.
            Workflow request id.
        model_deployment_id: (str, optional). Defaults to None.
            Model deployment OCID.
        model_deployment_url: (str, optional). Defaults to empty string.
            Model deployment url.
        kwargs:
            Keyword arguments for initializing `ModelDeploymentProperties`.
        """

        if config is None:
            utils.get_logger().info("Using default configuration.")
            config = auth.default_signer()

        # self.config is ADS auth dictionary for OCI authentication.
        self.config = config

        self.properties = (
            properties
            if isinstance(properties, ModelDeploymentProperties)
            else ModelDeploymentProperties(
                oci_model_deployment=properties, config=self.config, **kwargs
            )
        )

        self.current_state = (
            State._from_str(self.properties.lifecycle_state)
            if self.properties.lifecycle_state
            else State.UNKNOWN
        )
        self.url = (
            model_deployment_url
            if model_deployment_url
            else self.properties.model_deployment_url
        )
        self.model_deployment_id = (
            model_deployment_id if model_deployment_id else self.properties.id
        )

        self.workflow_state_progress = []
        self.workflow_steps = DEFAULT_WORKFLOW_STEPS

        client_manager = OCIClientManager(config)
        self.ds_client = client_manager.ds_client
        self.ds_composite_client = client_manager.ds_composite_client
        self.workflow_req_id = workflow_req_id

        if self.ds_client:
            self.log_search_client = oci_client.OCIClientFactory(
                **self.config
            ).create_client(oci.loggingsearch.LogSearchClient)

        self._access_log = None
        self._predict_log = None

    def deploy(
        self,
        wait_for_completion: bool = True,
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        """deploy deploys the current ModelDeployment object

        Parameters
        ----------
        wait_for_completion: bool
            Flag set for whether to wait for deployment to be deployed before proceeding.
            Defaults to True.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).

        Returns
        -------
        ModelDeployment
           The instance of ModelDeployment.
        """
        response = self.ds_composite_client.create_model_deployment_and_wait_for_state(
            self.properties.build()
        )
        self.workflow_req_id = response.headers["opc-work-request-id"]
        res_payload = json.loads(str(response.data))
        self.current_state = State._from_str(res_payload["lifecycle_state"])
        self.model_deployment_id = res_payload["id"]
        self.url = res_payload["model_deployment_url"]
        if wait_for_completion:
            try:
                self._wait_for_progress_completion(
                    State.ACTIVE.name,
                    DEFAULT_WORKFLOW_STEPS,
                    [State.FAILED.name, State.INACTIVE.name],
                    max_wait_time,
                    poll_interval,
                )
            except Exception as e:
                utils.get_logger().error(f"Error while trying to deploy: {str(e)}")
                raise e
        return self

    def delete(
        self,
        wait_for_completion: bool = True,
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        """Deletes the ModelDeployment

        Parameters
        ----------
        wait_for_completion: bool
            Flag set for whether to wait for deployment to be deleted before proceeding.
            Defaults to True.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).

        Returns
        -------
        ModelDeployment
            The instance of ModelDeployment.
        """

        response = self.ds_composite_client.delete_model_deployment_and_wait_for_state(
            self.model_deployment_id
        )
        # response.data from deleting model is None, headers are populated
        self.workflow_req_id = response.headers["opc-work-request-id"]
        oci_model_deployment_object = self.ds_client.get_model_deployment(
            self.model_deployment_id
        ).data
        self.current_state = State._from_str(
            oci_model_deployment_object.lifecycle_state
        )
        if wait_for_completion:
            try:
                self._wait_for_progress_completion(
                    State.DELETED.name,
                    DELETE_WORKFLOW_STEPS,
                    [State.FAILED.name, State.INACTIVE.name],
                    max_wait_time,
                    poll_interval,
                )
            except Exception as e:
                utils.get_logger().error(f"Error while trying to delete: {str(e)}")
                raise e
        return self

    def update(
        self,
        properties: Union[ModelDeploymentProperties, dict, None] = None,
        wait_for_completion: bool = True,
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        **kwargs,
    ):
        """Updates a model deployment

        You can update `model_deployment_configuration_details` and change `instance_shape` and `model_id`
        when the model deployment is in the ACTIVE lifecycle state.
        The `bandwidth_mbps` or `instance_count` can only be updated while the model deployment is in the `INACTIVE` state.
        Changes to the `bandwidth_mbps` or `instance_count` will take effect the next time
        the `ActivateModelDeployment` action is invoked on the model deployment resource.

        Parameters
        ----------
        properties: ModelDeploymentProperties or dict
            The properties for updating the deployment.
        wait_for_completion: bool
            Flag set for whether to wait for deployment to be updated before proceeding.
            Defaults to True.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).
        kwargs:
            dict

        Returns
        -------
        ModelDeployment
            The instance of ModelDeployment.
        """
        if not isinstance(properties, ModelDeploymentProperties):
            properties = ModelDeploymentProperties(
                oci_model_deployment=properties, config=self.config, **kwargs
            )

        if wait_for_completion:
            wait_for_states = ["SUCCEEDED", "FAILED"]
        else:
            wait_for_states = []

        try:
            response = (
                self.ds_composite_client.update_model_deployment_and_wait_for_state(
                    self.model_deployment_id,
                    properties.to_update_deployment(),
                    wait_for_states=wait_for_states,
                    waiter_kwargs={
                        "max_interval_seconds": poll_interval,
                        "max_wait_seconds": max_wait_time,
                    },
                )
            )
            if "opc-work-request-id" in response.headers:
                self.workflow_req_id = response.headers["opc-work-request-id"]
            # Refresh the properties when model is active
            if wait_for_completion:
                self.properties = ModelDeploymentProperties(
                    oci_model_deployment=self.ds_client.get_model_deployment(
                        self.model_deployment_id
                    ).data,
                    config=self.config,
                )
        except Exception as e:
            utils.get_logger().error(
                "Updating model deployment failed with error: %s", format(e)
            )
            raise e

        return self

    @property
    def state(self) -> State:
        """Returns the deployment state of the current Model Deployment object"""
        request_attempts = 0
        while request_attempts < DEFAULT_RETRYING_REQUEST_ATTEMPTS:
            request_attempts += 1
            try:
                oci_state = self.ds_client.get_model_deployment(
                    retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY,
                    model_deployment_id=self.model_deployment_id,
                ).data.lifecycle_state
                self.current_state = State._from_str(oci_state)
                break
            except:
                pass
            time.sleep(1)

        return self.current_state

    @property
    def status(self) -> State:
        """Returns the deployment state of the current Model Deployment object"""
        return self.state

    def list_workflow_logs(self) -> list:
        """Returns a list of the steps involved in deploying a model

        Returns
        -------
        list
            List of dictionaries detailing the status of each step in the deployment process.
        """
        if self.workflow_req_id == "" or self.workflow_req_id == None:
            utils.get_logger().info("Workflow req id not available")
            raise Exception
        return self.ds_client.list_work_request_logs(self.workflow_req_id).data

    def predict(
        self,
        json_input=None,
        data: Any = None,
        auto_serialize_data: bool = False,
        **kwargs,
    ) -> dict:
        """Returns prediction of input data run against the model deployment endpoint

        Parameters
        ----------
        json_input: Json serializable
            Json payload for the prediction.
        data: Any
            Data for the prediction.
        auto_serialize_data: bool.
            Whether to auto serialize input data. Defauls to `False`.
            If `auto_serialize_data=False`, `data` required to be bytes or json serializable
            and `json_input` required to be json serializable. If `auto_serialize_data` set
            to True, data will be serialized before sending to model deployment endpoint.
        kwargs:
            content_type: str
                Used to indicate the media type of the resource.
                By default, it will be `application/octet-stream` for bytes input and `application/json` otherwise.
                The content-type header will be set to this value when calling the model deployment endpoint.

        Returns
        -------
        dict
            Prediction results.

        """
        endpoint = f"{self.url}/predict"
        signer = self.config.get("signer")
        header = {
            "signer": signer,
            "content_type": kwargs.get("content_type", None),
        }

        if data is None and json_input is None:
            raise AttributeError(
                "Neither `data` nor `json_input` are provided. You need to provide one of them."
            )
        if data is not None and json_input is not None:
            raise AttributeError(
                "`data` and `json_input` are both provided. You can only use one of them."
            )

        if auto_serialize_data:
            data = data or json_input
            serialized_data = InputDataSerializer(data=data)
            return serialized_data.send(endpoint, **header)

        elif json_input is not None:
            if not _is_json_serializable(json_input):
                raise ValueError(
                    "`json_input` must be json serializable. "
                    "Set `auto_serialize_data` to True, or serialize the provided input data first,"
                    "or using `data` to pass binary data."
                )
            utils.get_logger().warning(
                "The `json_input` argument of `predict()` will be deprecated soon. "
                "Please use `data` argument. "
            )
            data = json_input

        is_json_payload = True if _is_json_serializable(data) else False
        prediction = send_request(
            data=data, endpoint=endpoint, is_json_payload=is_json_payload, header=header
        )
        return prediction

    def activate(
        self,
        wait_for_completion: bool = True,
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ) -> "ModelDeployment":
        """Activates a model deployment

        Parameters
        ----------
        wait_for_completion: bool
            Flag set for whether to wait for deployment to be activated before proceeding.
            Defaults to True.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).

        Returns
        -------
        ModelDeployment
            The instance of ModelDeployment.
        """
        response = (
            self.ds_composite_client.activate_model_deployment_and_wait_for_state(
                self.model_deployment_id
            )
        )
        self.workflow_req_id = response.headers["opc-work-request-id"]
        oci_model_deployment_object = self.ds_client.get_model_deployment(
            self.model_deployment_id
        ).data
        self.current_state = State._from_str(
            oci_model_deployment_object.lifecycle_state
        )
        self.model_deployment_id = oci_model_deployment_object.id
        self.url = oci_model_deployment_object.model_deployment_url

        if wait_for_completion:
            try:
                self._wait_for_progress_completion(
                    State.ACTIVE.name,
                    DEFAULT_WORKFLOW_STEPS,
                    [State.FAILED.name, State.INACTIVE.name],
                    max_wait_time,
                    poll_interval,
                )
            except Exception as e:
                utils.get_logger().error(f"Error while trying to activate: {str(e)}")
                raise e
        return self

    def deactivate(
        self,
        wait_for_completion: bool = True,
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ) -> "ModelDeployment":
        """Deactivates a model deployment

        Parameters
        ----------
        wait_for_completion: bool
            Flag set for whether to wait for deployment to be deactivated before proceeding.
            Defaults to True.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).

        Returns
        -------
        ModelDeployment
            The instance of ModelDeployment.
        """
        response = (
            self.ds_composite_client.deactivate_model_deployment_and_wait_for_state(
                self.model_deployment_id
            )
        )
        self.workflow_req_id = response.headers["opc-work-request-id"]
        oci_model_deployment_object = self.ds_client.get_model_deployment(
            self.model_deployment_id
        ).data
        self.current_state = State._from_str(
            oci_model_deployment_object.lifecycle_state
        )
        self.model_deployment_id = oci_model_deployment_object.id
        self.url = oci_model_deployment_object.model_deployment_url

        if wait_for_completion:
            try:
                self._wait_for_progress_completion(
                    State.INACTIVE.name,
                    DEACTIVATE_WORKFLOW_STEPS,
                    [State.FAILED.name],
                    max_wait_time,
                    poll_interval,
                )
            except Exception as e:
                utils.get_logger().error(f"Error while trying to deactivate: {str(e)}")
                raise e
        return self

    def _wait_for_progress_completion(
        self,
        final_state: str,
        work_flow_step: int,
        disallowed_final_states: List[str],
        max_wait_time: int = DEFAULT_WAIT_TIME,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        """_wait_for_progress_completion blocks until progress is completed.

        Parameters
        ----------
        final_state: str
            Final state of model deployment aimed to be reached.
        work_flow_step: int
            Number of work flow step of the request.
        disallowed_final_states: list[str]
            List of disallowed final state to be reached.
        max_wait_time: int
            Maximum amount of time to wait in seconds (Defaults to 1200).
            Negative implies infinite wait time.
        poll_interval: int
            Poll interval in seconds (Defaults to 10).
        """

        start_time = time.time()
        prev_message = ""
        prev_workflow_stage_len = 0
        with utils.get_progress_bar(work_flow_step) as progress:
            if max_wait_time > 0 and utils.seconds_since(start_time) >= max_wait_time:
                utils.get_logger().error(
                    f"Max wait time ({max_wait_time} seconds) exceeded."
                )
            while (
                max_wait_time < 0 or utils.seconds_since(start_time) < max_wait_time
            ) and self.current_state.name.upper() != final_state:
                if self.current_state.name.upper() in disallowed_final_states:
                    utils.get_logger().info(
                        f"Operation failed due to deployment reaching state {self.current_state.name.upper()}. Use Deployment ID for further steps."
                    )
                    break

                prev_state = self.current_state.name
                try:
                    model_deployment_payload = json.loads(
                        str(
                            self.ds_client.get_model_deployment(
                                self.model_deployment_id
                            ).data
                        )
                    )
                    self.current_state = (
                        State._from_str(model_deployment_payload["lifecycle_state"])
                        if "lifecycle_state" in model_deployment_payload
                        else State.UNKNOWN
                    )
                    workflow_payload = self.ds_client.list_work_request_logs(
                        self.workflow_req_id
                    ).data
                    if isinstance(workflow_payload, list) and len(workflow_payload) > 0:
                        if prev_message != workflow_payload[-1].message:
                            for _ in range(
                                len(workflow_payload) - prev_workflow_stage_len
                            ):
                                progress.update(workflow_payload[-1].message)
                            prev_workflow_stage_len = len(workflow_payload)
                            prev_message = workflow_payload[-1].message
                            prev_workflow_stage_len = len(workflow_payload)
                    if prev_state != self.current_state.name:
                        if "model_deployment_url" in model_deployment_payload:
                            self.url = model_deployment_payload["model_deployment_url"]
                        utils.get_logger().info(
                            f"Status Update: {self.current_state.name} in {utils.seconds_since(start_time)} seconds"
                        )
                except Exception as e:
                    # utils.get_logger().warning(
                    #     "Unable to update deployment status. Details: %s", format(
                    #         e)
                    # )
                    pass
                time.sleep(poll_interval)
            progress.update("Done")

    def _log_details(self, log_type: str = ModelDeploymentLogType.ACCESS):
        """Gets log details for the provided `log_type`.

        Properties
        ----------
        log_type: (str, optional). Defaults to "access".
            The log type. Can be "access" or "predict".

        Returns
        -------
        oci.datascience_model.CategoryLogDetails
            Category log details of the ModelDeployment.

        Raises
        ------
        AttributeError
            Deployment doesn't have requested log configuration.

        """
        if not self.properties.category_log_details or not getattr(
            self.properties.category_log_details, log_type
        ):
            raise LogNotConfiguredError(
                f"Deployment `{self.model_deployment_id}` "
                f"has no `{log_type}` log configuration."
            )
        return getattr(self.properties.category_log_details, log_type)

    @property
    def predict_log(self) -> OCILog:
        """Gets the model deployment predict logs object.

        Returns
        -------
        OCILog
            The OCILog object containing the predict logs.
        """
        if not self._predict_log:
            log_details = self._log_details(log_type=ModelDeploymentLogType.PREDICT)
            self._predict_log = OCILog(
                compartment_id=self.properties.compartment_id,
                id=log_details.log_id,
                log_group_id=log_details.log_group_id,
                source=self.model_deployment_id,
                annotation=ModelDeploymentLogType.PREDICT,
            )
        return self._predict_log

    @property
    def access_log(self) -> OCILog:
        """Gets the model deployment access logs object.

        Returns
        -------
        OCILog
            The OCILog object containing the access logs.
        """
        if not self._access_log:
            log_details = self._log_details(log_type=ModelDeploymentLogType.ACCESS)
            self._access_log = OCILog(
                compartment_id=self.properties.compartment_id,
                id=log_details.log_id,
                log_group_id=log_details.log_group_id,
                source=self.model_deployment_id,
                annotation=ModelDeploymentLogType.ACCESS,
            )
        return self._access_log

    def logs(self, log_type: str = None) -> ConsolidatedLog:
        """Gets the access or predict logs.

        Parameters
        ----------
        log_type: (str, optional). Defaults to None.
            The log type. Can be "access", "predict" or None.

        Returns
        -------
        ConsolidatedLog
            The ConsolidatedLog object containing the logs.
        """
        loggers = []
        if not log_type:
            try:
                loggers.append(self.access_log)
            except LogNotConfiguredError:
                pass

            try:
                loggers.append(self.predict_log)
            except LogNotConfiguredError:
                pass

            if not loggers:
                raise LogNotConfiguredError(
                    "Neither `predict` nor `access` log was configured for the model deployment."
                )
        elif log_type == ModelDeploymentLogType.ACCESS:
            loggers = [self.access_log]
        elif log_type == ModelDeploymentLogType.PREDICT:
            loggers = [self.predict_log]
        else:
            raise ValueError(
                "Parameter log_type should be either access, predict or None."
            )

        return ConsolidatedLog(*loggers)

    def show_logs(
        self,
        time_start: datetime.datetime = None,
        time_end: datetime.datetime = None,
        limit: int = LOG_RECORDS_LIMIT,
        log_type: str = None,
    ):
        """Shows deployment logs as a pandas dataframe.

        Parameters
        ----------
        time_start: (datetime.datetime, optional). Defaults to None.
            Starting date and time in RFC3339 format for retrieving logs.
            Defaults to None. Logs will be retrieved 14 days from now.
        time_end: (datetime.datetime, optional). Defaults to None.
            Ending date and time in RFC3339 format for retrieving logs.
            Defaults to None. Logs will be retrieved until now.
        limit: (int, optional). Defaults to 100.
            The maximum number of items to return.
        log_type: (str, optional). Defaults to None.
            The log type. Can be "access", "predict" or None.

        Returns
        -------
            A pandas DataFrame containing logs.
        """
        logging = self.logs(log_type=log_type)

        def prepare_log_record(log):
            """Converts a log record to ordered dict"""
            log_content = log.get("logContent", {})
            return collections.OrderedDict(
                [
                    ("type", log_content.get("type").split(".")[-1]),
                    ("id", log_content.get("id")),
                    ("message", log_content.get("data", {}).get("message")),
                    ("time", log_content.get("time")),
                ]
            )

        logs = logging.search(
            source=self.model_deployment_id,
            time_start=time_start,
            time_end=time_end,
            limit=limit,
        )
        return pd.DataFrame([prepare_log_record(log.data) for log in logs])
