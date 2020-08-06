""" airflow_management_plugin Module """
import logging

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable, DagBag
from airflow.operators.subdag_operator import SubDagOperator as AirflowSubDagOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

LOGGER = logging.getLogger(__name__)


class SubDagOperator(AirflowSubDagOperator):
    """ SubDagOperator Class

    This is an overload of the airflow base clase 'SubDagOperator'
    to share running parent dag conf with his sub-dags
    """
    def __init__(self, *args, required_keys=None, optional_keys=None, **kwargs):
        self.required_keys = required_keys or list()
        self.optional_keys = optional_keys or list()

        super(SubDagOperator, self).__init__(
            *args,
            on_retry_callback=SubDagOperator.callback_subdag_clear,
            **kwargs
        )

    def execute(self, context):
        super(SubDagOperator, self).execute(context=context)
        self.manage_output_conf(context=context)

    def manage_output_conf(self, context):
        for key in self.required_keys:
            self.log.info("[XCOM][PULL] from subdag id '%s', key '%s'",
                          self.subdag.dag_id, key)
            value = self.xcom_pull(context=context, dag_id=self.subdag.dag_id, key=key)

            if not value:
                raise AirflowException(f"None value for required key '{key}'!")

            self.log.info("[XCOM][PUSH] '%s': '%s'", key, value)
            self.xcom_push(context=context, key=key, value=value)

        for key in self.optional_keys:
            self.log.info("[XCOM][PULL] from subdag id '%s', key '%s'",
                          self.subdag.dag_id, key)
            value = self.xcom_pull(context=context, dag_id=self.subdag.dag_id, key=key)

            if not value:
                self.log.warning(f"None value for optional key '{key}'!")
                continue  # Ignore key/value because it is optional

            self.log.info("[XCOM][PUSH] '%s': '%s'", key, value)
            self.xcom_push(context=context, key=key, value=value)

    @staticmethod
    def callback_subdag_clear(context):
        """
        callback_subdag_clear Function

        Clears a subdag's tasks on retry.
        Dag is not able to retry cleanly so the hack is to clear sudag before each retry
        """
        dag_id = "{}.{}".format(
            context['dag'].dag_id,
            context['ti'].task_id,
        )
        execution_date = context['execution_date']
        sdag = DagBag().get_dag(dag_id)
        LOGGER.info("Clearing SubDag: %s %s", dag_id, execution_date)
        sdag.clear(
            start_date=execution_date,
            end_date=execution_date,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=False)


class ConfManagementOperator(BaseOperator):
    def __init__(self, *args, parent_dag_id=None, required_keys=None, optional_keys=None,
                 retries=0, **kwargs):
        super(ConfManagementOperator, self).__init__(
            *args,
            retries=retries,  # Don't need retries for this operator
            **kwargs)
        self.required_keys = required_keys or list()
        self.optional_keys = optional_keys or list()
        self.parent_dag_id = parent_dag_id

    def execute(self, context):
        # GET DEFAULT VARIABLES
        variable_confs = Variable.get(f"DEFAULT_CONF_{context['dag_run'].dag_id}",
                                      deserialize_json=True, default_var=dict())
        self.log.info("[VARIABLES] '%s'", variable_confs)

        # GET LOCAL CONF
        payload_confs = context['dag_run'].conf or dict()
        self.log.info("[CONFS] '%s'", payload_confs)

        # PULL XCOM IF PARENT DAG
        xcom_confs = dict()
        for key in [*self.required_keys, *self.optional_keys]:
            self.log.info("[XCOM][PULL] from parent dag id '%s', key '%s'",
                          self.parent_dag_id, key)
            value = self.xcom_pull(context=context, dag_id=self.parent_dag_id, key=key)
            self.log.info("[XCOM][PULL] '%s': '%s'", key, value)
            if value:
                xcom_confs[key] = value

        # CONCATENATE ALL CONFS
        complete_confs = dict()
        complete_confs.update(variable_confs)
        complete_confs.update(payload_confs)
        complete_confs.update(xcom_confs)

        # PUSH REQUIRED CONFS
        for key in self.required_keys:
            value = complete_confs.get(key)
            if not value:
                raise AirflowException(f"None value for required key '{key}'!")
            self.log.info("[XCOM][PUSH] '%s': '%s'", key, value)
            self.xcom_push(context=context, key=key, value=value)

        # PUSH OPTIONAL CONFS
        for key in self.optional_keys:
            value = complete_confs.get(key)
            if not value:
                self.log.warning(f"None value for optional key '{key}'!")
                continue  # Ignore key/value because it is optional
            self.log.info("[XCOM][PUSH] '%s': '%s'", key, value)
            self.xcom_push(context=context, key=key, value=value)

        # IF NOT optional_keys AND NOT optional_keys PUSH ALL CONFS
        if not self.optional_keys and not self.required_keys:
            for key, value in complete_confs.items():
                self.log.info("[XCOM][PUSH] '%s': '%s'", key, value)
                self.xcom_push(context=context, key=key, value=value)


class FinalStatusOperator(BaseOperator):
    def __init__(self, *args, retries=0, trigger_rule=TriggerRule.ALL_DONE, **kwargs):
        super(FinalStatusOperator, self).__init__(
            *args,
            retries=retries,  # Don't need retries for this operator
            trigger_rule=trigger_rule,  # Ensures this task runs even if upstream fails
            **kwargs
        )

    def execute(self, context):
        """ check_all_status function """
        for task_instance in context['dag_run'].get_task_instances():
            if task_instance.current_state() in (State.FAILED,) and \
                    task_instance.task_id != context['task_instance'].task_id:
                raise AirflowException(
                    f"Task {task_instance.task_id} is in state '{task_instance.current_state()}'. "
                    f"Failing this DAG run"
                )


class AirflowManagementPlugin(AirflowPlugin):
    """ AirflowManagementPlugin Class """
    name = 'airflow_management_plugin'
    operators = [
        BaseDbOperator,
        BaseDbSensorOperator,
        SubDagOperator,
        ConfManagementOperator,
        FinalStatusOperator,
    ]
