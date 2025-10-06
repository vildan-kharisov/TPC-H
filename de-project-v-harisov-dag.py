import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

SUBMIT_NAME = ["submit_customers","submit_lineitems","submit_orders","submit_parts","submit_suppliers"]
QUERIES = {
    SUBMIT_NAME[0]: """DROP EXTERNAL TABLE IF EXISTS "v-harisov".customers;
			 CREATE EXTERNAL TABLE "v-harisov".customers (
				R_NAME TEXT,
				N_NAME TEXT,
				C_MKTSEGMENT TEXT,
				unique_customers_count BIGINT,
				avg_acctbal FLOAT8,
				mean_acctbal FLOAT8,
				min_acctbal FLOAT8,
				max_acctbal FLOAT8
				)
				LOCATION ('pxf://de-project/v-harisov/customers_report?PROFILE=s3:parquet&SERVER=default')
				ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';""",
    SUBMIT_NAME[1]: """DROP EXTERNAL TABLE IF EXISTS "v-harisov".lineitems;
			 CREATE EXTERNAL TABLE "v-harisov".lineitems (
				L_ORDERKEY BIGINT,
				count BIGINT,
				sum_extendprice FLOAT8,
				mean_discount FLOAT8,
				mean_tax FLOAT8,
				delivery_days FLOAT8,
				A_return_flags BIGINT,
				R_return_flags BIGINT,
				N_return_flags BIGINT
				)
				LOCATION ('pxf://de-project/v-harisov/lineitems_report?PROFILE=s3:parquet&SERVER=default')
				ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';""",
	SUBMIT_NAME[2]: """DROP EXTERNAL TABLE IF EXISTS "v-harisov".orders;
				 CREATE EXTERNAL TABLE "v-harisov".orders (
					O_MONTH TEXT,
					N_NAME TEXT,
					O_ORDERPRIORITY TEXT,
					orders_count BIGINT,
					avg_order_price FLOAT8,
					sum_order_price FLOAT8,
					min_order_price FLOAT8,
					max_order_price FLOAT8,
					f_order_status BIGINT,
					o_order_status BIGINT,
					p_order_status BIGINT
					)
					LOCATION ('pxf://de-project/v-harisov/orders_report?PROFILE=s3:parquet&SERVER=default')
					ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';""",
    SUBMIT_NAME[3]: """DROP EXTERNAL TABLE IF EXISTS "v-harisov".parts;
			 CREATE EXTERNAL TABLE "v-harisov".parts (
				N_NAME TEXT,
				P_TYPE TEXT,
				P_CONTAINER TEXT,
				parts_count BIGINT,
				avg_retailprice FLOAT8,
				size BIGINT,
				mean_retailprice FLOAT8,
				min_retailprice FLOAT8,
				max_retailprice FLOAT8,
				avg_supplycost FLOAT8,
				mean_supplycost FLOAT8,
				min_supplycost FLOAT8,
				max_supplycost FLOAT8
				)
				LOCATION ('pxf://de-project/v-harisov/parts_report?PROFILE=s3:parquet&SERVER=default')
				ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';""",
	SUBMIT_NAME[4]: """DROP EXTERNAL TABLE IF EXISTS "v-harisov".suppliers;
				 CREATE EXTERNAL TABLE "v-harisov".suppliers (
					R_NAME TEXT,
					N_NAME TEXT,
					unique_supplers_count BIGINT,
					avg_acctbal FLOAT8,
					mean_acctbal FLOAT8,
					min_acctbal FLOAT8,
					max_acctbal FLOAT8
					)
					LOCATION ('pxf://de-project/v-harisov/suppliers_report?PROFILE=s3:parquet&SERVER=default')
					ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';""",


}

def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


with DAG(
    dag_id="de-project-v-harisov-dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 9, 10, tz="UTC"),
    tags=["kharisov", "vildan"],
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    submit_customers = _build_submit_operator(
        task_id=SUBMIT_NAME[0],
        application_file='customers_submit.yaml',
        link_dag=dag
    )

    submit_lineitems = _build_submit_operator(
        task_id=SUBMIT_NAME[1],
        application_file='lineitems_submit.yaml',
        link_dag=dag
    )
    submit_orders = _build_submit_operator(
        task_id=SUBMIT_NAME[2],
        application_file='orders_submit.yaml',
        link_dag=dag
    )
    submit_parts = _build_submit_operator(
        task_id=SUBMIT_NAME[3],
        application_file='parts_submit.yaml',
        link_dag=dag
    )
    submit_suppliers = _build_submit_operator(
        task_id=SUBMIT_NAME[4],
        application_file='suppliers_submit.yaml',
        link_dag=dag
    )

    sensor_customers = _build_sensor(
        task_id='sensor_customers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME[0]}')['metadata']['name']}}}}",
        link_dag=dag
    )
    sensor_lineitems = _build_sensor(
        task_id='sensor_lineitems',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME[1]}')['metadata']['name']}}}}",
        link_dag=dag
    )
    sensor_orders = _build_sensor(
        task_id='sensor_orders',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME[2]}')['metadata']['name']}}}}",
        link_dag=dag
    )
    sensor_parts = _build_sensor(
        task_id='sensor_parts',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME[3]}')['metadata']['name']}}}}",
        link_dag=dag
    )
    sensor_suppliers = _build_sensor(
        task_id='sensor_suppliers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME[4]}')['metadata']['name']}}}}",
        link_dag=dag
    )
    customers_datamart = SQLExecuteQueryOperator(
        task_id='customers_datamart',
        conn_id=GREENPLUM_ID,
        sql=QUERIES[SUBMIT_NAME[0]],
        split_statements=True,
        autocommit=True,
        return_last=False,
    )
    lineitems_datamart = SQLExecuteQueryOperator(
        task_id='lineitems_datamart',
        conn_id=GREENPLUM_ID,
        sql=QUERIES[SUBMIT_NAME[1]],
        split_statements=True,
        autocommit=True,
        return_last=False,
    )
    orders_datamart = SQLExecuteQueryOperator(
        task_id='orders_datamart',
        conn_id=GREENPLUM_ID,
        sql=QUERIES[SUBMIT_NAME[2]],
        split_statements=True,
        autocommit=True,
        return_last=False,
    )
    parts_datamart = SQLExecuteQueryOperator(
        task_id='parts_datamart',
        conn_id=GREENPLUM_ID,
        sql=QUERIES[SUBMIT_NAME[3]],
        split_statements=True,
        autocommit=True,
        return_last=False,
    )
    suppliers_datamart = SQLExecuteQueryOperator(
        task_id='suppliers_datamart',
        conn_id=GREENPLUM_ID,
        sql=QUERIES[SUBMIT_NAME[4]],
        split_statements=True,
        autocommit=True,
        return_last=False,
    )


    start >> submit_customers >> sensor_customers >> customers_datamart >> end
    start >> submit_lineitems >> sensor_lineitems >> lineitems_datamart >> end
    start >> submit_orders >> sensor_orders >> orders_datamart >> end
    start >> submit_parts >> sensor_parts >> parts_datamart>> end
    start >> submit_suppliers >> sensor_suppliers >> suppliers_datamart >> end
