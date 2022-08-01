from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

    
def _create_list():
    list1=[]
    for i in range(200):
        list1.append(randint(1,50))
    return list1

def _pass_list(ti):
    list1=ti.xcom_pull(task_ids='Create_List')
    list2=[]
    for i in range(len(list1)):
        if(list1[i]>25):
            list2.append(list1[i])
    return list2

def _fail_list(ti):
    list1=ti.xcom_pull(task_ids='Create_List')
    list2=[]
    for i in range(len(list1)):
        if(list1[i]<=25):
            list2.append(list1[i])
    return list2

def _class_quality(ti):
    list1=ti.xcom_pull(task_ids='Pass_List')
    list2=ti.xcom_pull(task_ids='Fail_List')
    if(len(list1) >= len(list2)):
        return 'Good_Class'
    return 'Bad_Class'


with DAG("anup_dag_4", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        Create_List = PythonOperator(
            task_id="Create_List",
            python_callable=_create_list
        )
        Pass_List = PythonOperator(
            task_id="Pass_List",
            python_callable=_pass_list
        )
        Fail_List = PythonOperator(
            task_id="Fail_List",
            python_callable=_fail_list
        )
        Class_Quality = PythonOperator(
            task_id="Class_Quality",
            python_callable=_class_quality
        )
        Good_Class = BashOperator(
            task_id="Good_Class",
            bash_command="echo ' It is a good class' "
        )
        Bad_Class = BashOperator(
            task_id="Bad_Class",
            bash_command="echo 'It is a bad class' "
        )

        Create_List >> [Pass_List,Fail_List] >> Class_Quality >> [Good_Class,Bad_Class]
        

