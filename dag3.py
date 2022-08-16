from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import time
import logging

from random import randint
from datetime import datetime

def _create_list():
    list1=[]
    for i in range(1000):
        logging.info("Adding Element")
        time.sleep(0.1)
        #list1.append(25)
        list1.append(randint(1,100))
    return list1

def _print_list(ti):
    list1=ti.xcom_pull(task_ids='Create_List')
    for i in range(len(list1)):
        print(list1[i]," ")

def _sort_list(ti):
    list1=ti.xcom_pull(task_ids='Create_List')
    list1.sort()
    return list1

def _print_mean(ti):
    list1=ti.xcom_pull(task_ids='Sort_List')
    print(sum(list1,0)/len(list1))
    return sum(list1,0)/len(list1)

def _print_median(ti):
    list1=ti.xcom_pull(task_ids='Sort_List')
    median1=0
    n=len(list1)
    if(n%2==0):
        median1=(list1[n//2]+list1[(n//2)-1])/2
    else:
        median1=list1[n//2]
    print(median1)
    return median1

def _is_equal(ti):
    mean=ti.xcom_pull(task_ids='Find_Mean')
    median=ti.xcom_pull(task_ids='Find_Median')
    if(mean==median):
        return 'Equal'
    return 'Not_Equal'


with DAG("anup_dag_2", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False,max_active_runs=30) as dag:

        Create_List = PythonOperator(
            task_id="Create_List",
            python_callable=_create_list
        )
        Print_List = PythonOperator(
            task_id="Print_List",
            python_callable=_print_list
        )
        Sort_List = PythonOperator(
            task_id="Sort_List",
            python_callable=_sort_list
        )
        Find_Mean = PythonOperator(
            task_id="Find_Mean",
            python_callable=_print_mean
        )
        Find_Median = PythonOperator(
            task_id="Find_Median",
            python_callable=_print_median
        )
        Is_Equal = BranchPythonOperator(
            task_id="Is_Equal",
            python_callable=_is_equal
        )
        Equal = BashOperator(
            task_id="Equal",
            bash_command="echo 'equal' "
        )
        Not_Equal =BashOperator(
            task_id="Not_Equal",
            bash_command="echo 'Not equal' "
        )

        Create_List >> Print_List >> Sort_List >> [Find_Mean,Find_Median] >> Is_Equal >> [Equal,Not_Equal]


        
