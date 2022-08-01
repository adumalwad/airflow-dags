from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

    
def _create_list():
    list1=[]
    for i in range(12):
        list1.append(randint(1,200))
    return list1

def _max_by_month(ti):
    ca=ti.xcom_pull(task_ids='Company_A')
    cb=ti.xcom_pull(task_ids='Company_B')
    cc=ti.xcom_pull(task_ids='Company_C')
    cd=ti.xcom_pull(task_ids='Company_D')
    list2=[]
    for i in range(12):
        m=max(ca[i],max(cb[i],max(cd[i],cc[i])))
        if(m==ca[i]):
            list2.append("Company_A_"+str(m))
        elif(m==cb[i]):
            list2.append("Company_B_"+str(m))
        elif(m==cc[i]):
            list2.append("Company_C_"+str(m))
        else:
            list2.append("Company_D_"+str(m))
    return list2

def _sum_A(ti):
    ca=ti.xcom_pull(task_ids='Company_A')
    return sum(ca,0)

def _sum_B(ti):
    cb=ti.xcom_pull(task_ids='Company_B')
    return sum(cb,0)

def _sum_C(ti):
    cc=ti.xcom_pull(task_ids='Company_C')
    return sum(cc,0)

def _sum_D(ti):
    cd=ti.xcom_pull(task_ids='Company_D')
    return sum(cd,0)

def _best_sale(ti):
    list3=ti.xcom_pull(task_ids=['sum_a','sum_b','sum_c','sum_d'])
    m=max(list3)
    if(m==list3[0]):
        return 'best_A'
    elif(m==list3[1]):
        return 'best_B'
    elif(m==list3[2]):
        return 'best_C'
    return 'best_D'

with DAG("anup_dag_3", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        Company_A = PythonOperator(
            task_id="Company_A",
            python_callable=_create_list
        )
        Company_B = PythonOperator(
            task_id="Company_B",
            python_callable=_create_list
        )
        Company_C = PythonOperator(
            task_id="Company_C",
            python_callable=_create_list
        )
        Company_D = PythonOperator(
            task_id="Company_D",
            python_callable=_create_list
        )
        Max_Month = PythonOperator(
            task_id="Max_Month",
            python_callable=_max_by_month
        )
        Total_Sale_A = PythonOperator(
            task_id="sum_a",
            python_callable=_sum_A
        )
        Total_Sale_B = PythonOperator(
            task_id="sum_b",
            python_callable=_sum_B
        )
        Total_Sale_C = PythonOperator(
            task_id="sum_c",
            python_callable=_sum_C
        )
        Total_Sale_D = PythonOperator(
            task_id="sum_d",
            python_callable=_sum_D
        )
        Best_Company = PythonOperator(
            task_id="Best_Company",
            python_callable=_best_sale
        )
        best_A = BashOperator(
            task_id="best_A",
            bash_command= "echo 'A is the best' "
        )
        best_B = BashOperator(
            task_id="best_B",
            bash_command= "echo 'B is the best' "
        )
        best_C = BashOperator(
            task_id="best_C",
            bash_command= "echo 'C is the best' "
        )
        best_D = BashOperator(
            task_id="best_D",
            bash_command= "echo 'D is the best' "
        )

        Company_A >> Company_B >> Company_C >> Company_D >> Max_Month >> [Total_Sale_A,Total_Sale_B,Total_Sale_C,Total_Sale_D] >> Best_Company >> [best_A,best_B,best_C,best_D]

