from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.subprocess import SubprocessHook
from datetime import datetime
import subprocess

def excuteC():
	s = SubprocessHook.run_command(self,"gcc 3.c -o out1;./out1",env=None, output_encoding='utf-8', cwd=None)
	#s = subprocess.check_call("gcc 3.c -o out1;./out1", shell = True)
	print(", return code", s)

def executeCpp():
	s = subprocess.check_output("g++ 2.cpp -o out2;./out2", shell = True)
	print(s.decode("utf-8"))

def Execute_Python1():
    print("THIS IS PYTHON1 OUTPUT")

def Execute_Python2():
    print("THIS IS PYTHON2 OUTPUT")

with DAG("anup_dag_5", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        Execute_Python11 = PythonOperator(
            task_id="Execute_Python11",
            python_callable=Execute_Python1
        )
        Execute_C = PythonOperator(
            task_id="Execute_C",
            python_callable=excuteC
        )
        Execute_CPP = PythonOperator(
            task_id="Execute_CPP",
            python_callable=executeCpp
        )
        Execute_Python12 = PythonOperator(
            task_id="Execute_Python12",
            python_callable=Execute_Python2
        )

        Execute_Python11 >> [Execute_C,Execute_CPP] >> Execute_Python12
