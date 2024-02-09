


We can use a Bash script to glue python scripts together and run a pipeline
- This isn't convenient, as we have to think about retry logic etc. 


DAG - specifies the dependencies between a set of tasks with explicit execution order
- Task - a defined unit of work aka operators in airflow
    - e.g. fetching data, running analyses, triggering other systems, ec. 
- DAG run - individual execution of a DAG
- task instances - an individual run of a single task
    - task instances have an indicative state e.g. running, failed, skipped, success, up for retry


Types of task:
1. Operators - pre-defined task with paramaeters that you can include. Are an abstraction
2. Sensors - wait for an external event to happen3. 
3. @task flow decorator - recommended over the class PythonOperator to execute Python callables with no template rendering in its arguments

All of these are subclasses of airflow's base operator 

Best practice is to have atomic operators - can stand on their own and don't need to share resources with other operators 