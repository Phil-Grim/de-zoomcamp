FROM prefecthq/prefect:2.77-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY 03_deployments/parameterized_flow.py /opt/prefect/03_deployments/parameterized_flow.py
COPY 03_deployments/data /opt/prefect/03_deployments/data