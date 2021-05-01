# Build An Airflow Data Pipeline to Monitor Errors

towards data science:  
[Step by step: build a data pipeline with Airflow](https://towardsdatascience.com/step-by-step-build-a-data-pipeline-with-airflow-4f96854f7466)

---

![Workflow](./screenshots/workflow_monitor_errors.png)

![Build an Airflow data pipeline to monitor errors](./screenshots/monitor_errors_flow.gif)

## References
https://github.com/puckel/docker-airflow


conda activate py382

docker-compose -f ./docker-compose-LocalExecutor.yml up

docker-compose -f ./docker-compose-LocalExecutor.yml down

http://localhost:8080