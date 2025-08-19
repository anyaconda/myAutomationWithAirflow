
# Anya HelloWorld DAG Project

This repository contains example Apache Airflow DAGs and related files for learning and testing Airflow workflows.

## Project Structure

- `dags/` - Contains Airflow DAG Python scripts:
  - `helloWorld` dags - Simple Hello World DAGs.


- `tests/` - Contains test scripts:
  - `test_examples.py` - Tests for example DAGs.
  - `test_requirements.txt` - Requirements for running tests.
- `start.sh` - Shell script to start or initialize the project (usage details below).
- `README.md` - Project documentation.

---

## [Original] Getting Started

1. **Clone the repository:**
    ```bash
    git clone <repo-url>
    cd anya-helloworld-dag
    ```
2. **Install dependencies:**
    - Make sure you have Python and Apache Airflow installed.
    - Install any additional requirements from `test_requirements.txt` if running tests.

3. **Run Airflow:**
    - Place the DAGs in your Airflow `dags` folder or configure Airflow to use this directory.
    - Start Airflow webserver and scheduler as per your setup.

4. **Run the start script (if applicable):**
    ```bash
    bash start.sh
    ```
    *(Check the script for specific usage instructions.)*

## Testing

- Run tests using your preferred Python test runner (e.g., `pytest`).
- Example:
  ```bash
  pytest tests/test_examples.py
  ```

## License

This project is for educational and demonstration purposes.

Basic Development steps:
1. Develop your DAG code in **dags/** directory. (Recommended)
2. Develop pytest unit tests in the **tests/** directory. (Recommended)
3. Run [pytest](https://docs.pytest.org/en/stable/) locally in IDE terminal.
    ```bash
    py.test tests -vv --durations=0
    ```
4. Push code to qa remote directly OR submit a merge request for your branch into qa.
5. View Pipeline status by navigating to **Build->Pipelines** under this project in GitLab.
6. If you created a merge request in step 4, you will need to action (merge) the merge request to deploy to QA at this point.
7. Repeat steps 1-6 until you are satisfied with your code, tests, and pipeline results.

8. Run and test DAGs in QA Airflow Webserver UI.
9. Repeat 1-8 until project is ready for production deployment.

### 6. Production Deployment
removed

## Additional Resources

External:
- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)


