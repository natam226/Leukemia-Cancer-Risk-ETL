# Health Data Analytics | ETL, Quality & Power BI

**An academic workflow for preparing, validating and exploring a public health dataset.**

Collaborative project using a Kaggle dataset containing **143,194 records across 22 countries**. The workflow connects data integration, dimensional storage, quality checks and reporting, with a Kafka streaming demonstration.

**[Explore the analysis](notebooks/)** · [Review the Airflow DAG](airflow/dags/dag_etl.py) · [View dashboard files](dashboard/) · [Review transformations and tests](tests/)

## Analytical questions

- How are the dataset's variables distributed across countries and demographic groups?
- Which schema, missing-value and range checks are needed before reporting?
- How can a repeatable data pipeline support consistent analytical views?

## Data flow

~~~mermaid
flowchart LR
    A[Public dataset and API] --> B[Airflow ETL]
    B --> C[Quality checks]
    C --> D[PostgreSQL dimensional model]
    D --> E[Power BI reporting]
    D --> F[Kafka streaming demonstration]
~~~

## What this project demonstrates

| Area | Implementation |
| --- | --- |
| Data preparation | Python transformations and enrichment from an external API |
| Orchestration | Apache Airflow tasks and dependencies |
| Data quality | Great Expectations checks for schema, null values, ranges and distributions |
| Storage and reporting | PostgreSQL analytical model and Power BI dashboards |
| Streaming | Kafka producer/consumer workflow and a Streamlit view |

**Stack:** Python · SQL · PostgreSQL · Airflow · Great Expectations · Kafka · Power BI · Streamlit.

The records belong to the public learning dataset; they do not represent patients managed by the project team. The analysis is descriptive and educational. Streaming demonstrates the software flow using the dataset, rather than a live clinical integration.

---

## Team and implementation guide

**Realized by**
* [Valentina Bueno Collazos](https://github.com/valentinabc19)

* [Liseth Esmeralda Erazo Varela](https://github.com/memerazo)

* [Natalia Moreno Montoya](https://github.com/natam226)

## Description

This project presents the development of a complete ETL (Extract, Transform, Load) pipeline designed to process, analyze, and deliver insights on leukemia-related risk factors across various global regions. This project used a dataset extracted from [Kaggle](https://www.kaggle.com/datasets/ankushpanday1/leukemia-cancer-risk-prediction-dataset?resource=download) containing leukemia-related health data, which 143,194 patient records from 22 different countries.

The technologies used are:

- *Python* → Core programming language used throughout the project for data extraction, transformation, validation, and integration tasks.
- *Jupyter Notebook* → For the execution and documentation of Python code.
- *PostgreSQL* → Used as the central DB for storing the processed data in a dimensional model optimized for analytical queries.
- *Airflow* → Served as the orchestrator of the ETL pipeline, managing task dependencies and automating the data flow from extraction to loading.
- *PowerBI Desktop* → Used to design and publish interactive dashboards that visualize key leukemia risk indicators and regional trends.
- *Great Expectations* → Integrated into the pipeline to enforce data quality through expectations on schema, null values, ranges, and distributions.
- *Kafka* → Implemented to enable real-time data streaming, allowing a live feed of key leukemia metrics to be consumed and visualized dynamically.

The dependencies used in python are in a `requirements.txt` file

## Dataset information

- **patient_id**: Patient ID is an auto-incremental number.
- **age**: patient's age.
- **gender**: the patient's gender, which could be Female or Male.
- **country**: the patient's country.
- **wbc_count**: white blood cells count. 
- **rbc_count**: red blood cells count.
- **platelet_count**
- **hemoglobin_level**
- **bone_marrow_blasts**
- **genetic_mutation**: indicates whether you have a genetic mutation, set with “yes” or “no”.
- **family_history**: indicates whether you have a family history of leukemia, set with “yes” or “no”.
- **smoking_status**: indicates whether the patient smokes or has smoked, set with “yes” or “no”.
- **alcohol_consumption**: indicates whether the patient consumes or has consumed alcohol, set with “yes” or “no”.
- **radiation_exposure**: indicates whether the patient has been exposed to radiation, set with “yes” or “no”.
- **infection_history**: indicates whether the patient has a history of infections, set with “yes” or “no”.
- **BMI**: is the body mass index.
- **chronic_illness**: indicates whether the patient has a chronic disease, set with “yes” or “no”.
- **immune_disorders:** indicates whether the patient has an immune disorder, set with “yes” or “no”.
- **ethnicity**: indicates ethnicity, such as “A”, “B” or “C”.
- **socioeconomic_status**: indicates the socioeconomic status of the patient, it is set as “Medium”, “Low” and “High”.
- **urban_rural**: indicates whether the patient lives in a rural or urban area.
- **leukemia_status**: indicates whether the patient has leukemia or not, it is set as “Negative” or “Positive”.


## 📂 Project Structure

```
Leukemia-Cancer-Risk-ETL/
├── airflow/                  # Airflow-related files
│   ├── dags/                      
│   │   ├── dag_etl.py        # Airflow DAG
│   ├── functions/            # Folder with all the functions used in the DAG
├── api/                      # API data extraction and EDA
├── dashboard/
├── data/                     # Data storaged
├── kafka/                    # Scripts for the producer and the consumer in the streaming  
├── notebooks/                # Jupyter notebooks
├── tests/                    # Unit tests of the transformations
├── venv/                     # Virtual environment
├── .gitignore                # Git ignore file
├── docker-compose.yml        # docker compose used for the proper functioning of kafka
└── requirements.txt          # Project dependencies
```

## 🛠️ Setup Instructions

### Clone the repository

Execute the following command to clone the repository

```bash
git clone https://github.com/natam226/Leukemia-Cancer-Risk-ETL.git
cd Leukemia-Cancer-Risk-ETL

```
> From this point on all processes are done in Visual Studio Code

### Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Credentials
To make a connection to the database you must have the database credentials in a JSON file called credentials. So this file must be created in the project folder, with the following syntax:

```bash
{
    "db_host": "DB_HOST",
    "db_name": "DB_NAME",
    "db_user": "DB_USER",
    "db_password": "DB_PASSWORD",
    "db_port": "DB_PORT"    
}
```
Ensure this file is included in `.gitignore`.
> Before running the pipeline, make sure the target database is properly created and configured, as it will serve as the destination for storing the processed data. This step is essential to ensure smooth data loading and prevent connection or schema-related issues during execution.

### Installing the dependencies
The necessary dependencies are stored in a file named requirements.txt. To install the dependencies you can use the command
```bash
pip install -r requirements.txt
```

### 4. Configure Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

## 🚀 Usage

### Initialize kafka

Open a terminal in Visual Studio Code and start docker
```bash
docker-compose up -d --build
```

Use this command to see the containers that are running
```bash
docker ps
```

Select the ID of the kafka container and open the bash of this one
```bash
docker exec -it ID bash
```

Run this command to iniatilize the consumer
```bash
kafka-console-consumer --bootstrap-server IDContainer:9092 --topic fact_table --from-beginning
```

### Initialize Airflow

Run this command to initialize airflow
```bash
airflow standalone
```

### Access Airflow UI

Open your browser and go to [http://localhost:8080](http://localhost:8080).  
Use the credentials given to you in Airflow standalone runtime, for example:  
- **Username**: `airflow`  
- **Password**: `airflow`

### Trigger the DAG

- Locate the `leukemia_etl` DAG.
- Turn it **On** and click **"Trigger DAG"**.

### Monitor the Pipeline

- Use the Airflow UI to track task status.
- Logs are available under `airflow/logs/`.

### Output

- The data send to the consumer in turn is sent to a dashboard in Streamlit, which can be accessed by [http://localhost:8501](http://localhost:8501)
---

## 📝 Pipeline Tasks

The `leukemia_etl` DAG includes:

- `extract_leukemia_op`
- `extract_api_op`
- `process_api_op`
- `merge_op`
- `transform_op`
- `validate_op`
- `load_op`
- `kafka_op`
