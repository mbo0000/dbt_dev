# Data Build Tool(dbt)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-%23f3f1ff)

## Introduction

Welcome to the NBA dbt repository. This project will cover the T part in the ELT data pipeline. It is an essential tool that enables seamless NBA data transformation using ingested raw data in Snowflake. It also allows for flexible data modeling (create data easily, backfills easily) and management.

## Project Overview

[Diagram](insert url)
1. **Transform Raw Data**: transforming raw data into clean. 
    - Review Snowflake setup and configuration [here](insert url)
    - Perform data tranformation, processing and moving transformed data from raw into source. 
2. **Structured Database Management**: new data models for analysis, custom macros and data warehouse management.

## Installation
### Prerequisites
- Python 3.10+
- Snowflake account
    - Follow Snowflake setup and management instruction [here](insert url)
    - Default Warehouse: *TRANSFORMING*
    - Default Role: *TRANSFORMER*
- [dbt Cloud](https://www.getdbt.com/product/dbt-cloud) account
    - schedule daily job
    - CI trigger for MR
- Github Repo for this dbt project

### Setup Steps
1. clone dbt project repo
    ```
    git clone https://github.com/mbo0000/dbt_dev.git
    cd dbt_dev
    ```
    Install 'dbt-core' and 'dbt-snowflake' packages from `requirements.txt` file if they are not yet installed. 

2. update `profiles.yml`
    ```
    transforming:
        type: snowflake
        account: "foo"
        user: "foo"
        password: "foo"
        role: "transformer"
        database: "staging"
        warehouse: "transformer"
        schema: "public"
        threads: 1
        client_session_keep_alive: False
    ```
    Replace "foo"s with your account creds. Use target *transforming* for when running locally.

3. Create a Github repo for this project
4. Setup [dbt Cloud](https://docs.getdbt.com/docs/cloud/git/connect-github):
    - Ensure to provide the same credentials as step 2 for Snowflake Connection setup. 
    - Linked dbt Cloud to Github repo and use main branch
    - Create a deploy job for daily schedule run
    - Create a CI job that will be trigger on pull request 

## Usage
dbt Cloud will run daily on the scheduled time config in step 4. To run project locally, run:
```
dbt run -s model_name --target transforming
```

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)