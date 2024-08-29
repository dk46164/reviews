# Review Batch Pipeline 

## Prerequisites

- Docker or Docker Desktop should be installed on your system & make sure docker deamon is running 

## Building the Docker Image

###  Nabvigate to application </app directory >
```bash
cd <absolute path of Dockerfile/path where main application is persent >
```

### Build the Docker image using the following command:
```bash
docker build . -t dask_dev 
```

### run the batch load  
```bash 
docker run -d -v <absoulte_data_path>:/app/data/ -e input=/app/data/review -e inappropriate_words=/app/data/inappropriate_words -e output=/app/data/output -e aggregations=/app/data/aggregations -p 8787:8787 <name of the image/image_id>
```

### monitor batch load
```bash
docker logs -f <container_id>
```


## Directroy Structure 

### 1. compute_utils
###### parse_json.py             --> parallely parse the files with dask cluster 
###### cluster_init.py           --> Intialize the dask cluster 
###### review_transformation.py  --> Apply transformation Logics

### 2. fs_utils
###### file_utils.py             --> get all and latest files from folder

### 3. logs_utils
###### dask_logger.py            --> intialize the logger for the dask worker  and scheduler

### 4. logs
###### logs                      --> Storing logs 

### 5. docs
###### design , sytem architecture, data flow ,deployment user guide




## All design Documents is Present inside ./docs
## Docs has two Pictures in PNG and SVG format, please open them  in browser


gitGraph
    commit
    branch develop
    checkout develop
    commit
    commit
    checkout main
    merge develop
    commit
    branch hotfix
    checkout hotfix
    commit
    checkout main
    merge hotfix
    checkout develop
    merge hotfix
    branch feature
    checkout feature
    commit
    commit
    checkout develop
    merge feature
    commit
    checkout main
    merge develop

