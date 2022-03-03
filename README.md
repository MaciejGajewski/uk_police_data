# Project description
```
Project consist of 2 modules:
1. main-etl.py: ELT Engine which simulates ELT process which Extracts (done here manually by downloading zip dataset), Loads & Transforms data into a 
   Data Lake simulated by table in parquet format
2. main-services.py: KPI Engine which simulates microservices framework serving data in json format for specific uses cases (here printing 
   tp the screen)
   
Project uses spark to perform computations      
```

# Run Instruction

```
1 .Download https://data.police.uk/data/ dataset as in the instruction and unzip the content into landing_zone foler simulating data source
2. Open terminal and navigate to the project folder
3a. Run "docker compose up"  # TODO - currently gives some permision errors when writing data: 
    chmod: changing permissions of '/opt/application/uk_police_data': Operation not permitted
3b. Run "docker run -u root uk_app driver local:///opt/application/main_etl.py" to run etl module   # TODO - run main_service.py afterwards
3c. Running manually on Windows:
 - install python, make
 - install Java & set JAVA_HOME
 - download winutils.exe and place in C:\hadoop\bin\
 - Set HADOOP_HOME enironment variable to C:\hadoop
 - Add C:\hadoop\bin to the PATH 
 - Run make in project folder:
    - make run-etl
    - make run-services
```
