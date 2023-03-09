#!/bin/bash

echo "*** Started Execution *****"

echo "project1.py"
spark-submit "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/project1.py"

if [ $? -eq 0 ]
then
    echo "executing project2.py"
    spark-submit "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/project2.py"
    if [ $? -eq 0 ]
    then 
        echo "executing prject3.py"
        spark-submit --jars "/home/miles/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar" "/mnt/c/Users/miles/Documents/futurense-dataengg-bootcamp/assignments/pyspark/Bank_Analysis/project3.py"
        if [ $? -eq 0 ]
        then
            echo  "All Jobs done"
        else
            echo "============== ERROR in bank_transformation.py  ==================="
        fi
    else
        echo "================ ERROR in bank_cleaning.py  ===================="
    fi 
else
    echo "================ ERROR in FILE bank_load.py =================="
fi
echo "Good Work :)"
