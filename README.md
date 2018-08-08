# map-reduce
map reduce example on hadoop <br>
Check doc for exercises

## Steps to run code on hadoop
- move dataset to hadoop <br>
```sh
  hdfs dfs -mkdir fragma-data
  hdfs dfs -put matches.csv fragma-data
  hdfs dfs -put deliveries.csv fragma-data
 ```
  
- build code using maven
```sh
   mvn clean && mvn install
   ```
   
- for "Top 4 teams which elected to field first after winning toss in the year 2016 and 2017." run below command:
```sh
  yarn jar ./fragmadata.test.jar fragmadata.question1.TopTeamMapReduce fragma-data/matches.csv fragma-data/top-4-team && hdfs dfs -cat fragma-data/top-4-team/*
  ```
  
- for "List total number of fours, sixes, total score with respect to team and year." run below command:
```sh
  yarn jar ./fragmadata.test.jar fragmadata.question2.TeamRun fragma-data/matches.csv fragma-data/deliveries.csv fragma-data/year-wise-team-run && hdfs dfs -cat fragma-data/year-wise-team-run/*
  ```
  
- for "Top 10 best economy rate bowler with respect to year who bowled at least 10 overs ​(​LEGBYE_RUNS and BYE_RUNS should not be considered for Total Runs Given by a bowler)" run below command:
```sh
  yarn jar ./fragmadata.test.jar fragmadata.question3.YearwiseTop10Bowler fragma-data/matches.csv fragma-data/deliveries.csv fragma-data/top-bowlers && hdfs dfs -cat fragma-data/top-bowlers/*
  ```
