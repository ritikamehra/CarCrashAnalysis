# CarCrashAnalysis
## Goal: 
Develop a spark application that analyzes the given data on the following points: 
1. Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
2. Analysis 2: How many two wheelers are booked for crashes?
3. Analysis 3: Which state has highest number of accidents in which females are involved?
4. Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
5. Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
7. Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Assumptions:
- AWS EMR has been used for implementation.

Setup
1. Create EMR Cluster with Hadoop and Spark, 1 m4x.large Master Node with 400 GiB EBS Storage and 20 GiB root device volume size.
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/7d4251c4-dbfb-4358-b347-0012230dcbbd)
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/bb681ffd-5c40-4def-bb11-b72e9f60d7bd)
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/48b28929-a85e-4bcd-8593-81558c7c19ee)

2. Once the Cluster is running, copy the Master DNS and login to WinSCP using key.
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/309e3ded-0a6b-4234-8199-1e1b87ba66c3)

3. Unzip Data.zip and place the Data Folder, utilities folder, config.json and main.py file in the /home/hadoop/ directory.
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/fd92393d-a652-4ca7-88e7-c69c2879f692)

4. Login to EMR cluster using putty and run the following command to place Data files in HDFS.
hadoop fs -copyFromLocal Data/ /user/hadoop/

5. Run the following spark command to run the application and store the console output in a file.
spark-submit --py-files utilities/utility.py --files config.json main.py > output.txt

6. Run the following command to copy the output to local system which will be visible in WinSCP.
hadoop fs -copyToLocal Output/   

![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/9af29e4d-0cb4-4507-8fa7-28ef98370c6e)
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/83027f31-0475-457a-a2b3-08b22aa35d4c)

Files should be visible in WinSCP as below.
![image](https://github.com/ritikamehra/CarCrashAnalysis/assets/54076372/26e81029-a78a-4cf6-a92f-a5042724d88c)

Output files can now be copied to local system.


