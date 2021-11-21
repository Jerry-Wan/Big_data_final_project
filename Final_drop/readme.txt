1.Use code in ana_code_sunspots/MapReduce to get the cleaned number of sunspots in each day during the years.
2.The result is shown as a mapreduce output, then I put it as a txt file and send it to Final_Proj/Hive_Table/second_num as the final version for hive table
3.Run the commands in profiling_code_yw3743/hive_command.txt, which will create one main table and three sub tables presenting each cycle we think there might have
4.After creating the table, we calculate the max, min and average number of sunspots in each cycle.
5.Results of these table commands can be found in the screenshots.

6. For the Lomnicky folder, first use shell scripts written in the data_merge.py to create a file containning all folders name inside the years_data folder named years.txt
7. Use data_merge.py to get two files: total_data.txt and total_year.txt
8. Use total_years.txt for the futher works