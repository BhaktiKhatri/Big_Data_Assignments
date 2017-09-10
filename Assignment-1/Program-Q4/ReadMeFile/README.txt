Save the jar file in some location. Here, for example, location is C: drive.
Open cmd to upload jar in Linux first. 
Write C:
Write-> pscp "C:/mapsidejoin.jar" brk160030@csgrads1.utdallas.edu:/home/010/b/br/brk160030
Enter the Net ID's Password.
Now open Putty and write csgrads1.utdallas.edu in Host Name and click on Open button.
Login as: brk160030
Enter Password
ssh brk160030@cs6360.utdallas.edu
Enter Password
hdfs dfs -rmr outputdata
hdfs dfs -put business.csv /user/brk160030
hdfs dfs -put mapsidejoin.jar /user/brk160030
To RUN: hadoop jar mapsidejoin.jar FourthProgram.MapSideJoin /user/brk160030/review.csv /user/brk160030/outputdata
hdfs dfs -cat /user/brk160030/outputdata/*
hdfs dfs -cat /user/brk160030/outputdata/part-r-00000
Copy output file from hdfs to linux: hdfs dfs -get /user/brk160030/outputdata /home/010/b/br/brk160030
Copy output file from Linux to your system using WinSCP