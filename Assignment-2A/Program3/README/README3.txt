Save the input file(business.csv,review.csv) in some location. Here, for example, location is C: drive.
Open cmd to upload input files in linux first.
Write C:
Write-> pscp "C:/business.csv" brk160030@csgrads1.utdallas.edu:/home/010/b/br/brk160030
Enter the Net ID's Password.
Write-> pscp "C:/review.csv" brk160030@csgrads1.utdallas.edu:/home/010/b/br/brk160030
Enter the Net ID's Password.
Now open Putty and write csgrads1.utdallas.edu in Host Name and click on Open button.
Login as: brk160030
Enter the Net ID's Password.
ssh brk160030@cs6360.utdallas.edu
Enter the Net ID's Password.
hdfs dfs -put business.csv /user/brk160030
hdfs dfs -put review.csv /user/brk160030
If Pig Path is not added then write lines 16-21 else go to 22
vi .bash_profile
export PIG_HOME=/usr/local/pig-0.13.0
export PATH=$PIG_HOME/bin:$PATH
esc :wq
vi .source_profile
esc :wq
pig
Now type the source from file question3program.pig
After dump F;
Remove the outputfile if it exists by: fs -rmr /user/brk160030/program3output;
Store the result into a file to your directory by: store F into '/user/brk160030/program3output';
The file is stored at HDFS. The file can be viewed by: fs -cat /user/brk160030/program3output/part-r-00000;
Store the file from HDFS to Linux by: fs -get /user/brk160030/program3output /home/010/b/br/brk160030;
Copy the output file 'program3output' from Linux to your system using WinSCP.
Click on part-r-00000 file inside program3output to view the output.



