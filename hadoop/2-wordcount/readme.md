# Hadoop WordCount Tutorial

## Project Setup
1. Open Eclipse IDE
2. Create a new Java Project:
   - Go to `File -> New -> Java Project`
   - Enter project name
3. Create a new Package:
   - Right-click on project name
   - Select `New -> Package`
   - Enter package name
4. Create a new Class:
   - Right-click on package name
   - Select `New -> Class`
   - Enter class name

## Configuration
1. Add Hadoop Dependencies:
   - Right-click on Project Name
   - Go to `Build Path -> Add External Archives`
   - Add required Hadoop JARs

## Creating JAR File
1. Export the project:
   - Right-click on Project Name
   - Select `Export -> JAR file`
   - Follow the wizard to create JAR

## Running the WordCount Program

### Upload Data to HDFS
```bash
# Upload input file to HDFS
hadoop fs -put <filename> <input-path in hdfs>
hadoop fs -put wordcount.txt wordcountfile

# Verify the file upload
hadoop fs -ls
```

### Execute MapReduce Job
```bash
# Run the WordCount program
hadoop jar <jar-file-name> <package-name>.<class-name> <input-path in hdfs> <output-path>
hadoop jar wordcount.jar WordCount.wordcount wordcountfile MRDir1

# List the output directory
hadoop fs -ls <output-path>
hadoop fs -ls MRDir1

# View the results
hadoop fs -cat MRDir1/part-r-00000
```

**Note**: Replace `wordcount.jar`, `WordCount.wordcount`, and file paths as per your specific configuration.
