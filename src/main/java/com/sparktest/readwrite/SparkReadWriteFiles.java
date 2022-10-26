package com.sparktest.readwrite;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SparkReadWriteFiles
{
    private static SparkSession sparkSession = null;

    private static final String codec = "parquet";
    private static final String BASE_PATH = String.valueOf(Paths.get("src/main/resources/").normalize().toAbsolutePath());
    private static final String PARQUET_FILE_PATH = BASE_PATH + "/test.parquet";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

    /**
     * Start a local Spark in-memory cluster with 2 threads
     */
    public SparkReadWriteFiles(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }


    public void test(SparkSession sparkSession) throws IOException, ExecutionException, InterruptedException {
        SparkReadWriteFiles inst = new SparkReadWriteFiles(sparkSession);

        // clean up first
        FileUtils.deleteDirectory(new File(PARQUET_FILE_PATH));

        long startTime = System.currentTimeMillis();
        inst.saveToHDFS("output_csv_full.csv", BASE_PATH + "/output_full.parquet",false);

        System.out.println("Time to write csv " + (System.currentTimeMillis() - startTime));

        Dataset<Row> testCsv = inst.readFromHDFSTest(BASE_PATH + "/output_full.parquet",null);
        System.out.println(testCsv.count());


        inst.saveToHDFS("test.csv", PARQUET_FILE_PATH, true);

        // append the data from test_2.csv to the existing Parquet_file_path
        inst.appendToHDFS("test_2.csv", PARQUET_FILE_PATH);

        // read one partition
        inst.readFromHDFS("src/main/resources/test.parquet","2022","9","11");

        // read a different partition
        inst.readFromHDFS("src/main/resources/test.parquet","2022","9","28");

        // below code shows how to do the reads in Async mode and union the results
        CompletableFuture<Dataset<Row>> ft1 = inst.readFromHDFSAsync("src/main/resources/test.parquet","2022","9","28");
        CompletableFuture<Dataset<Row>> ft2 = inst.readFromHDFSAsync("src/main/resources/test.parquet","2022","9","11");

        //create the async jobs but do not run them
        List<CompletableFuture<Dataset<Row>>> com = new ArrayList<>();
        com.add(ft1);
        com.add(ft2);

        // run the jobs and get the results
        List<Dataset<Row>> results2 = inst.sequence2(com);

        // union all the results and display them
        Dataset<Row> allResults= inst.unionAll(results2);

        System.out.println("Total results from Different partitions unioned : " + allResults.count());
        allResults.printSchema();
        allResults.show();

        System.out.println("END of test");
        sparkSession.stop();

    }

    /**
     * Read the partition in async mode
     * @param partitionYear
     * @param partitionMonth
     * @param partitionDay
     * @return
     */
    public  CompletableFuture<Dataset<Row>> readFromHDFSAsync(String filePath, String partitionYear, String partitionMonth, String partitionDay) {
        return CompletableFuture.supplyAsync(() -> readFromHDFS(filePath, partitionYear, partitionMonth, partitionDay), executorService);
    }

    /**
     * Returns a list of completable futures AFTER they have all finished processing.
     * This is a useful method when you have tasks made of subset of tasks and want to join them all later
     * @param listOfReadTasks
     * @param <T>
     * @return
     */
    static<T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> listOfReadTasks) {
        return CompletableFuture.allOf(listOfReadTasks.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> listOfReadTasks.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())

                );
    }


    /**
     * Returns a list of results AFTER they have all finished processing
     * @param listOfReadTasks
     * @param <T>
     * @return
     */
    public <T> List<Dataset<Row>> sequence2(List<CompletableFuture<T>> listOfReadTasks) throws ExecutionException, InterruptedException {
        CompletableFuture<?>[] fanoutRequestList = new CompletableFuture[listOfReadTasks.size()];

        // convert the list to an array so we can wait on the results
        int count = 0;
        for (CompletableFuture<T> msg : listOfReadTasks) {
            fanoutRequestList[count++] = msg;
        }

        // wait for all to complete
        try {
            CompletableFuture.allOf(fanoutRequestList).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // get all the results
        List<Dataset<Row>> mapResult = new ArrayList<>();
        for (int i=0; i< listOfReadTasks.size(); i++) {
            mapResult.add((Dataset<Row>)fanoutRequestList[i].get());
        }
        return mapResult;

    }


    /**
     * Union all results. They should ALL have the same columns but this is easy to check for.
     * @param listOfReadTasks
     * @return
     */
    public Dataset<Row> unionAll(List<Dataset<Row>> listOfReadTasks) {
        Dataset<Row> result = listOfReadTasks.get(0);

        for (int i=1; i<listOfReadTasks.size(); i++) {
            result = result.unionAll(listOfReadTasks.get(i));
        }
        return result;

    }

    /**
     * A generic function to read a parquet file with the given partitions
     * @param partitionYear
     * @param partitionMonth
     * @param partitionDay
     * @return
     */
    public Dataset<Row> readFromHDFS(String filePath, String partitionYear, String partitionMonth, String partitionDay) {
        return sparkSession.read().parquet(filePath + "/y=" + partitionYear + "/m=" + partitionMonth + "/d=" + partitionDay);
    }


    public Dataset<Row> readFromHDFSTest(String filePath,String partitionTimeRef) {
        if (partitionTimeRef == null) {
            return sparkSession.read().parquet(filePath);
        } else{
            return sparkSession.read().parquet(filePath + "/time_ref=" + partitionTimeRef);
        }
    }

    /**
     * Writes the data to the file system
     */
    public void saveToHDFS(String fileName, String parquetFilePath, boolean partition) {
        final String dir = BASE_PATH + "/" + fileName;
        Dataset<Row> ds = sparkSession.read().option("header", true).option("inferSchema", true).csv(dir);


        // how to specify a partition and NOT use the data in the dataset i.e overide it
        String year="2022";
        String month = "9";
        String day = "28";

        // ds.write().mode(SaveMode.Overwrite).parquet(FILE_PATH + "/y=" + year + "/m=" + month + "/d=" + day);

        if (partition) {
            ds.write().mode(SaveMode.Overwrite).format(codec).partitionBy("y", "m", "d").save(parquetFilePath);
        } else {
            ds.write().mode(SaveMode.Overwrite).format(codec).save(parquetFilePath);
        }

       // ds.write().mode(SaveMode.Overwrite).format(codec).save(parquetFilePath);
    }

    /**
     * Reads another csv file and APPENDS to the original data
     */
    public void appendToHDFS(String fileName, String appendToParquetFile) {
        // read a NEW dataset
        final String dir = BASE_PATH + "/" + fileName;
        Dataset<Row> ds = sparkSession.read().option("header", true).format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("inferSchema", true).csv(dir);

        // append to the SAME PARQUET FILE- -this is now appending a new partition is done
        ds.write().mode(SaveMode.Append).format(codec).partitionBy("y", "m","d").save(appendToParquetFile);

    }


}
