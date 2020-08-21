package murilo.spark.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;

public class SparkHello {
    public static void main(String[] args) {

        SparkSession spark =  SparkSession.builder()
                .master("local")
                .appName("Word Count")
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile( "/home/murilohg/Documentos/sparkFile/" );
        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        logData.show();


        logData.write().format("parquet").save("/home/murilohg/Documentos/sparkFile/logdata.txt");
        Dataset<String> teste = spark.read().textFile( "/home/murilohg/Documentos/sparkFile/logdata.txt" );
        teste.show();



//
//        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
//        Encoder<String> stringEncoder = Encoders.STRING();
//        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
//                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
//                stringEncoder);
//        teenagerNamesByIndexDF.show();

    }
}
