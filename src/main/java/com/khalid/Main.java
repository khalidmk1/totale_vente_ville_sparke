package com.khalid;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        SparkConf conf = new SparkConf()
                .setAppName("TotalVentesParVilleEtAnnee")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("ventes.txt");

        JavaPairRDD<Tuple2<String, String>, Double> ventesParVilleEtAnnee = lines
                .mapToPair(line -> {
                    String[] parts = line.split("\\s+");
                    String date = parts[0];               // e.g., 2023-01-10
                    String ville = parts[1];
                    double prix = Double.parseDouble(parts[3]);

                    String annee = date.split("-")[0];     // extract year

                    Tuple2<String, String> key = new Tuple2<>(ville, annee); // (ville, annee)
                    return new Tuple2<>(key, prix);
                })
                .reduceByKey(Double::sum);

        ventesParVilleEtAnnee.foreach(tuple -> {
            String ville = tuple._1._1;
            String annee = tuple._1._2;
            Double total = tuple._2;
            System.out.println("📅 " + annee + " | 📍 " + ville + " => " + total + " €");
        });

        sc.close();
    }
}