package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("application.main").config("spark.master", "local").getOrCreate();
        ArrayList<Dataset<Row>> datasets = new ArrayList<>();

        //Converte os arquivos do diretorio para CSV
        ProcessarArquivos processarArquivos = new ProcessarArquivos(args[0]);

        //Cria os DataSets dos arquivos
        for (String csvPath : processarArquivos.getCSVsPaths()) {
            Dataset<Row> csv = spark.read().format("csv").option("header", "true").load(csvPath);
            datasets.add(csv);
        }

        for (Dataset<Row> dataset:datasets)
            dataset.orderBy(dataset.col("preco_fechamento").desc()).show(2);

        spark.stop();
        spark.close();
    }
}
