package application;

import application.Cases.Cases;
import application.conversor.ProcessarArquivos;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import javax.xml.crypto.Data;
import java.io.File;


public class main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("application.main").config("spark.master", "local").getOrCreate();

        //Set log level
        //sparkSession.sparkContext().setLogLevel("ERROR");

        DatasetManager datasetManager = new DatasetManager(sparkSession);
        Dataset<Row> dataset, datasetValorizacaoMensal;
        Cases cases;

        //Converte os arquivos do diretorio para CSV
        ProcessarArquivos processarArquivos = new ProcessarArquivos(args[0]);

        //Retorna os DataSets dos arquivos processados
        dataset = datasetManager.getDatasetArquivoProcessado(processarArquivos);

        //Retorna um Dataset com as informacoes da valorizacao dos titulos nos anos
        datasetValorizacaoMensal = datasetManager.getDatasetValorizacao(dataset);

        //Instancia classe para resolução dos cases
        cases = new Cases(datasetValorizacaoMensal, datasetManager);


        //Case 1
        generateCSVFile(cases.getCase1(), "Case1");

        //Case 2
        generateCSVFile(cases.getCase2(), "Case2");

        //Case 3
        generateCSVFile(cases.getCase3(), "Case3");

        //Case 4
        generateCSVFile(cases.getCase4(), "Case4");

        sparkSession.stop();
        sparkSession.close();
    }

    public static void generateCSVFile(Dataset<Row> dataset, String fileName) {
        dataset.coalesce(1).write().option("header", "true").csv("outputCases" + File.separator + fileName);
    }

}
