package application;

import application.Cases.Cases;
import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("application.main").config("spark.master", "local").config("log4j.rootCategory", "ERROR").getOrCreate();
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
        cases = new Cases(datasetValorizacaoMensal);

        cases.runCase4();



        sparkSession.stop();
        sparkSession.close();
    }


}
