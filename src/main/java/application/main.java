package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("application.main").config("spark.master", "local").getOrCreate();
        DatasetManager dataSetManager = new DatasetManager(sparkSession);
        ArrayList<Dataset<Row>> datasets;

        //Converte os arquivos do diretorio para CSV
        ProcessarArquivos processarArquivos = new ProcessarArquivos(args[0]);

        //Retorna os DataSets dos arquivos processados
        datasets = dataSetManager.getDatasetArquivoProcessado(processarArquivos);

        dataSetManager.getDatasetValorizacaoAtivos(datasets);


        sparkSession.stop();
        sparkSession.close();
    }


}
