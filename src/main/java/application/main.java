package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("application.main").config("spark.master", "local").getOrCreate();
        DatasetManager datasetManager = new DatasetManager(sparkSession);
        Dataset<Row> dataset, datasetValorizacao;

        //Converte os arquivos do diretorio para CSV
        ProcessarArquivos processarArquivos = new ProcessarArquivos(args[0]);

        //Retorna os DataSets dos arquivos processados
        dataset = datasetManager.getDatasetArquivoProcessado(processarArquivos);

        //Retorna um Dataset com as informacoes da valorizacao dos titulos nos anos
        datasetValorizacao = datasetManager.getDatasetValorizacao(dataset);

        datasetValorizacao.filter("codigo_negociacao = 'SANB11'").orderBy("ano", "mes").show(100);

        sparkSession.stop();
        sparkSession.close();
    }


}
