package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.util.ArrayList;

public class main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("application.main").config("spark.master", "local").getOrCreate();
        DatasetManager datasetManager = new DatasetManager(sparkSession);
        ArrayList<Dataset<Row>> datasets;
        Dataset<Row> datasetValorizacaoAtivos;

        //Converte os arquivos do diretorio para CSV
        ProcessarArquivos processarArquivos = new ProcessarArquivos(args[0]);

        //Retorna os DataSets dos arquivos processados
        datasets = datasetManager.getDatasetArquivoProcessado(processarArquivos);

        //Retorna um Dataset com as informacoes da valorizacao dos titulos nos anos
        datasetValorizacaoAtivos = datasetManager.getDatasetValorizacaoAtivos(datasets);

        datasetValorizacaoAtivos.filter("codigo_negociacao = 'ITUB4'").orderBy("ano","mes").show(100);

        sparkSession.stop();
        sparkSession.close();
    }


}
