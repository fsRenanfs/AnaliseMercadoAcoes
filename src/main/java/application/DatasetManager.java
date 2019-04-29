package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder$;

import javax.xml.crypto.Data;
import java.util.ArrayList;

import static com.sun.tools.doclint.Entity.lambda;

public class DatasetManager {
    private SparkSession sparkSession;

    public DatasetManager(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> getDatasetArquivoProcessado(ProcessarArquivos processarArquivos) {
        ArrayList<Dataset<Row>> datasets = new ArrayList<>();
        //Cria os DataSets dos arquivos
        for (String csvPath : processarArquivos.getCSVsPaths()) {
            Dataset<Row> csv = sparkSession.read().format("csv").option("header", "true").load(csvPath);
            datasets.add(csv);
        }
        return unirDatasets(datasets);
    }

    //Retorna um dataset contendo a informacao da valorizacao dos ativos de todos os datasets por mes
    public Dataset<Row> getDatasetValorizacao(Dataset<Row> dataset) {
        //  Dataset<Row> datasetPrecoAbertura = ;

        //Registra a variacao por dia
        dataset = getDatasetVariacaoDia(dataset);

        //Registra a variacao por mes
        dataset = getDatasetVariacaoMes(dataset);

        return dataset;
    }

    //Retorna um dataset com os precos de abertura de cada mes
    private Dataset<Row> getPrecoAberturaMeses(Dataset<Row> datasetVariacaoDia) {
        Dataset<Row> dataset = getDatasetInfo(datasetVariacaoDia, "DataInicioMes",
                "select  ano, mes, preco_abertura from DataInicioMes " +
                        "        Order by ano, mes Asc");
     //   dataset.map
        //  dataset.map();
        return dataset;
    }

    private Dataset<Row> getDatasetVariacaoMes(Dataset<Row> datasetVariacaoDia) {
        return getDatasetInfo(datasetVariacaoDia, "DadosMensais",
                "select distinct ano as ano, mes as mes, codigo_negociacao as codigo_negociacao, descricao_negociacao as descricao_negociacao, prazo_dias_mercado as prazo_dias_mercado, sum(variacao) as variacao from DadosMensais" +
                        "        Group by ano, mes, codigo_negociacao, descricao_negociacao, prazo_dias_mercado");
    }

    private Dataset<Row> getDatasetVariacaoDia(Dataset<Row> dataset) {
        return getDatasetInfo(dataset, "Dados", "select *, (preco_fechamento-preco_abertura) as variacao from Dados");

    }

    private Dataset<Row> getDatasetInfo(Dataset<Row> dataset, String tempTable, String querySQL) {
        dataset.registerTempTable(tempTable);
        return dataset.sqlContext().sql(querySQL);
    }

    private Dataset<Row> unirDatasets(ArrayList<Dataset<Row>> datasets) {
        Dataset<Row> dataset = datasets.get(0);
        for (int i = 1; i < datasets.size(); i++) {
            dataset = dataset.union(datasets.get(i));
        }
        return dataset;
    }
}
