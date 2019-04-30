package application;

import application.conversor.ProcessarArquivos;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;


public class DatasetManager {
    private SparkSession sparkSession;
    private Dataset<Row> datasetVariacaoDia;

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
        //Registra a variacao por dia
        datasetVariacaoDia = getDatasetVariacaoDia(dataset);

        //Registra a variacao por mes
        return  getDatasetVariacaoMes(datasetVariacaoDia);
    }

    private Dataset<Row> getDatasetVariacaoMes(Dataset<Row> datasetVariacaoDia) {
        return getDatasetInfo(datasetVariacaoDia, "DadosMensais",
                "select " +
                        "   distinct ano as ano, " +
                        "   mes as mes, " +
                        "   codigo_negociacao as codigo_negociacao, " +
                        "   descricao_negociacao as descricao_negociacao, " +
                        "   prazo_dias_mercado as prazo_dias_mercado, " +
                        "   sum(variacao)/100 as variacao, " +
                        "   sum(percentual_variacao) as percentual_variacao, " +
                        "   substring(max(data_preco_fechamento),9,13)/100 as preco_fechamento " +
                        "from DadosMensais " +
                        "Group by ano, mes, codigo_negociacao, descricao_negociacao, prazo_dias_mercado");
    }

    private Dataset<Row> getDatasetVariacaoDia(Dataset<Row> dataset) {
        return getDatasetInfo(dataset, "Dados",
                "select *, " +
                        "   (preco_fechamento-preco_abertura) as variacao," +
                        "   ((preco_fechamento-preco_abertura)/preco_abertura*100) as percentual_variacao," +
                        "   concat(ano,mes,dia,preco_fechamento) as data_preco_fechamento " +
                        "from Dados");

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
