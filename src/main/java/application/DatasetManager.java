package application;

import application.conversor.ProcessarArquivos;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;
import java.util.ArrayList;


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
        //Registra a variacao por mes
        return getDatasetVariacaoMes(getDatasetPrecosDia(dataset));
    }

    private Dataset<Row> getDatasetVariacaoMes(Dataset<Row> datasetVariacaoDia) {
        Dataset<Row> mes = getDatasetInfo(datasetVariacaoDia, "DadosMensais",
                "select " +
                        "   max(ano) as ano, " +
                        "   max(mes) as mes, " +
                        "   max(codigo_negociacao) as codigo_negociacao, " +
                        "   max(descricao_negociacao) as descricao_negociacao, " +
                        "   max(prazo_dias_mercado) as prazo_dias_mercado, " +
                        "   substring(min(data_preco_abertura),9,13)/100 as preco_abertura, " +
                        "   substring(max(data_preco_fechamento),9,13)/100 as preco_fechamento, " +
                        "   min(data_preco_abertura) as data_preco_abertura, " +
                        "   max(data_preco_fechamento) as data_preco_fechamento " +
                        "from DadosMensais " +
                        "Group by ano, mes, codigo_negociacao, descricao_negociacao, prazo_dias_mercado");
        return getVariacao(mes);

    }

    public Dataset<Row> getVariacao(Dataset<Row> dataset) {
        return getDatasetInfo(dataset, "Dados",
                "select *, " +
                        "   (preco_fechamento-preco_abertura) as variacao," +
                        "   ((preco_fechamento-preco_abertura)/preco_abertura*100) as percentual_variacao " +
                        "from Dados");

    }

    private Dataset<Row> getDatasetPrecosDia(Dataset<Row> dataset) {
        return getDatasetInfo(dataset, "Dados",
                "select *, " +
                        "   concat(ano,mes,dia,preco_abertura) as data_preco_abertura, " +
                        "   concat(ano,mes,dia,preco_fechamento) as data_preco_fechamento " +
                        "from Dados");
    }

    public Dataset<Row> getDatasetInfo(Dataset<Row> dataset, String tempTable, String querySQL) {
        dataset.registerTempTable(tempTable);
        return dataset.sqlContext().sql(querySQL);
    }

    public Dataset<Row> unirDatasets(ArrayList<Dataset<Row>> datasets) {
        Dataset<Row> dataset = datasets.get(0);
        for (int i = 1; i < datasets.size(); i++) {
            dataset = dataset.union(datasets.get(i));
        }
        return dataset;
    }
}
