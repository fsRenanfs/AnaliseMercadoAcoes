package application;

import application.conversor.ProcessarArquivos;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class DatasetManager {
    private SparkSession sparkSession;

    public DatasetManager(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public ArrayList<Dataset<Row>> getDatasetArquivoProcessado(ProcessarArquivos processarArquivos) {
        ArrayList<Dataset<Row>> datasets = new ArrayList<>();
        //Cria os DataSets dos arquivos
        for (String csvPath : processarArquivos.getCSVsPaths()) {
            Dataset<Row> csv = sparkSession.read().format("csv").option("header", "true").load(csvPath);
            datasets.add(csv);
        }
        return datasets;
    }

    //Retornar um dataset contendo a informacao da valorizacao dos ativos de todos os datasets
    public Dataset<Row> getDatasetValorizacaoAtivos(ArrayList<Dataset<Row>> datasets) {
        Dataset<Row> dataset = null;
        for (int i = 0; i < datasets.size(); i++) {
            //Registra a variacao mensal
            datasets.set(i, getDatasetInfo(datasets.get(i), "Dados",
                    "select *, (preco_fechamento-preco_abertura) as variacao from Dados"));

            //Registra a variacao anual
            datasets.set(i, getDatasetInfo(datasets.get(i), "DadosMensais",
                    "select distinct ano as ano, mes as mes, codigo_negociacao as codigo_negociacao, descricao_negociacao as descricao_negociacao,sum(variacao) as variacao from DadosMensais" +
                            "        Group by ano, mes, codigo_negociacao, descricao_negociacao"));
            datasets.get(i).show(2);
            datasets.get(i).filter("codigo_negociacao = 'ITUB4'").show();
        }
        return dataset;
    }

    private Dataset<Row> getDatasetInfo(Dataset<Row> dataset, String tempTable, String querySQL) {
        dataset.registerTempTable(tempTable);
        return dataset.sqlContext().sql(querySQL);
    }
}
