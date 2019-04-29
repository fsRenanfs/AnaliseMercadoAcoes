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

    //Retorna um dataset contendo a informacao da valorizacao dos ativos de todos os datasets
    public Dataset<Row> getDatasetValorizacaoAtivos(ArrayList<Dataset<Row>> datasets) {
        for (int i = 0; i < datasets.size(); i++) {
            //Registra a variacao por dia
            datasets.set(i, getDatasetInfo(datasets.get(i), "Dados",
                    "select *, (preco_fechamento-preco_abertura) as variacao from Dados"));

            //Registra a variacao por mes
            datasets.set(i, getDatasetInfo(datasets.get(i), "DadosMensais",
                    "select distinct ano as ano, mes as mes, codigo_negociacao as codigo_negociacao, descricao_negociacao as descricao_negociacao, prazo_dias_mercado as prazo_dias_mercado, sum(variacao) as variacao from DadosMensais" +
                            "        Group by ano, mes, codigo_negociacao, descricao_negociacao, prazo_dias_mercado"));
        }

        return unirDatasets(datasets);
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
