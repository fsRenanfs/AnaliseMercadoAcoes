package application.Cases;

import application.DatasetManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;

public class Cases {

    Dataset<Row> dataset;
    Dataset<Row> principaisAtivosAnos;
    DatasetManager datasetManager;

    public Cases(Dataset<Row> dataset, DatasetManager datasetManager) {
        this.dataset = dataset;
        this.datasetManager = datasetManager;
        this.principaisAtivosAnos = getVariacaoAnos();
    }

    //CASE 1
    public Dataset<Row> getCase1() {
        principaisAtivosAnos.show(1);
        return dataset.filter("codigo_negociacao").join(principaisAtivosAnos, principaisAtivosAnos.col("codigo_negociacao").equalTo(dataset.col("codigo_negociacao")));

    }




    public Dataset<Row> getVariacaoAnos() {
        Dataset<Row> anos = datasetManager.getDatasetInfo(dataset, "Temp",
                "select " +
                        "   max(codigo_negociacao) as codigo_negociacao, " +
                        "   max(descricao_negociacao) as descricao_negociacao, " +
                        "   max(prazo_dias_mercado) as prazo_dias_mercado, " +
                        "   substring(min(data_preco_abertura),9,13)/100 as preco_abertura," +
                        "   substring(max(data_preco_fechamento),9,13)/100 as preco_fechamento " +
                        "from Temp Group by codigo_negociacao, descricao_negociacao, prazo_dias_mercado");
        return datasetManager.getVariacao(anos);
    }

    //CASE 2

    //CASE 4 - Periodos em que houve maior valorizacao e desvalorizacao Ativos Santander
    public Dataset<Row> getCase4() {
        return dataset.filter("codigo_negociacao = 'SANB11'");
    }

}
