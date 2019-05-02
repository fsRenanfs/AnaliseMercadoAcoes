package application.Cases;

import application.DatasetManager;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Cases {

    Dataset<Row> dataset;
    Dataset<Row> principaisAtivosAnos;
    DatasetManager datasetManager;

    public Cases(Dataset<Row> dataset, DatasetManager datasetManager) {
        this.dataset = dataset;
        this.datasetManager = datasetManager;
        this.principaisAtivosAnos = getVariacaoAnos();
    }

    //CASE 1 - 3 ativos mais valorizados nos anos
    public Dataset<Row> getCase1() {
        Dataset<Row> case1 = principaisAtivosAnos.orderBy(principaisAtivosAnos.col("variacao").desc()).limit(3);
        return joinVariacaoMesesVariacaoAnos(dataset, case1);
    }

    //CASE 2 - 3 ativos mais desvalorizados nos anos
    public Dataset<Row> getCase2() {
        Dataset<Row> case2 = principaisAtivosAnos.orderBy(principaisAtivosAnos.col("variacao").asc()).limit(3);
        return joinVariacaoMesesVariacaoAnos(dataset, case2);
    }

    //CASE 3 - Valorizacao Bancos: BBDC4 – Bradesco; ITUB4 – Itaú; BBAS3 - Banco do Brasil e SANB11 Santander
    public Dataset<Row> getCase3() {
        Dataset<Row> case3 = principaisAtivosAnos.filter("codigo_negociacao in ('BBAS3','BBDC4','ITUB4','SANB11')");
        return joinVariacaoMesesVariacaoAnos(dataset, case3);
    }

    //CASE 4 - Periodos em que houve maior valorizacao e desvalorizacao Ativos Santander
    public Dataset<Row> getCase4() {
        return dataset.select("ano", "mes", "codigo_negociacao", "descricao_negociacao", "preco_abertura", "preco_fechamento", "variacao", "percentual_variacao").
                filter("codigo_negociacao = 'SANB11'").
                orderBy("ano", "mes");
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

    private Dataset<Row> joinVariacaoMesesVariacaoAnos(Dataset<Row> mes, Dataset<Row> ano) {
        //Registra tabela temporaria anos
        Dataset<Row> valorizados = ano;
        valorizados.registerTempTable("DadosAno");

        //Registra tabela temporaria meses
        valorizados = mes;
        valorizados.registerTempTable("DadosMes");

        //Efetua select com join
        valorizados = valorizados.sqlContext().sql(
                "select " +
                        "   mes.ano, " +
                        "   mes.mes, " +
                        "   mes.codigo_negociacao, " +
                        "   mes.descricao_negociacao, " +
                        "   mes.prazo_dias_mercado, " +
                        "   mes.preco_abertura, " +
                        "   mes.preco_fechamento, " +
                        "   mes.variacao, " +
                        "   mes.percentual_variacao, " +
                        "   ano.variacao as variacao_total " +
                        "from DadosMes mes " +
                        "   inner join DadosAno ano on " +
                        "       mes.codigo_negociacao = ano.codigo_negociacao and " +
                        "       mes.descricao_negociacao = ano.descricao_negociacao and " +
                        "       (mes.prazo_dias_mercado = ano.prazo_dias_mercado or isnull(mes.prazo_dias_mercado)=isnull(ano.prazo_dias_mercado))").
                orderBy("variacao_total", "ano", "mes");

        return valorizados;
    }
}
