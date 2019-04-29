package application.Cases;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.xml.crypto.Data;

public class Cases {

    Dataset<Row> dataset;

    public Cases(Dataset<Row> dataset) {
        this.dataset = dataset;
    }


    //CASE 4 - Periodos em que houve maior valorizacao e desvalorizacao Ativos Santander
    public void runCase4() {
        Dataset<Row> dsAux = dataset;
        //Valorizacao
        dataset.filter("codigo_negociacao = 'SANB11'").orderBy(dataset.col("variacao").desc()).show(1);
        //Desvalorizacao
        dataset.filter("codigo_negociacao = 'SANB11'").orderBy(dataset.col("variacao").asc()).show(1);
//
//        //Exibe tudo
//        dataset.filter("codigo_negociacao = 'SANB11'").orderBy("ano", "mes");
//
//        dataset.write().csv("teste");
    }

}
