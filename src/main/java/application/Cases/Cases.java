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
    public Dataset<Row> getCase4() {
        return dataset.filter("codigo_negociacao = 'SANB11'").orderBy("ano", "mes");
    }

}
