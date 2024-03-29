package application.conversor;

import application.conversor.arquivoTXT.Campo;
import application.conversor.arquivoTXT.CotacoesHistoricas;

public class CSVConverter {
    //LAYOUT to convert to CSV
    private final Campo[] CAMPOS = {
            // new Campo("data", 3, 10),
            new Campo("ano", 3, 6),
            new Campo("mes", 7, 8),
            new Campo("dia", 9, 10),
            new Campo("codigo_negociacao", 13, 24),
            new Campo("descricao_negociacao", 28, 39),
            new Campo("prazo_dias_mercado",50,52),
            new Campo("preco_abertura", 57, 69),
            new Campo("preco_fechamento", 109, 121)
    };

    //Constructor (converte o arquivo de texto da bolsa para um arquivo CSV )
    public void converter(String filePath, String csvPath) {
        new CotacoesHistoricas(filePath, CAMPOS).generateCSV(csvPath);
    }

}
