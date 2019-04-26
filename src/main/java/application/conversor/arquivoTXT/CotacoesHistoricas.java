package application.conversor.arquivoTXT;

import application.conversor.csv.CSV;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class CotacoesHistoricas {
    private CSV csv;

    public CotacoesHistoricas(String path, Campo[] campos) {
        csv = new CSV(getHeaderNames(campos), getLineValues(path, campos));
    }

    //Generate csv
    public void generateCSV(String outputPath) {
        System.out.println("\nQUANTIDADE TOTAL DE REGISTROS: " + csv.getLines().size());
        csv.writeCSV(outputPath);
        System.out.println("\nARQUIVO CSV GERADO COM SUCESSO: " + outputPath);
    }

    //Return header names from campos
    private String[] getHeaderNames(Campo[] campos) {
        String[] headerNames = new String[campos.length];
        for (int i = 0; i < campos.length; i++) {
            headerNames[i] = campos[i].getName();
        }
        return headerNames;
    }

    //Return line values
    private ArrayList<String[]> getLineValues(String path, Campo[] campos) {
        int cont = 0;
        ArrayList<String[]> lines = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            while (br.ready()) {
                cont++;
                System.out.println("Reading.. line " + cont);
                String line = br.readLine();
                if (line.substring(0, 2).equals("01")) {
                    String values[] = new String[campos.length];
                    for (int i = 0; i < campos.length; i++) {
                        String value = line.substring(campos[i].getPosicaoInicial(), campos[i].getPosicaoFinal());
                        values[i] = value.trim();
                    }
                    lines.add(values);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return lines;
    }
}
