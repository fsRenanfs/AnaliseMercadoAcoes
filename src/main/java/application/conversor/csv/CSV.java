package application.conversor.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class CSV {
    private final char SPLIT = ',';
    private String[] headers;
    private ArrayList<String[]> lines;

    public CSV(String[] headers, ArrayList<String[]> lines) {
        this.headers = headers;
        this.lines = lines;
    }

    public CSV(String path) {
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(SPLIT)
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());

            headers = getHeader(path);
            lines = getLinesArray(csvParser);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCSV(String path) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path))) {

            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.newFormat(SPLIT)
                    .withHeader(headers));

            csvPrinter.printRecord("\n");
            for (String[] values : lines) {
                csvPrinter.printRecord(values);
                csvPrinter.printRecord("\n");
            }
            csvPrinter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getHeader(String path) {
        try {
            return Files.readAllLines(Paths.get(path)).get(0).split(SPLIT + "");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private ArrayList<String[]> getLinesArray(CSVParser csvParser) {
        ArrayList<String[]> lines = new ArrayList<>();
        for (CSVRecord csvRecord : csvParser) {
            String line[] = new String[headers.length];
            for (int i = 0; i < line.length; i++) {
                line[i] = csvRecord.get(i);
            }
            lines.add(line);
        }
        return lines;
    }

    public String[] getHeaders() {
        return headers;
    }

    public int getHeaderIndex(String header) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(header))
                return i;
        }
        System.err.println("O cabecalho '" + header + "' nao foi localizado no arquivo conversor.CSV!");
        return -1;
    }

    public void setLines(ArrayList<String[]> lines) {
        this.lines = lines;
    }

    public ArrayList<String[]> getLines() {
        return lines;
    }


}
