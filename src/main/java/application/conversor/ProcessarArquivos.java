package application.conversor;

import java.io.File;
import java.util.ArrayList;

public class ProcessarArquivos {
    private final String OUTPUT_DIRECTORY = "csvFiles" + File.separator;
    private ArrayList<String> csvsPaths = new ArrayList<>();

    public ProcessarArquivos(String directory) {
        //Verifica se o diretorio de saida existe
        checkOutputDirectory();
        //Converte os arquivos do diretorio para CSV
        converterArquivos(directory);
    }

    private void checkOutputDirectory() {
        File file = new File(OUTPUT_DIRECTORY);
        if (!file.exists()) file.mkdir();
    }

    public void converterArquivos(String directory) {
        File[] files = new File(directory).listFiles();
        CSVConverter csvConverter = new CSVConverter();

        for (File file : files) {
            String csvName = file.getName().toLowerCase().replace(".txt", ".csv");
            String csvPath = OUTPUT_DIRECTORY + csvName;
            csvConverter.converter(file.getPath(), csvPath);
            csvsPaths.add(csvPath);
        }
    }

    public ArrayList<String> getCSVsPaths() {
        return csvsPaths;
    }
}
