package application.conversor.arquivoTXT;

public class Campo {
    private String name;
    private int posicaoInicial;
    private int posicaoFinal;


    public Campo(String name, int posicaoInicial, int posicaoFinal) {
        this.name = name;
        this.posicaoInicial = posicaoInicial - 1;
        this.posicaoFinal = posicaoFinal;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPosicaoInicial() {
        return posicaoInicial;
    }

    public void setPosicaoInicial(int posicaoInicial) {
        this.posicaoInicial = posicaoInicial;
    }

    public int getPosicaoFinal() {
        return posicaoFinal;
    }

    public void setPosicaoFinal(int posicaoFinal) {
        this.posicaoFinal = posicaoFinal;
    }


}
