package Step6;

public class SLAVE {
    public static void main(String[] args) {
        try {
            System.out.println("Je suis un esclave !");

            // Attente de 10 secondes
            Thread.sleep(10000);

            // Affichage du résultat du calcul 3+5
            int resultat = 3 + 5;
            System.out.println("Le résultat du calcul 3+5 est : " + resultat);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
