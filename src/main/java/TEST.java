import java.io.IOException;
import java.net.Socket;

public class TEST {
    public static void main(String[] args) {
        try (Socket socket = new Socket("tp-3a101-01.enst.fr", 8888)) {
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
