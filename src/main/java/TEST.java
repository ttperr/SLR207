import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class TEST {
    public static void main(String[] args) throws UnknownHostException {
        InetAddress localhost = InetAddress.getLocalHost();
            String hostName = localhost.getHostName();
            System.out.println("Host name: " + hostName);
    }
}
