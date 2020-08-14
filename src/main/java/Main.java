import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {


    public static void main(String[] args) throws IOException {

        BufferedReader bufferRead = new BufferedReader(new FileReader(args[0]));
        String line = bufferRead.readLine();
        while (line != null) {
            String[] data = line.split("\t");
//            String[] syntacticNgram = data[1].split(" ");
//            for (String s : syntacticNgram) {
                if(data[0].charAt(0)>='a') {
                    System.out.println(line);
                }
//            }
            line = bufferRead.readLine();
        }
    }
}
