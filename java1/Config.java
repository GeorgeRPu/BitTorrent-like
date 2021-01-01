import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {
    private final Properties prop = new Properties();

    public Config(String filename) {
        try {
            FileInputStream in = new FileInputStream(filename);
            prop.load(in);
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public int getInt(String key) {
        return Integer.parseInt(prop.getProperty(key));
    }

    public String getString(String key) {
        return prop.getProperty(key);
    }
}
