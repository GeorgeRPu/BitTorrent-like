import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {
    private final Properties properties;

    public Config(String filename) {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(filename));
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public String getString(String key) {
        return properties.getProperty(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }
}
