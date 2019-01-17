package org.apache.tomcat.jdbc.pool;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DatabaseSettings {

    private DatabaseSettings() {
    }

    static Properties readDbProperties() {
        Properties properties = new Properties();

        try (InputStream resourceAsStream = DatabaseSettings.class.getResourceAsStream("/META-INF/datasource.properties")) {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return properties;

    }


}


