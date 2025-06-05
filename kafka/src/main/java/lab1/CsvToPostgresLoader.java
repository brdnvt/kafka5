package lab1;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.sql.*;

public class CsvToPostgresLoader {

    private static final String DB_URL = "jdbc:postgresql://localhost:5430/postgres_db";
    private static final String USER = "postgres_user";
    private static final String PASSWORD = "postgres_password";

    private static final String CSV_FILE_PATH = "starbucks.csv";

    public static void main(String[] args) {
        try {
            System.out.println("Спроба підключення до бази даних...");
            System.out.println("URL: " + DB_URL);
            System.out.println("Користувач: " + USER);
            
            try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
                System.out.println("Підключення до бази даних успішне!");
                
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet tables = metaData.getTables(null, null, "coffee_products", new String[]{"TABLE"});
                if (tables.next()) {
                    System.out.println("Таблиця coffee_products вже існує");
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("DROP TABLE IF EXISTS coffee_products");
                        System.out.println("Існуючу таблицю видалено");
                    }
                }
                
                System.out.println("Створюємо нову таблицю...");
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS coffee_products (" +
                        "id SERIAL PRIMARY KEY," +
                        "product_name VARCHAR(255)," +
                        "size VARCHAR(50)," +
                        "milk INTEGER," +
                        "whip INTEGER," +
                        "serv_size_m_l INTEGER," +
                        "calories INTEGER," +
                        "total_fat_g DOUBLE PRECISION," +
                        "saturated_fat_g DOUBLE PRECISION," +
                        "trans_fat_g DOUBLE PRECISION," +
                        "cholesterol_mg INTEGER," +
                        "sodium_mg INTEGER," +
                        "total_carbs_g DOUBLE PRECISION," +
                        "fiber_g DOUBLE PRECISION," +
                        "sugar_g DOUBLE PRECISION," +
                        "caffeine_mg INTEGER" +
                        ")");
                    System.out.println("Таблицю створено успішно");
                }

                if (!new java.io.File(CSV_FILE_PATH).exists()) {
                    throw new RuntimeException("Файл " + CSV_FILE_PATH + " не знайдено!");
                }

                try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
                    String[] nextLine;
                    boolean isFirstLine = true;

                    String sql = "INSERT INTO coffee_products (product_name, size, milk, whip, serv_size_m_l, " +
                            "calories, total_fat_g, saturated_fat_g, trans_fat_g, cholesterol_mg, sodium_mg, " +
                            "total_carbs_g, fiber_g, sugar_g, caffeine_mg) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    PreparedStatement stmt = conn.prepareStatement(sql);

                    while ((nextLine = reader.readNext()) != null) {
                        if (isFirstLine) {
                            isFirstLine = false;
                            continue;
                        }

                        stmt.setString(1, nextLine[0]);  
                        stmt.setString(2, nextLine[1]);  
                        stmt.setInt(3, Integer.parseInt(nextLine[2]));  
                        stmt.setInt(4, Integer.parseInt(nextLine[3]));  
                        stmt.setInt(5, Integer.parseInt(nextLine[4]));  
                        stmt.setInt(6, Integer.parseInt(nextLine[5]));  
                        stmt.setDouble(7, Double.parseDouble(nextLine[6]));  
                        stmt.setDouble(8, Double.parseDouble(nextLine[7]));  
                        stmt.setDouble(9, Double.parseDouble(nextLine[8]));  
                        stmt.setInt(10, Integer.parseInt(nextLine[9]));  
                        stmt.setInt(11, Integer.parseInt(nextLine[10]));  
                        stmt.setDouble(12, Double.parseDouble(nextLine[11]));  
                        stmt.setDouble(13, Double.parseDouble(nextLine[12]));  
                        stmt.setDouble(14, Double.parseDouble(nextLine[13]));  
                        stmt.setInt(15, Integer.parseInt(nextLine[14]));  

                        stmt.executeUpdate();
                    }

                    System.out.println("CSV дані успішно імпортовано до PostgreSQL.");
                }
            }
        } catch (Exception e) {
            System.err.println("Помилка при роботі з базою даних:");
            e.printStackTrace();
        }
    }
}