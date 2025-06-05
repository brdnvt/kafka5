package lab1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseCleaner {
    private static final String DB_URL = "jdbc:postgresql://localhost:5430/postgres_db";
    private static final String USER = "postgres_user";
    private static final String PASSWORD = "postgres_password";
    private static final String TABLE_NAME = "coffee_products";

    public static void main(String[] args) {
        clearDatabaseTable();
    }

    public static void clearDatabaseTable() {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            String sql = "TRUNCATE TABLE " + TABLE_NAME + " RESTART IDENTITY;";
            stmt.executeUpdate(sql);
            System.out.println("Table '" + TABLE_NAME + "' cleared successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Error clearing table: " + e.getMessage());
        }
    }
} 