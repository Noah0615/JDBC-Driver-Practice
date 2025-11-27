import java.sql.*;
import java.util.Scanner;

public class HW8_Transaction {
    // Database credentials - Update these if necessary
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/company";
    private static final String USER = "jeonjaewon";
    private static final String PASS = "wjs125689";

    public static void main(String[] args) {
        Connection conn = null;
        Scanner scanner = new Scanner(System.in);

        try {
            // 1. Establish Connection
            // Ensure the JDBC driver is loaded (optional for newer JDBC versions but good practice)
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                System.out.println("PostgreSQL JDBC Driver not found. Include it in your library path.");
                return;
            }

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            
            // 2. Set AutoCommit to false to manage transaction manually
            conn.setAutoCommit(false);

            // 3. Get User Input
            System.out.println("Enter Sender Account ID:");
            int senderId = scanner.nextInt();

            System.out.println("Enter Receiver Account ID:");
            int receiverId = scanner.nextInt();

            System.out.println("Enter Transfer Amount:");
            double amount = scanner.nextDouble();

            // 4. Transaction Logic
            
            // Check Sender Balance
            if (!checkBalance(conn, senderId, amount)) {
                System.out.println("Transaction fully rolled back: Insufficient funds in sender's account.");
                conn.rollback();
                return;
            }

            // Deduct from Sender
            updateBalance(conn, senderId, -amount);
            
            // Set Savepoint after deduction
            Savepoint afterDeduct = conn.setSavepoint("AfterDeduct");

            // Add to Receiver
            boolean receiverExists = checkAccountExists(conn, receiverId);
            if (!receiverExists) {
                System.out.println("Transaction rolled back to savepoint: No such account found for receiver.");
                conn.rollback(afterDeduct);
                // We commit here to save the deduction from sender as per the "Savepoint" logic description in the prompt?
                // Wait, the prompt says: "송금 계좌의 금액 차감은 유지되지만, 전체 트랜잭션을 다시 시작할 필요는 없음."
                // This implies the deduction should persist. So we commit the transaction which now only contains the deduction.
                conn.commit(); 
                printFinalBalances(conn, senderId, receiverId);
                return;
            }

            // If receiver exists, proceed to add amount
            updateBalance(conn, receiverId, amount);

            // If we reached here, everything is successful
            conn.commit();
            System.out.println("Transaction successful!");
            printFinalBalances(conn, senderId, receiverId);

        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                    System.out.println("Transaction rolled back due to error: " + e.getMessage());
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (conn != null) conn.close();
                if (scanner != null) scanner.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean checkBalance(Connection conn, int accountId, double amount) throws SQLException {
        String sql = "SELECT balance FROM hw8_accounts WHERE account_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, accountId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    double balance = rs.getDouble("balance");
                    return balance >= amount;
                }
            }
        }
        return false; // Account not found or error
    }

    private static boolean checkAccountExists(Connection conn, int accountId) throws SQLException {
        String sql = "SELECT 1 FROM hw8_accounts WHERE account_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, accountId);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void updateBalance(Connection conn, int accountId, double amount) throws SQLException {
        String sql = "UPDATE hw8_accounts SET balance = balance + ? WHERE account_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, amount);
            pstmt.setInt(2, accountId);
            pstmt.executeUpdate();
        }
    }

    private static void printFinalBalances(Connection conn, int senderId, int receiverId) throws SQLException {
        // We need to query again to get updated balances
        // Note: If we just committed, we can see the changes.
        String sql = "SELECT account_id, balance FROM hw8_accounts WHERE account_id IN (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, senderId);
            pstmt.setInt(2, receiverId);
            try (ResultSet rs = pstmt.executeQuery()) {
                System.out.println("Final Balances:");
                while (rs.next()) {
                    System.out.println("Account " + rs.getInt("account_id") + ": " + rs.getDouble("balance"));
                }
            }
        }
    }
}
