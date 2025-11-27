/*
 * HW8_Transaction.java
 * 데이터베이스 트랜잭션을 사용하여 계좌 이체를 시뮬레이션하는 자바 프로그램입니다.
 * ACID 속성을 이해하고 Savepoint를 활용하는 방법을 보여줍니다.
 */

import java.sql.*; // JDBC(Java Database Connectivity) 관련 클래스들을 가져옵니다. 데이터베이스 연결, 쿼리 실행 등에 사용됩니다.
import java.util.Scanner; // 사용자로부터 입력을 받기 위한 Scanner 클래스를 가져옵니다.

public class HW8_Transaction { // HW8_Transaction 클래스: 데이터베이스 트랜잭션을 관리하는 주 클래스입니다.
    // 데이터베이스 연결 정보를 설정합니다. 필요에 따라 사용자 환경에 맞게 수정하세요.
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/company"; // PostgreSQL 데이터베이스 URL
    private static final String USER = "jeonjaewon"; // 데이터베이스 사용자 이름
    private static final String PASS = "wjs125689"; // 데이터베이스 비밀번호

    public static void main(String[] args) {
        Connection conn = null; // 데이터베이스 연결 객체
        Scanner scanner = new Scanner(System.in); // 사용자 입력을 위한 Scanner 객체

        try {
            // 1. 데이터베이스 연결 설정
            // JDBC 드라이버 로드: 최신 JDBC 버전에서는 필수가 아닐 수 있지만, 명시적으로 로드하는 것은 좋은 습관입니다.
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                System.out.println("PostgreSQL JDBC Driver를 찾을 수 없습니다. 라이브러리 경로에 추가해주세요.");
                return; // 드라이버가 없으면 프로그램 종료
            }

            conn = DriverManager.getConnection(DB_URL, USER, PASS); // 데이터베이스에 연결
            
            // 2. AutoCommit(자동 커밋) 비활성화: 트랜잭션을 수동으로 관리하기 위해 설정합니다.
            conn.setAutoCommit(false);

            // 3. 사용자 입력 받기
            System.out.println("Enter Sender Account ID:");
            int senderId = scanner.nextInt(); // 보내는 사람 계좌 ID

            System.out.println("Enter Receiver Account ID:");
            int receiverId = scanner.nextInt(); // 받는 사람 계좌 ID

            System.out.println("Enter Transfer Amount:");
            double amount = scanner.nextDouble(); // 송금액

            // 4. 트랜잭션 로직 시작
            
            // 보내는 계좌 잔액 확인
            if (!checkBalance(conn, senderId, amount)) { // 잔액이 부족하면
                System.out.println("Transaction fully rolled back: Insufficient funds in sender's account.");
                conn.rollback(); // 전체 트랜잭션 롤백
                return;
            }

            // 보내는 계좌에서 금액 차감
            updateBalance(conn, senderId, -amount); // 잔액에서 송금액만큼 감소

            // 차감 후 Savepoint 설정: 만약 이후 과정에서 문제가 발생해도 여기까지의 작업은 유지할 수 있도록 저장점을 만듭니다.
            Savepoint afterDeduct = conn.setSavepoint("AfterDeduct");

            // 받는 계좌 존재 여부 확인
            boolean receiverExists = checkAccountExists(conn, receiverId);
            if (!receiverExists) { // 받는 계좌가 존재하지 않으면
                System.out.println("Transaction rolled back to savepoint: No such account found for receiver.");
                conn.rollback(afterDeduct); // Savepoint까지 롤백 (보내는 계좌 차감은 유지)
                // 여기에 commit()을 추가하는 것은 Savepoint 롤백 후,
                // 보내는 계좌의 차감만 영구적으로 적용.
                // "송금 계좌의 금액 차감은 유지되지만, 전체 트랜잭션을 다시 시작할 필요는 없음." 이라는
                // 요구사항에 맞춰 차감된 상태를 유지하기 위해 커밋합니다.
                conn.commit(); 
                printFinalBalances(conn, senderId, receiverId); // 최종 잔액 출력
                return;
            }

            // 받는 계좌가 존재하면 금액 추가
            updateBalance(conn, receiverId, amount); // 받는 계좌 잔액에 송금액만큼 증가

            // 모든 과정이 성공적으로 완료되면 트랜잭션 커밋
            conn.commit();
            System.out.println("트랜잭션이 성공적으로 완료되었습니다!");
            printFinalBalances(conn, senderId, receiverId); // 최종 잔액 출력

        } catch (SQLException e) { // SQL 예외 발생 시
            e.printStackTrace(); // 예외 스택 트레이스 출력
            try {
                if (conn != null) {
                    conn.rollback(); // 예외 발생 시 트랜잭션 롤백
                    System.out.println("Transaction rolled back due to error: " + e.getMessage());
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally { // 트랜잭션 성공/실패 여부와 관계없이 항상 실행되는 블록
            try {
                if (conn != null) conn.close(); // 데이터베이스 연결 닫기
                if (scanner != null) scanner.close(); // Scanner 객체 닫기
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean checkBalance(Connection conn, int accountId, double amount) throws SQLException {
        // 계좌의 현재 잔액이 송금액보다 충분한지 확인하는 메서드
        String sql = "SELECT balance FROM hw8_accounts WHERE account_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, accountId); // 쿼리 파라미터에 계좌 ID 설정
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) { // 결과가 있다면 (해당 계좌가 존재하면)
                    double balance = rs.getDouble("balance"); // 잔액을 가져옴
                    return balance >= amount; // 잔액이 송금액보다 크거나 같으면 true 반환
                }
            }
        }
        return false; // 계좌를 찾을 수 없거나 오류 발생 시 false 반환
    }

    private static boolean checkAccountExists(Connection conn, int accountId) throws SQLException {
        // 특정 계좌 ID가 존재하는지 확인하는 메서드
        String sql = "SELECT 1 FROM hw8_accounts WHERE account_id = ?"; // 단순히 존재 여부만 확인하므로 1을 선택
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, accountId); // 쿼리 파라미터에 계좌 ID 설정
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next(); // 결과가 있으면 true (계좌 존재), 없으면 false (계좌 없음) 반환
            }
        }
    }

    private static void updateBalance(Connection conn, int accountId, double amount) throws SQLException {
        // 계좌 잔액을 업데이트하는 메서드 (증가 또는 감소)
        String sql = "UPDATE hw8_accounts SET balance = balance + ? WHERE account_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, amount); // 잔액에 더할 금액 (음수이면 감소)
            pstmt.setInt(2, accountId); // 업데이트할 계좌 ID
            pstmt.executeUpdate(); // 쿼리 실행
        }
    }

    private static void printFinalBalances(Connection conn, int senderId, int receiverId) throws SQLException {
        // 송금인과 수취인의 최종 잔액을 출력하는 메서드
        // 트랜잭션이 커밋된 후 업데이트된 잔액을 다시 조회하여 출력합니다.
        String sql = "SELECT account_id, balance FROM hw8_accounts WHERE account_id IN (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, senderId); // 송금인 계좌 ID
            pstmt.setInt(2, receiverId); // 수취인 계좌 ID
            try (ResultSet rs = pstmt.executeQuery()) {
                System.out.println("최종 잔액:");
                while (rs.next()) { // 결과 집합을 순회하며 잔액 출력
                    System.out.println("계좌 " + rs.getInt("account_id") + ": " + rs.getDouble("balance"));
                }
            }
        }
    }
}
