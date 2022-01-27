package org.benchmark;

import org.openjdk.jmh.annotations.*;

import java.sql.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 5)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MyBench {

    /**
     * Initialize method for each benchmark thread
     */
    @State(Scope.Thread)
    public static class MyState {

        private Connection connection;

        @Param({"1", "10", "100", "1000", "10000"})
        public int size;

        @Param({"mysql", "mariadb"})
        public String driver;

        @Param({"false", "true"})
        public String binary;

        @Setup(Level.Trial)
        public void createConnections() throws Exception {
            String connectionString = String.format(
                    "jdbc:%s://localhost/db?user=root&sslMode=DISABLED&useServerPrepStmts=%s",
                    driver, binary);
            connection = DriverManager.getConnection(connectionString);
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws SQLException {
            connection.close();
        }
    }

    /**
     * Method to benchmark
     * @param state benchmarl state
     * @return results
     * @throws SQLException if any error occurs
     */
    @Benchmark
    public int[] testSeq(MyState state) throws SQLException {
        int i = 0;
        int[] values = new int[state.size];

        try (PreparedStatement prep = state.connection.prepareStatement("select * from seq_1_to_" + state.size)) {
            ResultSet rs = prep.executeQuery();
            while (rs.next()) {
                values[i++] = rs.getInt(1);
            }
        }

        return values;
    }
}

