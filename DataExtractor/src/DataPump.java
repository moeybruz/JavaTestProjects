package service;

import com.sun.org.apache.xml.internal.serialize.LineSeparator;
import com.univocity.parsers.csv.*;
import model.DatabaseResource;
import model.WriterRunnable;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DataPump {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final String JDBC_POSTGRES_PATTERN = "JDBC:POSTGRESQL";

    private DatabaseResource readerResource = new DatabaseResource();
    private DatabaseResource writerResource = new DatabaseResource();
    private PreparedStatement writePs;
    private CsvWriter writer;

    public CompletableFuture<Void> runTask() {
        PipedOutputStream readerPipedOutputStream;
        PipedInputStream readerPipedInputStream;

        PipedOutputStream writerPipedOutputStream;
        PipedInputStream writerPipedInputStream;

        Connection readConnection = null;
        Connection writeConnection = null;

        try {
            readerResource.setUsername(System.getProperty("readUsername"));
            readerResource.setPassword(System.getProperty("readPassword"));
            readerResource.setUrl(System.getProperty("readUrl"));
            readerResource.setQuery(System.getProperty("readQuery"));

            writerResource.setUsername(System.getProperty("writeUsername"));
            writerResource.setPassword(System.getProperty("writePassword"));
            writerResource.setUrl(System.getProperty("writeUrl"));
            writerResource.setQuery(System.getProperty("writeQuery"));


            Scanner scanner = new Scanner(System.in);

            // Reader
            if (readerResource.getUsername() == null) {
                logger.info("Enter reader username: ");
                readerResource.setUsername(scanner.nextLine());
            }

            if (readerResource.getPassword() == null) {
                logger.info("Enter reader password: ");
                readerResource.setPassword(scanner.nextLine());
            }

            if (readerResource.getUrl() == null) {
                logger.info("Enter reader URL: ");
                readerResource.setUrl(scanner.nextLine());
            }

            if (readerResource.getQuery() == null) {
                logger.info("Enter reader query: ");
                readerResource.setQuery(scanQuery(scanner));
            }


            // Writer
            if (writerResource.getUsername() == null) {
                logger.info("Enter writer username: ");
                writerResource.setUsername(scanner.nextLine());
            }

            if (writerResource.getPassword() == null) {
                logger.info("Enter writer password: ");
                writerResource.setPassword(scanner.nextLine());
            }

            if (writerResource.getUrl() == null) {
                logger.info("Enter writer URL: ");
                writerResource.setUrl(scanner.nextLine());
            }

            if (writerResource.getQuery() == null) {
                logger.info("Enter writer query: ");
                writerResource.setQuery(scanQuery(scanner));
            }

            // Hashing
            logger.info("Enter indices of columns to hash on (Starting at 0 for First Column. Example: 0,2,3): ");
            String hashIndicesStringWS = scanner.nextLine();
            // Remove whitespaces from user input
            String hashIndicesString = hashIndicesStringWS.replaceAll("\\s+", "");

            int[] hashIndices = {};
            if (!hashIndicesString.equals("")) {
                hashIndices = Stream.of(hashIndicesString.split(",")).mapToInt(Integer::parseInt).toArray();
            }
            Arrays.sort(hashIndices);

            // These settings cannot be exposed, as they are required to match database output. If changes are
            // wanted, they can be operated through the InputStreamIteratorReader
            CsvWriterSettings tempWriterSettings = new CsvWriterSettings();
            tempWriterSettings.getFormat().setLineSeparator("\n");
            tempWriterSettings.setEscapeUnquotedValues(true);
            tempWriterSettings.setHeaderWritingEnabled(true);
            tempWriterSettings.trimValues(false);
            tempWriterSettings.setEmptyValue("");

            CsvRoutines routines = new CsvRoutines(tempWriterSettings);
            routines.setKeepResourcesOpen(true);


            readerPipedOutputStream = new PipedOutputStream();// Postgres writes in to OutputStream
            readerPipedInputStream = new PipedInputStream(readerPipedOutputStream);// Iterator reads from InputStream
            writerPipedOutputStream = new PipedOutputStream();
            writerPipedInputStream = new PipedInputStream(writerPipedOutputStream);

            long startReadConnection = System.nanoTime();
            logger.info("Creating read connection...");
            readConnection = DriverManager.getConnection(
                    readerResource.getUrl(),
                    readerResource.getUsername(),
                    readerResource.getPassword()
            );
            logger.info("Created read connection [readConnectionTime={}]",
                    (System.nanoTime() - startReadConnection) / 1E9);

            // Detect if reader is JDBC, otherwise use Postgres
            CompletableFuture<Void> readFuture;
            Supplier<Void> supplier;
            boolean readPg = false;
            if (readerResource.getUrl().toUpperCase().contains(JDBC_POSTGRES_PATTERN)) {
                CopyManager copyManager = new CopyManager((BaseConnection) readConnection);

                supplier = () -> readAsyncPostgres(copyManager, readerResource.getQuery(), readerPipedOutputStream);
                readPg = true;

            } else {
                PreparedStatement ps = readConnection.prepareStatement(readerResource.getQuery());
                ps.setFetchSize(100000);

                logger.info("Executing query...");
                ResultSet rs = ps.executeQuery();
                logger.info("Executed query");

                supplier = () -> readAsyncJdbc(readerPipedOutputStream, routines, rs);
            }

            logger.info("Starting async...");
            readFuture = CompletableFuture.supplyAsync(supplier);
            logger.info("Moved on from async");

            CsvParserSettings settings = new CsvParserSettings();
            settings.setDelimiterDetectionEnabled(true, ',');
            settings.setQuoteDetectionEnabled(true);
            settings.setEscapeUnquotedValues(true);
            settings.setHeaderExtractionEnabled(false);
            settings.trimValues(false);
            settings.setEmptyValue("");

            CsvParser parser = new CsvParser(settings);

            logger.info("Creating reader iterator...");
            Iterator<String[]> iterator = parser.iterate(readerPipedInputStream).iterator();
            logger.info("Created reader iterator");


            // Get headers from Postgres
            List<String> headers = new ArrayList<>();
            if (iterator.hasNext()) {
                headers = Arrays.asList(iterator.next());
            }

            long startWriteConnection = System.nanoTime();
            logger.info("Creating write connection...");
            writeConnection = DriverManager.getConnection(writerResource.getUrl(),
                    writerResource.getUsername(),
                    writerResource.getPassword()
            );
            logger.info("Created write connection [writeConnectionTime={}]",
                    (System.nanoTime() - startWriteConnection) / 1E9);


            CompletableFuture<Void> writeFuture = null;
            WriterRunnable writeJdbc;
            WriterRunnable writePostgres;
            boolean writePg = false;
            // Detect if reader is JDBC, otherwise use Postgres
            if (writerResource.getUrl().toUpperCase().contains(JDBC_POSTGRES_PATTERN)) {
                writePg = true;

                CsvWriterSettings writerSettings = new CsvWriterSettings();
                writerSettings.trimValues(false);
                writerSettings.setEmptyValue("");
                writerSettings.setHeaderWritingEnabled(true);

                writer = new CsvWriter(writerPipedOutputStream, writerSettings);

                CopyManager copyManager = new CopyManager((BaseConnection) writeConnection);

                logger.info("Starting Postgres write async...");
                writeFuture = CompletableFuture.supplyAsync(() -> writeAsyncPostgres(copyManager,
                        writerResource.getQuery(), writerPipedInputStream));
                logger.info("Moved on from Postgres write async");


                writeJdbc = (doNothing, doNothing2) -> {
                };
                writePostgres = this::writePostgres;

            } else {
                writeConnection.setAutoCommit(false);

                writePs = writeConnection.prepareStatement(writerResource.getQuery());

                writeJdbc = this::writePreparedStatement;
                writePostgres = (doNothing, doNothing2) -> {
                };

            }

            long readStart = System.nanoTime();
            String[] recordArr;
            long records = 0;
            int foundHash;

            logger.info("Starting read...");
            while (iterator.hasNext()) {
                recordArr = iterator.next();

                foundHash = 0;
                for (int i = 1; i <= headers.size(); i++) {

                    // Assumes hashIndices are still sorted
                    if (hashIndices.length > 0 && foundHash < hashIndices.length) {
                        recordArr[hashIndices[i - 1]] = sha256Hash(recordArr[hashIndices[i - 1]]);
                        ++foundHash;
                    }

                    // This runnable only executes if writing to JDBC
//                    if (!writePg) {
//                        writePs.setObject(i, recordArr[i - 1]);
//                    }

                    writeJdbc.run(i, recordArr[i - 1]);
                }

                // This runnable only executes if writing to Postgres

//                if (writePg) {
//                    writer.writeRow(recordArr);
//                }
                writePostgres.run(recordArr, writerPipedOutputStream);


                if (records % 100000 == 0) {
                    logger.info("Progress event [recordCount={},thread={}]", records);

                    if (records == 0) {

                        logger.info("Checking first row...");

                        StringBuilder printHeader = new StringBuilder("| ");
                        StringBuilder printDashes = new StringBuilder("|");
                        StringBuilder printData = new StringBuilder(  "| ");

                        int totalLength = 6;
                        int longestHeader = 0;
                        //Get the length of the longest header
                        for (int i = 0; i < headers.size(); i++) {
                            if (headers.get(i).length() > longestHeader) {
                                longestHeader = headers.get(i).length();
                            }
                        }

                        /*
                        Over the length of the longest header,
                        print out the characters for the current header.
                        If the counter is greater than the current header length, print empty spaces.
                         */
                        StringBuilder result = new StringBuilder(System.lineSeparator());
                        for (int i = 0; i < headers.size(); ++i) {
                            if (i < 10) {
                                result.append(i + "  |  ");
                            } else {
                                result.append(i + " |  ");
                            }
                            for (int j = 0; j < longestHeader; j++) {
                                if (j < headers.get(i).length()) {
                                    result.append(headers.get(i).charAt(j));
                                } else {
                                    result.append(" ");
                                }
                            }
                            result.append(" | ").append(recordArr[i]).append(System.lineSeparator());
                        }
                        logger.info(result.toString());
                        logger.info("Is the above data masked correctly? (Y/N)");

                        String response = scanner.nextLine();
                        if (!(response.toUpperCase().equals("Y") || response.toUpperCase().equals("YES"))) {
                            System.exit(0);
                        }

                        logger.info("Continuing data pump...");
                        logger.info("Closing scanner...");
                        scanner.close();
                        logger.info("Closed scanner");
                    }
                }
                ++records;

                if (!writePg) {
                    writePs.addBatch();

                    if (records % 65536 == 0) {
                        writePs.executeBatch();
                    }
                }
            }

            if (!writePg) {
                writePs.executeBatch();
                writeConnection.commit();

            } else {
                writer.close();
            }

            double readTime = ((double) (System.nanoTime() - readStart)) / 1E9;
            logger.info("Finished read [readTime={}s,records={},tps={}TPS]", readTime, records, records / readTime);

            readFuture.get();
            if (writeFuture != null) {
                writeFuture.get();
            }


        } catch (SQLException | IOException e) {
            logger.error("SQLException - ", e);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Async error - ", e);
        } finally {
            try {
                readConnection.close();
            } catch (Exception ignore) {
            }
            try {
                writeConnection.close();
            } catch (Exception ignore) {

            }
        }

        return null;
    }

    private String scanQuery(Scanner scanner) {
        StringBuilder lines = new StringBuilder();
        boolean cont = true;
        while (cont) {
            String line = scanner.nextLine();

            if (line.trim().isEmpty()) {
                cont = false;

            } else {
                lines.append(line).append(System.lineSeparator());
            }
        }

        return lines.toString();
    }

    private Void readAsyncJdbc(PipedOutputStream source, CsvRoutines routines, ResultSet rs) {
        logger.info("Read Started in Thread {} ", Thread.currentThread());
        long start = System.currentTimeMillis();
        try {
            logger.debug("Starting CsvRoutines write");
            routines.write(rs, source);
            logger.debug("Finished CsvRoutines write");

            return null;

        } finally {
            try {
                source.flush();
            } catch (IOException e) {
                logger.error("", e);
            }
            try {
                source.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            logger.info("Read Completed in {}", (System.currentTimeMillis() - start));
        }
    }

    private Void readAsyncPostgres(CopyManager copyManager, String query, PipedOutputStream source) {
        long start = System.nanoTime();
        long rows;
        try {
            logger.info("Starting async PostgreSQL read [query={}]", query);
            rows = copyManager.copyOut(query, source);
            logger.info("Finished async PostgreSQL read");
            return null;

        } catch (SQLException | IOException e) {
            logger.error("", e);
        } finally {
            try {
                source.flush();
            } catch (IOException e) {
                logger.error("", e);
            }
            try {
                source.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            logger.info("Closing async PostgreSQL read [asyncTime={}]", (System.nanoTime() - start) / 1E9);
        }

        return null;
    }

    private Void writeAsyncPostgres(CopyManager copyManager, String query, PipedInputStream sink) {
        long start = System.nanoTime();
        long rows;
        try {
            logger.info("Starting async PostgreSQL write [query={}]", query);
            rows = copyManager.copyIn(query, sink);
            logger.info("Finished async PostgreSQL write");

            return null;
        } catch (SQLException | IOException e) {
            logger.error("", e);

        } finally {
            try {
                sink.close();
            } catch (IOException e) {
                logger.error("", e);
            }
            logger.info("Closing async PostgreSQL write [asyncTime={}]",
                    (System.nanoTime() - start) / 1E9);
        }
        return null;
    }

    private String sha256Hash(String hash) {
        String hashedKey = "";
        if (hash != null) {
            if (!hash.trim().equals("")) {
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("SHA-256");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                md.update(hash.getBytes());
                byte[] digest = md.digest();
                hashedKey = DatatypeConverter.printHexBinary(digest);
            }
        }
        return hashedKey;
    }

    private void writePreparedStatement(Object i, Object val) {
        try {
            writePs.setObject((int) i, val);
        } catch (SQLException e) {
            logger.error("", e);
        }
    }

    private void writePostgres(Object valArr, Object ignore) {
        writer.writeRow((String[]) valArr);
    }

    private String shard(String key, int shards) {
        return String.valueOf(Math.floorMod(key.hashCode(), shards));
    }

}
