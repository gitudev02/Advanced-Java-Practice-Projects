import java.io.*;
import java.util.*;

/**
 * MiniDB - Single file mini SQLite-like database in pure Java
 */
public class MiniDB {

    /* ===================== ROW ===================== */
    static class Row implements Serializable {
        Map<String, String> data = new LinkedHashMap<>();

        void put(String col, String val) {
            data.put(col, val);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /* ===================== TABLE ===================== */
    static class Table implements Serializable {
        String name;
        List<String> columns;
        List<Row> rows = new ArrayList<>();

        Table(String name, List<String> columns) {
            this.name = name;
            this.columns = columns;
        }

        void insert(List<String> values) {
            if (values.size() != columns.size())
                throw new RuntimeException("Column count mismatch");

            Row row = new Row();
            for (int i = 0; i < columns.size(); i++) {
                row.put(columns.get(i), values.get(i));
            }
            rows.add(row);
        }
    }

    /* ===================== DATABASE ===================== */
    static class Database implements Serializable {
        Map<String, Table> tables = new HashMap<>();

        void createTable(String name, List<String> cols) {
            if (tables.containsKey(name))
                throw new RuntimeException("Table already exists");
            tables.put(name, new Table(name, cols));
        }

        Table getTable(String name) {
            Table t = tables.get(name);
            if (t == null) throw new RuntimeException("Table not found");
            return t;
        }
    }

    /* ===================== STORAGE ===================== */
    static class Storage {
        static final String FILE = "minidb.db";

        static void save(Database db) {
            try (ObjectOutputStream oos =
                         new ObjectOutputStream(new FileOutputStream(FILE))) {
                oos.writeObject(db);
            } catch (Exception e) {
                System.out.println("Save error");
            }
        }

        static Database load() {
            try (ObjectInputStream ois =
                         new ObjectInputStream(new FileInputStream(FILE))) {
                return (Database) ois.readObject();
            } catch (Exception e) {
                return new Database();
            }
        }
    }

    /* ===================== SQL ENGINE ===================== */
    static class Engine {
        Database db;

        Engine(Database db) {
            this.db = db;
        }

        void execute(String sql) {
            sql = sql.trim();

            if (sql.equalsIgnoreCase("EXIT")) {
                Storage.save(db);
                System.out.println("Database saved. Bye!");
                System.exit(0);
            }

            if (sql.toUpperCase().startsWith("CREATE TABLE")) {
                createTable(sql);
            } else if (sql.toUpperCase().startsWith("INSERT INTO")) {
                insert(sql);
            } else if (sql.toUpperCase().startsWith("SELECT")) {
                select(sql);
            } else {
                System.out.println("Invalid SQL");
            }
        }

        void createTable(String sql) {
            // CREATE TABLE users (id,name,age)
            String name = sql.split("\\s+")[2];
            String cols = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
            List<String> columns = Arrays.asList(cols.split(","));
            db.createTable(name, columns);
            System.out.println("Table created");
        }

        void insert(String sql) {
            // INSERT INTO users VALUES (1,John,25)
            String name = sql.split("\\s+")[2];
            String vals = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
            List<String> values = Arrays.asList(vals.split(","));
            db.getTable(name).insert(values);
            System.out.println("Row inserted");
        }

        void select(String sql) {
            // SELECT * FROM users
            String name = sql.split("\\s+")[3];
            Table t = db.getTable(name);

            if (t.rows.isEmpty()) {
                System.out.println("Empty table");
                return;
            }

            for (Row r : t.rows) {
                System.out.println(r);
            }
        }
    }

    /* ===================== MAIN ===================== */
    public static void main(String[] args) {
        Database db = Storage.load();
        Engine engine = new Engine(db);
        Scanner sc = new Scanner(System.in);

        System.out.println("MiniDB (Pure Java) | type EXIT to quit");

        while (true) {
            System.out.print("db> ");
            String sql = sc.nextLine();
            try {
                engine.execute(sql);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
