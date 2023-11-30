package simpledb.query;

public class ExtendScan implements Scan {
    private Scan s;
    private String currentFieldName;
    private Expression newField;

    public ExtendScan(Scan s, String currentFieldName, Expression newField) {
        this.s = s;
        this.currentFieldName = currentFieldName;
        this.newField = newField;
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fieldName) {
        if (fieldName.equals(currentFieldName)) {
            return newField.evaluate(this).asInt();
        } else {
            return s.getInt(fieldName);
        }
    }

    public String getString(String fieldName) {
        if (fieldName.equals(currentFieldName)) {
            return newField.evaluate(this).asString();
        } else {
            return s.getString(fieldName);
        }
    }

    public Constant getVal(String fieldName) {
        if (fieldName.equals(currentFieldName)) {
            return newField.evaluate(this);
        } else {
            return s.getVal(fieldName);
        }
    }

    public boolean hasField(String fieldName) {
        return s.hasField(fieldName) || fieldName.equals(currentFieldName);
    }

    public void close() {
        s.close();
    }
}
