package simpledb.query;

public class RenameScan implements Scan {
    private Scan s;
    private String currentField;
    private String newField;

    public RenameScan(Scan s, String currentField, String newField) {
        this.s = s;
        this.currentField = currentField;
        this.newField = newField;
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fieldName) {
        return s.getInt(getFieldName(fieldName));
    }

    public String getString(String fieldName) {
        return s.getString(getFieldName(fieldName));
    }

    public Constant getVal(String fieldName) {
        return s.getVal(getFieldName(fieldName));
    }

    public boolean hasField(String name) {
        return newField.equals(name) || s.hasField(name);
    }

    public void close() {
        s.close();
    }

    private String getFieldName(String fieldName) {
        if (fieldName.equals(newField)) {
            return currentField;
        } else {
            return fieldName;
        }
    }
}
