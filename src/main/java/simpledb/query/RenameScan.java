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
        String oldName = getcurrentFieldName(fieldName);
        return s.getInt(oldName);
    }

    public String getString(String fieldName) {
        String oldName = getcurrentFieldName(fieldName);
        return s.getString(oldName);
    }

    public Constant getVal(String fieldName) {
        String oldName = getcurrentFieldName(fieldName);
        return s.getVal(oldName);
    }

    public boolean hasField(String name) {
        return newField.contains(name);
    }

    public void close() {
        s.close();
    }

    private String getcurrentFieldName(String newField) {
        int index = newField.indexOf(newField);
        if (index != -1) {
            return currentField;
        } else {
            throw new RuntimeException("Field " + newField + " not found.");
        }
    }
}
