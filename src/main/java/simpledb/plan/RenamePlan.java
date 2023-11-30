package simpledb.plan;

import simpledb.record.Schema;
import simpledb.query.*;

public class RenamePlan implements Plan {
    private Plan p;
    private String currentField;
    private String newField;
    private Schema schema = new Schema();

    public RenamePlan(Plan p, String currentField, String newField) {
        this.p = p;
        this.currentField = currentField;
        this.newField = newField;
    }

    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, currentField, newField);
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String name) {
        return p.distinctValues(name);
    }

    public Schema schema() {
        return schema;
    }
}
