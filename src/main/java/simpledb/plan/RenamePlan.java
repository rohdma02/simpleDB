package simpledb.plan;

import simpledb.query.RenameScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class RenamePlan implements Plan {
    private Plan p;
    private String currentField;
    private String newField;
    private Schema schema;

    public RenamePlan(Plan p, String currentField, String newField) {
        this.p = p;
        this.currentField = currentField;
        this.newField = newField;
        this.schema = new Schema();
        schema.addAll(p.schema());
        schema.addField(newField, p.schema().type(currentField), p.schema().length(currentField));
        schema.fields().remove(currentField);
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
