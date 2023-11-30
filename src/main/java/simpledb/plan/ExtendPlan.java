package simpledb.plan;

import simpledb.query.Expression;
import simpledb.query.ExtendScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class ExtendPlan implements Plan {
    private Plan p;
    private String fieldName;
    private Expression newField;

    public ExtendPlan(Plan p, String fieldName, Expression newField) {
        this.p = p;
        this.fieldName = fieldName;
        this.newField = newField;
    }

    public Scan open() {
        Scan s = p.open();
        return new ExtendScan(s, fieldName, newField);
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
        Schema schema = p.schema();
        schema.addField(fieldName, 0, 20);
        return schema;
    }
}
