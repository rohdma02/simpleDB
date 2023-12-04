package simpledb.plan;

import simpledb.query.Scan;
import simpledb.query.UnionScan;
import simpledb.record.Schema;

public class UnionPlan implements Plan {
    private Plan firstPlan;
    private Plan secondPlan;
    private Schema schema;

    public UnionPlan(Plan firstPlan, Plan secondPlan) {
        this.firstPlan = firstPlan;
        this.secondPlan = secondPlan;
        this.schema = new Schema();

        for (String fieldName : firstPlan.schema().fields()) {
            schema.addField(fieldName, firstPlan.schema().type(fieldName), firstPlan.schema().length(fieldName));
        }

        for (String fieldName : secondPlan.schema().fields()) {
            if (!schema.hasField(fieldName)) {
                schema.addField(fieldName, secondPlan.schema().type(fieldName), secondPlan.schema().length(fieldName));
            }
        }
    }

    public Scan open() {
        Scan s1 = firstPlan.open();
        Scan s2 = secondPlan.open();
        return new UnionScan(s1, s2);
    }

    public int blocksAccessed() {
        return firstPlan.blocksAccessed() + secondPlan.blocksAccessed();
    }

    public int recordsOutput() {
        return firstPlan.recordsOutput() + secondPlan.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return firstPlan.distinctValues(fldname) + secondPlan.distinctValues(fldname);
    }

    public Schema schema() {
        return schema;
    }
}
