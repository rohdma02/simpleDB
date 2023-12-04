package simpledb.plan;

import simpledb.query.JoinScan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class JoinPlan implements Plan {
    private Plan leftPlan;
    private Plan rightPlan;
    private Schema schema;
    private Predicate joinPredicate;

    public JoinPlan(Plan leftPlan, Plan rightPlan, Predicate joinPredicate) {
        this.leftPlan = leftPlan;
        this.rightPlan = rightPlan;
        this.schema = new Schema();
        schema.addAll(leftPlan.schema());
        schema.addAll(rightPlan.schema());
        this.joinPredicate = joinPredicate;
    }

    public Scan open() {
        Scan leftScan = leftPlan.open();
        Scan rightScan = rightPlan.open();
        return new JoinScan(leftScan, rightScan, joinPredicate);
    }

    public int blocksAccessed() {
        return leftPlan.blocksAccessed() + rightPlan.blocksAccessed();
    }

    public int recordsOutput() {
        int leftRecords = leftPlan.recordsOutput();
        int rightRecords = rightPlan.recordsOutput();
        int reductionFactor = joinPredicate.reductionFactor(this);
        return (leftRecords * rightRecords) / reductionFactor;
    }

    public int distinctValues(String fldname) {
        int leftRecords = leftPlan.recordsOutput();
        int rightRecords = rightPlan.recordsOutput();
        System.out.println(leftRecords + "----" + rightRecords);
        if (leftRecords > rightRecords) {
            return rightRecords;
        } else {
            return leftRecords;
        }
    }

    public Schema schema() {
        return schema;
    }
}
