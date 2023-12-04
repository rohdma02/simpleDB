// UnionPlan.java
package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.UnionScan;
import simpledb.record.Schema;

/**
 * The Plan class corresponding to the <i>union</i> relational algebra operator.
 * This plan returns all records that are present in either of the underlying
 * plans.
 * Duplicate records are not eliminated.
 * 
 * @author [Your Name]
 */
public class UnionPlan implements Plan {
    private Plan firstPlan;
    private Plan secondPlan;
    private Schema schema;

    /**
     * Creates a new union node in the query tree, having the two specified
     * subqueries.
     * 
     * @param firstPlan   the first subquery
     * @param secondPlan   the second subquery
     * @param pred the predicate that defines the union condition
     */
    public UnionPlan(Plan firstPlan, Plan secondPlan, Predicate pred) {
        this.firstPlan = firstPlan;
        this.secondPlan = secondPlan;
        this.schema = new Schema();
        schema.addAll(firstPlan.schema());
        schema.addAll(secondPlan.schema());
        // Note: You may need to handle the predicate for more complex cases.
    }

    /**
     * Creates a union scan for this query.
     * 
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s1 = firstPlan.open();
        Scan s2 = secondPlan.open();
        return new UnionScan(s1, s2);
    }

    /**
     * Estimates the number of block accesses in the union.
     * The formula is the sum of block accesses of the underlying plans.
     * 
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return firstPlan.blocksAccessed() + secondPlan.blocksAccessed();
    }

    /**
     * Estimates the number of output records in the union.
     * The formula is the sum of output records of the underlying plans.
     * 
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return firstPlan.recordsOutput() + secondPlan.recordsOutput();
    }

    /**
     * Estimates the distinct number of field values in the union.
     * Since the union does not eliminate duplicates, the estimate is the sum of
     * distinct values
     * from each underlying plan.
     * 
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return firstPlan.distinctValues(fldname) + secondPlan.distinctValues(fldname);
    }

    /**
     * Returns the schema of the union, which is the union of the schemas of the
     * underlying plans.
     * 
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }
}
