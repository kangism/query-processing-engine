/**
 * 
 */
package qp.operators;

/**
 * @author Yann-Loup
 */
public enum OperatorType {

    SCAN(0), SELECT(1), PROJECT(2), JOIN(3), SORT(4), DISTINCT(5);

    /**
     * To be compatible with the constant define in {@link OpType}.
     * <br />{@link OpType} is a C-like 'enum': so <i>ugly</i> for Java.
     * @deprecated never used
     */
    private final int opId;

    /**
     *  to be compatible with OpType
     * @param opId
     */
    private OperatorType(int opId) {
	this.opId = opId;
    }

    /**
     * @return
     * @deprecated never used
     */
    public int getOpId() {
	return this.opId;
    }
}
