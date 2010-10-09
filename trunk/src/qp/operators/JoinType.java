/**
 * Enumeration of join algorithm types Change this class depending on actual algorithms you have
 * implemented in your query processor
 **/

package qp.operators;

/**
 * @author Yann-Loup
 */
public enum JoinType {

    NESTEDJOIN(0), BLOCKNESTED(1),
    /* SORTMERGE(2), HASHJOIN(3), */
    HASHJOIN(2), SORTMERGE(3), INDEXNESTED(4);

    /**
     * To be compatible with the constant define in the ancien version. <br />
     * before it was a C-like 'enum': so <i>ugly</i> for Java.
     */
    private final int joinId;

    private JoinType(int joinId) {
	this.joinId = joinId;
    }

    /**
     * @return
     */
    public int getJoinId() {
	return this.joinId;
    }

    /**
     * For the optimizer that use the id to make calculation...
     * 
     * @param id
     * @return the JoinType corresponding with the given id.
     */
    public static JoinType getJoinTypeById(int id) {
	switch (id) {
	    case 0:
		return NESTEDJOIN;
	    case 1:
		return BLOCKNESTED;
	    case 2:
		return HASHJOIN;
	    case 3:
		return SORTMERGE;
	    case 4:
		return INDEXNESTED;
	    default:
		return NESTEDJOIN;
	}
    }

    /**
     * Return the number of implemented join.
     * 
     * @return
     */
    public static int numJoinTypes() {
	return 3;

	// return k for k joins
    }

}
