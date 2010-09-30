package qp.operators;

import qp.utils.Condition;
import qp.utils.Schema;

/**
 *  This is base class for all the join operators.
 */
public class Join extends Operator{

    /**
     * left child.
     */
    Operator left;
    /**
     * right child.
     */
    Operator right;
    /**
     * join condition.
     */
    Condition con;
    /**
     * Number of buffers available.
     */
    int numBuff;

    /**
     * JoinType.NestedJoin/SortMerge/HashJoin/BlockNestedJoin
     */
    int jointype;
    /**
     * Each join node is given a number
     */
    int nodeIndex;

    /**
     * @param left
     * @param right
     * @param cn
     * @param type
     */
    public Join(Operator left, Operator right, Condition cn, int type){
	super(type);
	this.left=left;
	this.right=right;
	this.con=cn;

    }

	/* number of buffers available to this join operator */

    public void setNumBuff(int num){
	this.numBuff = num;
    }

    public int getNumBuff(){
	return numBuff;
    }



	/* index of this node in query plan tree */

    public int getNodeIndex(){
	return nodeIndex;
    }

    public void  setNodeIndex(int num){
	this.nodeIndex = num;

    }


    public int getJoinType(){
	return jointype;
    }

	/* type of join */

    public void setJoinType(int type){
	this.jointype=type;
    }

    public Operator getLeft(){
	return left;
    }

    public void setLeft(Operator left){
	this.left = left;
    }

    public Operator getRight(){
	return right;
    }

    public void setRight(Operator right){
	this.right = right;
    }

    public void setCondition(Condition cond){
	this.con = cond;
    }

    public Condition getCondition(){
	return con;
    }

    /* (non-Javadoc)
     * @see qp.operators.Operator#clone()
     */
    public Object clone(){
	Operator newleft = (Operator) left.clone();
	Operator newright =(Operator) right.clone();
	Condition newcond = (Condition) con.clone();

	Join jn = new Join(newleft,newright,newcond,optype);
	Schema newsche = newleft.getSchema().joinWith(newright.getSchema());
	jn.setSchema(newsche);
	jn.setJoinType(jointype);
	jn.setNodeIndex(nodeIndex);
	jn.setNumBuff(numBuff);
	return jn;

    }

}





