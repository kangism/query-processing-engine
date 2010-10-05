
/**
This is base class for all the operators **/

package qp.operators;

import java.util.Iterator;

import qp.utils.Batch;
import qp.utils.Schema;

/**
 * Should implement {@link Iterator}, but it does not.
 *
 */
public class Operator{

    /**
     * Whether it is OperatorType.SELECT/ Optype.PROJECT/OpType.JOIN.
     */
    OperatorType operatorType;
    
    /**
     * Whether it is OpType.SELECT/ Optype.PROJECT/OpType.JOIN.
     * @deprecated We should now use OperatorType.
     */
    int optype;
    /**
     * Schema of the result at this operator.
     */
    Schema schema;

    /**
     * @deprecated
     * @param type
     */
    public Operator(int type){
	this.optype = type;
    }

    public Operator(OperatorType operatorType){
	this.operatorType= operatorType;
    }

    public Schema getSchema(){
	return schema;
    }

    public void setSchema(Schema schm){
	this.schema = schm;
    }

    /**
     * @return the operatorType
     */
    public OperatorType getOperatorType() {
        return operatorType;
    }

    /**
     * @param operatorType the operatorType to set
     */
    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public void setOpType(int type){
	this.optype = type;
    }

    public int  getOpType(){
	return optype;
    }


    /**
     * @return
     */
    public boolean open(){

	return true;
    }

    /**
     * @return
     */
    public Batch next(){
	System.out.println("Operator:  ");
	return null;
    }

    /**
     * @return
     */
    public boolean close(){

	return true;
    }

    
    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    public Object clone(){
	return new Operator(optype);
    }

}










