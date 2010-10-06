/**
 * 
 */
package qp.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * @author Yann-Loup
 */
public class Sort extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize; // number of tuples per outbatch

    /**
     * The following fields are requied during execution of the Sort Operator
     **/

    Batch inbatch;
    Batch outbatch;

    public Sort(Operator base, Vector as, int type) {
	super(type);
	this.attrSet = as;
	this.base = base;
    }

    public void setBase(Operator base) {
	this.base = base;
    }

    public Operator getBase() {
	return base;
    }

    public Vector getSortAttr() {
	return attrSet;
    }

    /**
     * Opens the connection to the base operator Also figures out what are the columns to be
     * projected from the base operator
     **/

    public boolean open() {
	/** setnumber of tuples per batch **/
	int tuplesize = schema.getTupleSize();
	batchsize = Batch.getPageSize() / tuplesize;

	return base.open();
    }

    /** Read next tuple from operator */

    public Batch next() {
	// System.out.println("Project:-----------------in next-----------------");
	outbatch = new Batch(batchsize);

	/**
	 * all the tuples in the inbuffer goes to the output buffer
	 **/

	inbatch = base.next();
	// System.out.println("Sort:-------------- inside the next---------------");

	if (inbatch == null) {
	    return null;
	}
	// System.out.println("Sort:---------------base tuples---------");
	for (int i = 0; i < inbatch.size(); i++) {

	    // XXX For now I just sort on the first attribute!


	    boolean found = false;
	    
	    if(outbatch.isEmpty()){
		outbatch.add(inbatch.elementAt(i));
		found = true;
	    }
	    
	    if (!found && Tuple.compareTuples(inbatch.elementAt(i), outbatch.elementAt(0), 0) <= 0) {
		// The tuples is smaller than the first attribute
		outbatch.insertElementAt(inbatch.elementAt(i), 0);
		found = true;
	    }
	    if (!found && Tuple.compareTuples(inbatch.elementAt(i), outbatch.elementAt(outbatch.size() - 1), 0) >= 0) {
		// The tuples is larger than the last attribute
		outbatch.add(inbatch.elementAt(i));
		found = true;
	    }
	    int j = 1;
	    while (!found) {
		if (Tuple.compareTuples(inbatch.elementAt(i), outbatch.elementAt(j - 1), 0) >= 0 && Tuple.compareTuples(inbatch.elementAt(i), outbatch.elementAt(j), 0) <= 0) {
		    // the tuple is between the element j-1 and j
		    outbatch.insertElementAt(inbatch.elementAt(i), j);
		    found = true;
		}
		j++;
	    }

	}
	return outbatch;
    }

    /** Close the operator */
    public boolean close() {
	return base.close();
    }

    public Object clone() {
	Operator newbase = (Operator) base.clone();
	Vector newattr = new Vector();
	for (int i = 0; i < attrSet.size(); i++)
	    newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
	Sort newproj = new Sort(newbase, newattr, optype);
	newproj.setSchema(newbase.getSchema());
	return newproj;
    }

}
