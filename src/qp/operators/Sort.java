/**
 * 
 */
package qp.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.AttributeOption;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * @author Yann-Loup
 */
public class Sort extends Operator {

    /**
     * Number of buffers available.
     */
    int numBuff;

    Operator base;
    Vector<AttributeOption> attrSet;
    
    
    /**
     * number of tuples per outbatch
     */
    int batchsize;

    /**
     * The following fields are required during execution of the Sort Operator
     **/

    Batch next;
    Batch outbatch;

    List<Tuple> tuplesInMem;

    
    /**
     * index of the attributes in the base operator that are to be projected
     **/
    int[] attrIndex;
    
    public Sort(Operator base, Vector<AttributeOption> as, int type) {
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

    public Vector<AttributeOption> getSortAttr() {
	return attrSet;
    }

    public void setNumBuff(int num) {
	this.numBuff = num;
    }

    public int getNumBuff() {
	return numBuff;
    }

    /**
     * Opens the connection to the base operator Also figures out what are the columns to be
     * projected from the base operator
     **/

    public boolean open() {
	/** setnumber of tuples per batch **/
	int tuplesize = schema.getTupleSize();
	batchsize = Batch.getPageSize() / tuplesize;
	this.numBuff = BufferManager.getNumBuffer();

	Schema baseSchema = base.getSchema();
	attrIndex = new int[attrSet.size()];
	for (int i = 0; i < attrSet.size(); i++) {
	    AttributeOption attr = attrSet.elementAt(i);
	    int index = baseSchema.indexOf(attr.getAttribute());
	    attrIndex[i] = index;
	}
	
	
	return base.open();
    }

    /** Read next tuple from operator */

    public Batch next() {
	outbatch = new Batch(batchsize);

	tuplesInMem = new ArrayList<Tuple>();

	// all the tuples in the different page of the
	// memory are in one list to make it easier
	for (int i = 0; i < numBuff; i++) {
	    next = base.next();
	    if (next != null) {
		tuplesInMem.addAll(next.getTuples());
	    }
	}

	// the sort is finished
	if (tuplesInMem.isEmpty()) {
	    return null;
	}

	for (int i = 0; i < tuplesInMem.size(); i++) {

	    // XXX For now I just sort on the first given attribute without any OPTION (ASC or DESC)

	    boolean found = false;

	    // We add each tuples one by one and we try to find the good position!

	    // we add the first one
	    if (outbatch.isEmpty()) {
		outbatch.add(tuplesInMem.get(i));
		found = true;
	    }

	    // case: smaller than the first one
	    if (!found && Tuple.compareTuples(tuplesInMem.get(i), outbatch.elementAt(0), attrIndex[0]) <= 0) {
		// The tuples is smaller than the first attribute
		outbatch.insertElementAt(tuplesInMem.get(i), 0);
		found = true;
	    }

	    // case: larger than the last one
	    if (!found && Tuple.compareTuples(tuplesInMem.get(i), outbatch.elementAt(outbatch.size() - 1), attrIndex[0]) >= 0) {
		// The tuples is larger than the last attribute
		outbatch.add(tuplesInMem.get(i));
		found = true;
	    }
	    int j = 1;
	    while (!found) {
		if (Tuple.compareTuples(tuplesInMem.get(i), outbatch.elementAt(j - 1), attrIndex[0]) >= 0 && Tuple.compareTuples(tuplesInMem.get(i), outbatch.elementAt(j), attrIndex[0]) <= 0) {
		    // the tuple is between the element j-1 and j
		    outbatch.insertElementAt(tuplesInMem.get(i), j);
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
	Vector<AttributeOption> newattr = new Vector<AttributeOption>();
	for (int i = 0; i < attrSet.size(); i++)
	    newattr.add(attrSet.elementAt(i).clone());
	Sort newproj = new Sort(newbase, newattr, optype);
	newproj.setSchema(newbase.getSchema());
	return newproj;
    }

}
