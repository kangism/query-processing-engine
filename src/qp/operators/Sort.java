/**
 * 
 */
package qp.operators;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import qp.optimizer.BufferManager;
import qp.utils.AttributeOption;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * Content the Sort Algorithm which is not completed yet. Do only the phase ONE of the external
 * sort. Can Sort several given attributes. DESC not implemented yet
 * 
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
     * The file num of the temp file.
     */
    int filenum = 0;
    /**
     * The current temp file name.
     */
    String tempFile = "";

    public Sort(Operator base, Vector<AttributeOption> as, OperatorType type) {
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
	AttributeOption attr;
	int index;
	for (int i = 0; i < attrSet.size(); i++) {
	    attr = attrSet.elementAt(i);
	    index = baseSchema.indexOf(attr.getAttribute());
	    attr.setAttributeIndexInSchema(index);
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

	// the phase One of sort is finished
	if (tuplesInMem.isEmpty()) {
	    return null;
	}

	for (int i = 0; i < tuplesInMem.size(); i++) {
	    // XXX For now I just sort on the first given attribute without any OPTION (ASC or DESC)
	    findGoodPlaceASC(tuplesInMem.get(i), outbatch, 0);

	}

	filenum++;
	tempFile = "SortTemp-" + String.valueOf(filenum);
	try {
	    ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile));
	    out.writeObject(outbatch);
	    out.close();
	} catch (IOException io) {
	    System.out.println("Sort:writing the temporay file error");
	}

	return outbatch;
    }

    private void findGoodPlaceASC(Tuple tuple, Batch outbatch, int sortAttributeIndex) {
	/**
	 * Flag to know if we have found the right place.
	 */
	boolean found = false;

	// We add each tuples one by one and we try to find the good position!

	// we add the first one
	if (outbatch.isEmpty()) {
	    outbatch.add(tuple);
	    found = true;
	}

	// case: smaller than the first one
	if (!found && Tuple.compareTuples(tuple, outbatch.elementAt(0), attrSet.get(sortAttributeIndex).getAttributeIndexInSchema()) < 0) {
	    // The tuples is smaller than the first attribute
	    outbatch.insertElementAt(tuple, 0);
	    found = true;
	}

	// case: larger than the last one
	if (!found && Tuple.compareTuples(tuple, outbatch.elementAt(outbatch.size() - 1), attrSet.get(sortAttributeIndex).getAttributeIndexInSchema()) > 0) {
	    // The tuples is larger than the last attribute
	    outbatch.add(tuple);
	    found = true;
	}
	int j = 1;
	while (!found) {
	    if (Tuple.compareTuples(tuple, outbatch.elementAt(j - 1), attrSet.get(sortAttributeIndex).getAttributeIndexInSchema()) > 0
		    && Tuple.compareTuples(tuple, outbatch.elementAt(j), attrSet.get(sortAttributeIndex).getAttributeIndexInSchema()) < 0) {
		// the tuple is between the element j-1 and j
		outbatch.insertElementAt(tuple, j);
		found = true;
	    } else if (Tuple.compareTuples(tuple, outbatch.elementAt(j - 1), attrSet.get(sortAttributeIndex).getAttributeIndexInSchema()) == 0) {
		// the equality case need to check the next attribute of sort.
		equalityCaseASC(tuple, outbatch, sortAttributeIndex, j);
		found = true;
	    }
	    j++;
	}
    }

    private void equalityCaseASC(Tuple tuple, Batch outbatch, int sortAttributeIndex, int j){
	    if(sortAttributeIndex==attrSet.size()-1){
		//we are sorting on the last attribute Sort. The position is not important
		outbatch.insertElementAt(tuple, j);
	    }else{
		// there is at least one other sort attribute we should compare with sortAttributeIndex+1
		
		boolean found=false;
		if(Tuple.compareTuples(tuple, outbatch.elementAt(j-1), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) < 0){
		    outbatch.insertElementAt(tuple, j-1);
		    found=true;
		}
		int jj=j-1;
		while(!found){
		    if(Tuple.compareTuples(tuple, outbatch.elementAt(jj), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) == 0 && Tuple.compareTuples(tuple, outbatch.elementAt(jj+1), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) == 0 && Tuple.compareTuples(tuple, outbatch.elementAt(jj), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) > 0 && Tuple.compareTuples(tuple, outbatch.elementAt(jj+1), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) < 0){
			outbatch.insertElementAt(tuple, jj+1);
			found=true;
		    }else if(Tuple.compareTuples(tuple, outbatch.elementAt(jj+1), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) != 0){
			outbatch.insertElementAt(tuple, jj+1);
			found=true;
		    }else if(Tuple.compareTuples(tuple, outbatch.elementAt(jj), attrSet.get(sortAttributeIndex+1).getAttributeIndexInSchema()) == 0) {
			equalityCaseASC(tuple, outbatch, sortAttributeIndex+1, jj+1);
			found = true;
		    }
		    jj++;
		}
	    }
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
	Sort newproj = new Sort(newbase, newattr, operatorType);
	newproj.setSchema(newbase.getSchema());
	return newproj;
    }

}
