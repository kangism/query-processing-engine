/**********
 Tuple container class
 **********/
package qp.utils;
import java.io.Serializable;
import java.util.Vector;

/** Tuple - a simple object which holds a Vector
     of data */

public class Tuple implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 8738898719856953652L;
    public Vector<Object> _data;
 


    public Tuple(Vector<Object> d){
	_data=d;
    }

    /** Accessor for data */
    public Vector<Object> data() {
        return _data;
    }

    public Object dataAt(int index){
	return _data.elementAt(index);
    }


    /** Checks whether the join condition is satisfied or not
     ** before performing actual join operation
     **/

    public boolean checkJoin(Tuple right, int leftindex, int rightindex){
	Object leftData = dataAt(leftindex);
	Object rightData = right.dataAt(rightindex);

	if(leftData.equals(rightData))
	    return true;
	else
	    return false;
    }



    /** Joining two tuples Without duplicate column elimination**/


    public Tuple joinWith(Tuple right){ //, Attribute leftAttr, Attribute rightAttr){
	Vector<Object> newData = new Vector<Object>(this.data());
	newData.addAll(right.data());
	return new Tuple(newData);
    }




	/** Compare two tuples in the same table on given attribute **/

    public static int compareTuples(Tuple left,Tuple right, int index){
	return compareTuples(left,right,index,index);
    }


    	/** comparing tuples in different tables, used for join condition checking **/

    public static int compareTuples( Tuple left,Tuple right, int leftIndex, int rightIndex){
	Object leftdata = left.dataAt(leftIndex);
	Object rightdata = right.dataAt(rightIndex);
	if(leftdata instanceof Integer){
	    return ((Integer)leftdata).compareTo((Integer)rightdata);
	}else if(leftdata instanceof String){
	    return ((String)leftdata).compareTo((String)rightdata);

	}else if(leftdata instanceof Float){
	    return ((Float)leftdata).compareTo((Float)rightdata);
	}else{
	    System.out.println("Tuple: Unknown comparision of the tuples");
	    System.exit(1);
	    return 0;
	}
    }
    
    
    /**
     * Return true is 'left' is suppose to be before 'right' when it is sorted by 'attrOp'.
     * Return false if the tuples are equal.
     * @param left
     * @param right
     * @param attrOp
     * @return
     */
    private static boolean goodOrder(Tuple left, Tuple right, AttributeOption attrOp){
	return (attrOp.option==OrderByOption.ASC && compareTuples(left,right,attrOp.getAttributeIndexInSchema())<0)
	|| (attrOp.option==OrderByOption.DESC && compareTuples(left,right,attrOp.getAttributeIndexInSchema())>0);
    }

    /**
     * Make the comparison on several level of attributeOption.
     * @param left
     * @param right
     * @param attrOps
     * @return
     */
    public static boolean goodOrder(Tuple left, Tuple right, Vector<AttributeOption> attrOps){
	if(goodOrder(left, right, attrOps.get(0))){
	    return true;
	}else if(compareTuples(left,right,  attrOps.get(0).getAttributeIndexInSchema())==0){
	    int nextAttrSort=1;
	    while(nextAttrSort<attrOps.size()){
		if(goodOrder(left, right, attrOps.get(nextAttrSort))){
		    return true;
		}
		nextAttrSort++;
	    }
	    return false;
	}else{
	    return false;
	}
    }

}










