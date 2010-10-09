/** the format of the parse SQL quer, also see readme file **/

package qp.utils;

import java.util.Vector;

public class SQLQuery {

    Vector<Attribute> projectList; // List of project attributes from select clause
    Vector<String> fromList; // List of tables in from clause
    Vector<Condition> conditionList; // List of conditions appeared in where clause

    /**
     * represent again the the selection and join conditions in separate lists
     **/

    Vector<Condition> selectionList; // List of select predicates
    Vector<Condition> joinList; // List of join predicates

    Vector<Attribute> groupbyList; // List of attibutes in groupby clause
    boolean isDistinct = false; // Whether distinct key word appeared in select clause

    Vector<AttributeOption> orderbyList; // List of attributes in orderby clause



    public SQLQuery(Vector<Attribute> list1, Vector<String> list2, Vector<Condition> list3, Vector<Attribute> list4) {
	projectList = list1;
	fromList = list2;
	conditionList = list3;
	groupbyList = list4;
	orderbyList = null;
	splitConditionList(conditionList);

    }

    public SQLQuery(Vector<Attribute> list1, Vector<String> list2, Vector<Condition> list3) {
	projectList = list1;
	fromList = list2;
	conditionList = list3;
	groupbyList = null;
	orderbyList = null;
	splitConditionList(conditionList);
    }

    // 12 july 2003 (whtok)
    // Constructor to handle no WHERE clause case
    public SQLQuery(Vector<Attribute> list1, Vector<String> list2) {
	projectList = list1;
	fromList = list2;
	conditionList = null;
	groupbyList = null;
	orderbyList = null;
	joinList = new Vector<Condition>();
	selectionList = new Vector<Condition>();
    }

    /**
     * split the condition list into selection, and join list
     */
    protected void splitConditionList(Vector<Condition> tempVector) {
	selectionList = new Vector<Condition>();
	joinList = new Vector<Condition>();
	for (int i = 0; i < tempVector.size(); i++) {
	    Condition cn = (Condition) tempVector.elementAt(i);
	    if (cn.getOpType() == Condition.SELECT)
		selectionList.add(cn);
	    else
		joinList.add(cn);
	}
    }

    public void setIsDistinct(boolean flag) {
	isDistinct = flag;
    }

    public boolean isDistinct() {
	return isDistinct;
    }


    public Vector<Attribute> getProjectList() {
	return projectList;
    }

    public Vector<String> getFromList() {
	return fromList;
    }

    public Vector<Condition> getConditionList() {
	return conditionList;
    }

    public Vector<Condition> getSelectionList() {
	return selectionList;
    }

    public Vector<Condition> getJoinList() {
	return joinList;
    }

    public void setGroupByList(Vector<Attribute> list) {
	groupbyList = list;
    }

    public Vector<Attribute> getGroupByList() {
	return groupbyList;
    }

    public void setOrderByList(Vector<AttributeOption> list) {
	orderbyList = list;
    }

    public Vector<AttributeOption> getOrderByList() {
	return orderbyList;
    }

    public int getNumJoin() {
	if (joinList == null)
	    return 0;

	return joinList.size();
    }

    public int getNumSort() {
	if (orderbyList == null)
	    return 0;

	return orderbyList.size();
    }

}
