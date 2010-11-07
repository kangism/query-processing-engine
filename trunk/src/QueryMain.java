/** This is main driver program of the query processor **/

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import qp.utils.*;
import qp.operators.*;
import qp.optimizer.*;
import qp.parser.*;

public class QueryMain {

	static PrintWriter out;
	static int numAtts;
	static int numJoinThreshold = 1;

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out
					.println("usage: java QueryMain <queryfilename> <resultfile>");
			System.exit(1);
		}

		/** Enter the number of bytes per page **/

		System.out.println("enter the number of bytes per page");
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String temp;
		try {
			temp = in.readLine();
			int pagesize = Integer.parseInt(temp);
			Batch.setPageSize(pagesize);
		} catch (Exception e) {
			e.printStackTrace();
		}

		String queryfile = args[0];
		String resultfile = args[1];
		FileInputStream source = null;
		try {
			source = new FileInputStream(queryfile);
		} catch (FileNotFoundException ff) {
			System.out.println("File not found: " + queryfile);
			System.exit(1);
		}

		/** scan the query **/

		Scaner sc = new Scaner(source);
		parser p = new parser();
		p.setScanner(sc);

		/** parse the query **/

		try {
			p.parse();
		} catch (Exception e) {
			System.out.println("Exception occured while parsing");
			System.exit(1);
		}

		/** SQLQuery is the result of the parsing **/

		SQLQuery sqlquery = p.getSQLQuery();
		int numJoin = sqlquery.getNumJoin();
		int numSort = sqlquery.getNumSort();

		/**
		 * If there are joins then assigns buffers to each join operator while
		 * preparing the plan
		 **/
		/**
		 * As buffer manager is not implemented, just input the number of
		 * buffers available
		 **/

		if (numJoin != 0 || numSort != 0) {
			System.out.println("enter the number of buffers available");

			try {
				temp = in.readLine();
				int numBuff = Integer.parseInt(temp);

				new BufferManager(numBuff, numJoin); // has static parameters
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/** Let check the number of buffers available is enough or not **/

		int numBuff = BufferManager.getBuffersPerJoin();
		if (numJoin > 0 && numBuff < 3) {
			System.out
					.println("Minimum 3 buffers are required per a join operator ");
			System.exit(1);
		}
		if (numSort > 0 && BufferManager.getNumBuffer() < 3) {
			System.out
					.println("Minimum 3 buffers are required per a sort operator ");
			System.exit(1);
		}

		/**
		 * This part is used When some random initial plan is required instead
		 * of comple optimized plan
		 **/
		/**
		 * RandomInitialPlan rip = new RandomInitialPlan(sqlquery); Operator
		 * logicalroot = rip.prepareInitialPlan(); PlanCost pc = new PlanCost();
		 * int initCost = pc.getCost(logicalroot); Debug.PPrint(logicalroot);
		 * System.out.print("   "+initCost); System.out.println();
		 **/

		Operator root = null;
		if (numJoin < numJoinThreshold) {// DynamicProgrammingOptimizer
			DynamicProgrammingOptimizer dynamicOptimizer = new DynamicProgrammingOptimizer(
					sqlquery);
			Operator logicalroot = dynamicOptimizer.getOptimalPlan();
			if (logicalroot == null) {
				System.out.println("root is null");
				System.exit(1);
			}
			root = DynamicProgrammingOptimizer.makeExecPlan(logicalroot);
		} else {// Use random Optimization algorithm to get a random optimized
				// execution plan
			RandomOptimizer ro = new RandomOptimizer(sqlquery);
			Operator logicalroot = ro.getOptimizedPlan();
			if (logicalroot == null) {
				System.out.println("root is null");
				System.exit(1);
			}
			root = RandomOptimizer.makeExecPlan(logicalroot);
		}

		/** Print final Plan **/
		System.out
				.println("----------------------Execution Plan----------------");
		Debug.PPrint(root);
		System.out.println();

		/** Ask user whether to continue execution of the program **/

		System.out.println("enter 1 to continue, 0 to abort ");

		try {
			temp = in.readLine();
			int flag = Integer.parseInt(temp);
			if (flag == 0) {
				System.exit(1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		long starttime = System.currentTimeMillis();

		if (root.open() == false) {
			System.out.println("Root: Error in opening of root");
			System.exit(1);
		}
		try {
			out = new PrintWriter(
					new BufferedWriter(new FileWriter(resultfile)));
		} catch (IOException io) {
			System.out.println("QueryMain:error in opening result file: "
					+ resultfile);
			System.exit(1);
		}

		/** print the schema of the result **/
		Schema schema = root.getSchema();
		numAtts = schema.getNumCols();
		printSchema(schema);
		Batch resultbatch;

		/** print each tuple in the result **/

		while ((resultbatch = root.next()) != null) {
			for (int i = 0; i < resultbatch.size(); i++) {
				printTuple(resultbatch.elementAt(i));
			}
		}
		root.close();
		out.close();

		long endtime = System.currentTimeMillis();
		double executiontime = (endtime - starttime) / 1000.0;
		System.out.println("Execution time = " + executiontime);

	}

	protected static void printTuple(Tuple t) {
		String temp = "";
		for (int i = 0; i < numAtts; i++) {
			Object data = t.dataAt(i);
			if (data instanceof Integer) {
				temp = String.valueOf(((Integer) data).intValue());
				temp = formatString(temp, 20);
				out.print(temp);
				// out.print(((Integer)data).intValue()+"\t");
			} else if (data instanceof Float) {
				temp = String.valueOf(((Float) data).floatValue());
				temp = formatString(temp, 20);
				out.print(temp);
				// out.print(((Float)data).floatValue()+"\t");
			} else {
				// out.print(((String)data)+"\t");
				temp = (String) data;
				if (isTime(temp))
					temp = formatString(temp, 20);
				else
					temp = formatString(temp, 50);
				out.print(temp);
			}
		}
		out.println();
	}
	
	private static boolean isTime(String src)
	{
		if(!src.contains(":"))
			return false;
		String fields[]=src.split(":");
		boolean flag=true;
		int len=fields.length;
		Pattern pat=Pattern.compile("\\d+");
		Matcher matcher;
		for(int i=0;i<len;i++)
		{
			matcher=pat.matcher(fields[i]);
			if(!matcher.find())
			{
				flag=false;
				break;
			}
				
		}
		return flag;
	}
	
	private static String formatString(String src, int len) {
		int length = len - src.length();
		String temp = src;
		for (int i = 0; i < length; i++) {
			temp += " ";
		}
		return temp;
	}

	protected static void printSchema(Schema schema) {
		String name = "";
		for (int i = 0; i < numAtts; i++) {
			Attribute attr = schema.getAttribute(i);
			name = attr.getTabName() + "." + attr.getColName();
			if (attr.getType() == Attribute.INT
					|| attr.getType() == Attribute.REAL)
				name = formatString(name, 20);
			else if (attr.getType() == Attribute.STRING)
				name = formatString(name, 50);
			else if (attr.getType() == Attribute.TIME)
				name = formatString(name, 20);
			// out.print(attr.getTabName() + "." + attr.getColName() + "  ");
			out.print(name);
		}
		out.println();
	}

}
