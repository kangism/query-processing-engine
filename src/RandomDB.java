import java.util.*;
import java.io.*;
import qp.utils.*;

public class RandomDB {

	private static Random random;

	public RandomDB() {
		random = new Random(System.currentTimeMillis());
	}

	/** Generates a random string of length equal to range * */

	public String randString(int range) {

		String s = "";
		for (int j = 0; j < range; j++)
			s += (new Character((char) (97 + random.nextInt(26)))).toString();
		return s;
	}

	/***************************************************************************
	 * Generates a random string of length equal to range casss: -1:-all
	 * lowercase 0: starts with uppercase 1: all uppercase
	 */
	public String randRangeString(int start, int end, int cases) {
		int range = end - start;
		String s = "";

		for (int j = 0; j < start + range; j++) {
			s += (new Character((char) (97 + random.nextInt(26)))).toString();
		}
		switch (cases) {
		case -1: {
			break;
		}
		case 0: {
			String first = s.charAt(0) + "";
			s = first.toUpperCase() + s.substring(1);
			break;
		}
		case 1: {
			s = s.toUpperCase();
			break;
		}
		}

		return s;
	}

	public int randInt(int start, int end) {
		int range = end - start;
		int random = (int) (Math.random() * range);
		return start += random;
	}

	public void randomGenerator(String schemas,int numtuple)
	{
		/*
		 * if (args.length != 2) { System.out.println("Usage: java RandomDB
		 * <dbname> <numrecords> "); System.exit(1); }
		 */
		 boolean[] pk=null;

		 boolean[] fk=null;
		
		String tblname = schemas;
		String srcfile = schemas + ".det";
		String metafile = schemas + ".md";
		String datafile = schemas + ".txt";
		String statfile = schemas + ".stat";
		// int numtuple = Integer.parseInt(args[1]);
		
		try {
			BufferedReader in = new BufferedReader(new FileReader(srcfile));
			ObjectOutputStream outmd = new ObjectOutputStream(
					new FileOutputStream(metafile));
			PrintWriter outtbl = new PrintWriter(new BufferedWriter(
					new FileWriter(datafile)));
			PrintWriter outstat = new PrintWriter(new BufferedWriter(
					new FileWriter(statfile)));

			outstat.print(numtuple);
			outstat.println();
			/** first line is <number of columns> * */

			String line = in.readLine();
			int numCol = Integer.parseInt(line);
			String[] columnname = new String[numCol];
			String[] datatype = new String[numCol];
			int[] range = new int[numCol];
			String[] keytype = new String[numCol];

			/** second line is <size of tuple = number of bytes> * */
			line = in.readLine();

			int size = Integer.parseInt(line);
			// outstat.print(size);
			// outstat.println();

			/**
			 * Capture information about data types, range and primary/foreign
			 * keys*
			 */
			/** format is <colname><coltype><keytype><attrsize><range> * */

			/** for schema generation * */
			Vector attrlist = new Vector();
			Attribute attr;
			boolean flag = false;
			int i = 0;
			Vector<Vector<String>> reference = new Vector<Vector<String>>();

			while ((line = in.readLine()) != null) {

				StringTokenizer tokenizer = new StringTokenizer(line);
				int tokenCount = tokenizer.countTokens();
				/** get column name * */
				String colname = tokenizer.nextToken();
				columnname[i] = colname;
				/** get data type * */
				datatype[i] = tokenizer.nextToken();

				int type;

				if (datatype[i].equals("INTEGER")) {
					type = Attribute.INT;
					// System.out.println("integer");
				} else if (datatype[i].equals("STRING")) {
					type = Attribute.STRING;
					// System.out.println("String");
				} else if (datatype[i].equals("REAL")) {
					type = Attribute.REAL;
				} else if (datatype[i].equals("TIME")) {
					type = Attribute.TIME;
				} else {

					type = -1;
					System.err.println("invalid data type");
					System.exit(1);
				}

				/** range of the values allowed * */
				range[i] = Integer.parseInt(tokenizer.nextToken());

				/** key type PK/FK/NK * */
				keytype[i] = tokenizer.nextToken();
				int typeofkey;
				if (keytype[i].equals("PK")) {
					pk = new boolean[range[i]];
					typeofkey = Attribute.PK;

				} else if (keytype[i].equals("FK")) {
					fk = new boolean[range[i]];
					typeofkey = Attribute.FK;
					// =========================
					
					String filename=columnname[i].substring(0, columnname[i].indexOf(".")) + ".txt";
					// System.out.println("filename=" + filename);
					String newline;
					File f=new File(filename);
					if(!f.exists())
					{
						System.out.println("Can not open file "+filename);
						return;
					}
					
					BufferedReader br = new BufferedReader(new FileReader(f));
					Vector<String> currefer = new Vector<String>();
					String fields[] = null;
					while ((newline = br.readLine()) != null) {
						fields = newline.split("\t");
						currefer.add(fields[0]);
					}
					br.close();
					
					reference.add(currefer);
					// ===========================
				} else {
					typeofkey = -1;
				}
				int numbytes = Integer.parseInt(tokenizer.nextToken());

				if (typeofkey != -1) {
					if(colname.contains("."))
					colname=colname.substring(colname.indexOf(".")+1);
					attr = new Attribute(tblname, colname, type);// ,typeofkey,numbytes);
				} else {
					if(colname.contains("."))
					colname=colname.substring(colname.indexOf(".")+1);
					attr = new Attribute(tblname, colname, type, typeofkey);
				}
				attr.setAttrSize(numbytes);
				attrlist.add(attr);
				i++;
			}
			Schema schema = new Schema(attrlist);
			schema.setTupleSize(size);
			outmd.writeObject(schema);
			outmd.close();

			Vector<String> instances = new Vector<String>();
			for (i = 0; i < numtuple; i++) {
				// System.out.println("in table "+schemas+" generation: "+i);

				if (pk != null) {
					int numb = random.nextInt(range[0]);
					while (pk[numb] == true) {
						numb = random.nextInt(range[0]);
						// System.out.println("pk["+numb+"]");
					}
					// System.out.println("pk["+numb+"]");
					pk[numb] = true;
					outtbl.print(numb + "\t");
					for (int j = 1; j < numCol; j++) {
						if (datatype[j].equals("STRING")) {
							String temp = randString(range[j]);
							outtbl.print(temp + "\t");
						} else if (datatype[j].equals("FLOAT")) {
							float value = range[j] * random.nextFloat();
							outtbl.print(value + "\t");
						} else if (datatype[j].equals("INTEGER")) {
							// int value = random.nextInt(range[j]);
							// outtbl.print(value + "\t");
							int value = 0;
							if (keytype[j].equals("FK")) {
								int index = getIndex(keytype, j);
								Vector<String> temp = reference.elementAt(index);
								int siz = temp.size();
								int randint = (int) (Math.random() * siz);
								value = Integer.parseInt(temp.elementAt(randint));
								fk[value] = true;
							} else {
								value = random.nextInt(range[j]);
							}
							
							outtbl.print(value + "\t");
						} else if (datatype[j].equals("TIME")) {
							int hour = (int) (Math.random() * 24);
							int minute = (int) (Math.random() * 60);
							String times = hour + ":" + minute + "\t";
							// System.out.println("count="+i);
							outtbl.print(times);
						}

					}
				}

				else {
					// all attributes form a key
					String tup;
					int value = 0;
					boolean containFK = false;
					do {
						tup = "";
						for (int j = 0; j < numCol; j++) {
							if (datatype[j].equals("STRING")) {
								String temp = randString(range[j]);
								// outtbl.print(temp + "\t");
								tup += temp + "\t";
							} else if (datatype[j].equals("FLOAT")) {
								float fvalue = range[j] * random.nextFloat();
								// outtbl.print(value + "\t");
								tup += fvalue + "\t";
							} else if (datatype[j].equals("INTEGER")) {
								/*
								 * int value = random.nextInt(range[j]);
								 * outtbl.print(value + "\t"); tup+=value+"\t";
								 * if (keytype[j].equals("FK")) { fk[value] =
								 * true; }
								 */

								// int value=0;
								if (keytype[j].equals("FK")) {
									int index = getIndex(keytype, j);
									Vector<String> temp = reference.elementAt(index);
									int siz = temp.size();
									int randint = (int) (Math.random() * siz);
									value = Integer.parseInt(temp.elementAt(randint));
									containFK = true;
								} else {
									value = random.nextInt(range[j]);
								}

								tup += value + "\t";

							} else if (datatype[j].equals("TIME")) {
								int hour = (int) (Math.random() * 24);
								int minute = (int) (Math.random() * 60);
								String times = hour + ":" + minute + "\t";
								// System.out.println("count="+i);
								// outtbl.print(times);
								tup += times + "\t";
							}

						}
						
					} while (instances.contains(tup.trim()));
					if (containFK)
						fk[value] = true;
					instances.add(tup.trim());
					
					outtbl.print(tup.trim());
				}

				if (i != numtuple - 1)
					outtbl.println();

			}
			outtbl.close();

			// System.out.println("end of table generation");
			/**
			 * printing the number of distinct values of each column in
			 * <tablename>.stat file
			 */

			for (i = 0; i < numCol; i++) {
				if (datatype[i].equals("STRING")) {
					outstat.print(numtuple + "\t");
				} else if (datatype[i].equals("FLOAT")) {
					outstat.print(numtuple + "\t");
				} else if (datatype[i].equals("INTEGER")) {
					if (keytype[i].equals("PK")) {
						int numdist = getnumdistinct(pk);
						outstat.print(numdist + "\t");
					} else if (keytype[i].equals("FK")) {
						int numdist = getnumdistinct(fk);
						outstat.print(numdist + "\t");
					} else {
						if (numtuple < range[i])
							outstat.print(numtuple + "\t");
						else
							outstat.print(range[i] + "\t");
					}

				} else if (datatype[i].equals("TIME")) {
					// System.out.println("instances.size="+instances.size());
					outstat.print(numtuple + "\t");
				}
			}
			outstat.close();
			in.close();
		} catch (IOException io) {
			System.out.println("error in IO " + io.getMessage());
			System.exit(1);
		}

	}

	public static void main(String[] args) {

	}

	private static int getIndex(String[] datatype, int cur) {
		int len = datatype.length;
		int count = 0;
		String tar = datatype[cur];
		for (int i = 0; i < cur; i++) {
			if (datatype[i].equals(tar)) {
				count++;
			}
		}
		return count;
	}

	public int getnumdistinct(boolean[] key) {
		int lenght = key.length;
		int count = 0;
		for (int i = 0; i < lenght; i++) {
			if (key[i] == true)
				count++;
		}
		return count;
	}

}
