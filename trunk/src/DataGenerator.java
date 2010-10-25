import java.io.IOException;

/**
 * 
 */

/**
 * @author workshop
 *
 */
public class DataGenerator {

	/**
	 * 
	 */
	public DataGenerator() {
		// TODO Auto-generated constructor stub
	}

	public void generator(String dir)
	{
		String table_schema[]={"Flights","Aircrafts","Employees","Certified","Schedule"};
		int numtuples[]={1500,1500,1500,1500,1500};
		int len=table_schema.length;
		
		RandomDB rand=new RandomDB();
		for(int i=0;i<len;i++)
		{
			
			//table_schema[i]=dir+"\\"+table_schema[i];
			System.out.println("Start generating table "+table_schema[i]+" ...");
			rand.randomGenerator(table_schema[i], numtuples[i]);
			try {
				ConvertTxtToTbl.convert(table_schema[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
			System.out.println("Table "+table_schema[i]+" generated successfully!");
		}		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String dir="C:\\workspace\\sql";
		dir="";
		DataGenerator datag=new DataGenerator();
		datag.generator(dir);
		

	}

}
