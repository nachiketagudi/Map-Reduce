import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class GenerateData {

	public static void main(String[] args) {
		File file=new File("/home/cloudera/input.txt");
		FileWriter writer=null;
		try {
			writer = new FileWriter(file);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Random randomNumber=new Random();
		
		for(int i=0;i<100000000;i++){
		Integer myInt=randomNumber.nextInt();
		if(myInt%4==0){
			try {
				writer.write("Gauva,10,2\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else if(myInt%4==1){
			try {
				writer.write("Apple,20,3\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(myInt%4==2){
			try {
				writer.write("Mango,15,4\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(myInt%4==3){
			try {
				writer.write("Banana,5,5\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		}
		
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
