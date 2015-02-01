package rss.categorizer.util.feed;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class RSSFetcher {

	private static boolean reset = false;

	public static void main(String[] args) {
		
		ArrayList<Reader> readers = new ArrayList<Reader>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("./src/main/resources/urls.txt"));
			String line;
			while ((line = br.readLine()) != null) {
				String tokens[] = line.split(" ");
				
				String url = tokens[0];
				String category  = tokens[1];
				
				long lastUpdate = reset  ? 0 : Long.parseLong(tokens[2]);
				
				readers.add(new Reader(url, category, lastUpdate));
				
			}
			br.close();
			
			FileWriter rssArch = new FileWriter("./src/main/resources/rss-arch.txt", true);
			
			for(Reader reader : readers) {
				reader.sinkItems(rssArch);
			}
			
			rssArch.close();
			
			FileWriter urls = new FileWriter("./src/main/resources/urls.txt", false);
			
			for(Reader reader : readers) {
				urls.write(reader.toString());
			}
			
			urls.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
}
