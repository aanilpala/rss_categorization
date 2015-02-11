package rss.categorizer.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import rss.categorizer.model.StreamItem;

public class DictionaryWriter {
	public static void main(String[] args) {
		
		StringToInt stringToInt = new StringToInt();
		
		try {
		BufferedReader br = new BufferedReader(new FileReader("./src/main/resources/refined/part-00000"));
		String line;
		
			while ((line = br.readLine()) != null) {
				String[] terms = line.replaceAll("\\(|\\)", "").split(",");
				
				String[] terms_array = terms[1].toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split("\\s+", -1);;
				
				for(String term : terms_array) {
					if(term.isEmpty()) continue;
					//System.out.println(stringToInt.toInt(term));
					stringToInt.toInt(term);
				} 
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		Map<String, Integer> dictionary = stringToInt.getMap();
		
		try {
			FileWriter writer = new FileWriter("./src/main/resources/dictionary");
			
			for(String key : dictionary.keySet()) {
				writer.append(key);
				writer.append(",");
				writer.append(dictionary.get(key).toString());
				writer.append("\n");
			}
			
			
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
	}
}
