package rss.categorizer.dummy;

public class RegexTest {

	public static void main(String[] args) {
		
		String str = "'Older brother' proud of Sterling, Liverpool striker Daniel Sturridge says he is \"proud\" of the performances of fellow Liverpool man Raheem Sterling.'Older brother' proud of Sterling, Liverpool striker Daniel Sturridge says he is \"proud\" of the performances of fellow Liverpool man Raheem Sterlin";
		String[] terms = str.toLowerCase().trim().replaceAll("[^0-9A-Za-z]", " ").split(" ");
		
		for(String each : terms) {
			System.out.println(each);
		}

		
	}
	
}
