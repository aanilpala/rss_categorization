package rss.categorizer.dummy;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class DateTest {

	
	public static void main(String[] args) {
		String a = "15 Jan 2015 00:00:00 GMT";
		
		DateFormat format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
		
		try {
			System.out.println(format.parse(a).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
}
