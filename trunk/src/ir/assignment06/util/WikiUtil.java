package ir.assignment06.util;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

public class WikiUtil {
	
	WikiModel wikiModel;
	PlainTextConverter textConverter;
	
	public WikiUtil() {
		wikiModel = new WikiModel("", "");
		textConverter = new PlainTextConverter();
	}
	
	public String getPlainTextFromWikiMarkup(String markup) {
		
		wikiModel.setUp();
		String text = wikiModel.render(textConverter, markup);
		wikiModel.tearDown();
		
		return text;
		
	}
	
}
