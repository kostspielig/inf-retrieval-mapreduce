package ir.assignment06.util;

public class TextUtil {
	
	public static String dropNonAlphaNumericCharacters(String str) {
		char[] originalChars = str.toCharArray();
		char[] newChars = new char[str.length()];

		boolean lastWasSpace = false;
		int idx = 0;
		for (int i = 0; i < originalChars.length; i++) {
			char ch = originalChars[i];
			if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch >= '0' && ch <= '9') {
				newChars[idx++] = ch;
				lastWasSpace = false;
			} else {
				if (!lastWasSpace) {
					newChars[idx++] = ' ';
					lastWasSpace = true;
				}
			}
		}
		return new String(newChars, 0, idx);
	}
}
