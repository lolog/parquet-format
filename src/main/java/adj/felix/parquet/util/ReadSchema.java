package adj.felix.parquet.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ReadSchema {
	public static MessageType readSchema(String path) throws IOException {
		URL            url    = ClassLoader.getSystemClassLoader().getResource(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
		
		String line    = null;
		String content = "";
		while((line = reader.readLine()) != null) {
			content += line + "\n";
		}
		
		MessageType messageType = MessageTypeParser.parseMessageType(content);
		
		return messageType;
	}
}
