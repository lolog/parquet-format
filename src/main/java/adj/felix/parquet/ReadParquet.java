package adj.felix.parquet;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class ReadParquet {
	public static void main(String[] args) throws IOException {
		parquetReaderV2("out_data/ne_sys_l.parquet");
	}
	
	public static void parquetReaderV2(String path) throws IOException {
		GroupReadSupport     readSupport   = new GroupReadSupport();
		Builder<Group>       builder       = ParquetReader.builder(readSupport, new Path(path));
		
		ParquetReader<Group> reader        = builder.build();
		
		Group group = null;
		while((group = reader.read()) != null) {
			Integer city_id   = group.getInteger("city_id", 0);
			String  city_name = group.getString("city_name", 0);
			
			Group   timeGroup = group.getGroup("time", 0);
			String  start_time = timeGroup.getString("start_time", 0);
			Integer  millisecond = timeGroup.getInteger("millisecond", 0);
			
			System.out.println("{city_id:" + city_id + ", city_name:" + city_name 
					+ ", time:{start_time:" + start_time + ", millisecond:" + millisecond + "}}");
		}
	}
}
