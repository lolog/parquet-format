package adj.felix.parquet;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import adj.felix.parquet.util.ReadSchema;

public class WriteParquet {
	public static void main(String[] args) throws IOException {
		writeParquet("out_data/ne_sys_l.parquet");
	}
	
	public static void writeParquet(String outpath) throws IOException {
		MessageType  schema = ReadSchema.readSchema("ne_sys_l.schema");
		Path         path   = new Path(outpath);
		
		ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(path)
				                                           .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
				                                           .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
				                                           .withCompressionCodec(CompressionCodecName.SNAPPY)
				                                           .withType(schema);
		ParquetWriter<Group> writer = builder.build();
		
		GroupFactory groupFactory = new SimpleGroupFactory(schema);
		
		Group group = groupFactory.newGroup()
		                          .append("city_id", 25)
		                          .append("city_name", "南京");
		      group.addGroup("time")
				   .append("start_time", "2019-01-16 10:46:20")
				   .append("millisecond", 121);

		writer.write(group);
		writer.close();
	}
}
