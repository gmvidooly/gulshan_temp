package vidooly.main;

import java.net.URI;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import vidooly.mapper.HbaseBulkLoadMapper;

public class HbaseBulkLoadDriver extends Configured implements Tool {
	private static final String TABLE_NAME = "activities";
	private static final String SNIPPET = "sn";
    
	
	/**
	 * HBase bulk collection example from Activities Data preparation MapReduce
	 * job driver
	 *
	 * args[0]: Activities input path
	 *
	 */
	public static void main(String[] args) {
		try {
			int response = ToolRunner.run(HBaseConfiguration.create(), new HbaseBulkLoadDriver(), args);
			if (response == 0) {
				System.out.println("\nJob is successfully completed...\n");
			} else {
				System.out.println("Job failed...");
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		Configuration configuration = getConf();
		configuration.set("hbase.table.name", TABLE_NAME);
		configuration.set("SNIPPET", SNIPPET);
		configuration.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		configuration.set("fs.s3n.awsAccessKeyId", "awsAccessKey");
		configuration.set("fs.s3n.awsSecretAccessKey","awsSecretAccessKey");
		configuration.set("fs.default.name","s3n://com.spnotes.hadoop.input.books");

		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(HbaseBulkLoadDriver.class);
		job.setJobName("Insert activities in HBase Table::" + TABLE_NAME);
		job.setOutputFormatClass(TableOutputFormat.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setMapperClass(HbaseBulkLoadMapper.class);
		FileInputFormat.addInputPath(job, new Path(new URI(args[0])));

		TableMapReduceUtil.addDependencyJars(job);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
		if (!job.isSuccessful()) {
			result = -1;
		}
		return result;
	}
}