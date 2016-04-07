package vidooly.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import vidooly.utils.HBaseUtils;

public class HbaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private byte[] snippet;
	private JSONParser parser;

	public void setup(Context context) {
		Configuration configuration = context.getConfiguration();
//		HBaseUtils.initializeEmrAccount();
		this.snippet = Bytes.toBytes(configuration.get("SNIPPET"));
		this.parser = new JSONParser();
	}

	public void map(LongWritable key, Text value, Context context) {
		try {
			System.out.println(value.toString());
			JSONObject obj = (JSONObject) this.parser.parse(value.toString());
			String id = (String) obj.get("id");

			JSONObject json = (JSONObject) obj.get("snippet");
			String publishedAt = (String) json.get("publishedAt");
			String date[] = publishedAt.split("T");

			String subscriber = (String) json.get("channelId");
			String type = (String) json.get("type");

			String rowKey = date[0] + ":" + subscriber + ":" + id;
			Put put = new Put(Bytes.toBytes(rowKey));

			put.addColumn(this.snippet, Bytes.toBytes("publishedAt"), Bytes.toBytes(publishedAt));
			put.addColumn(this.snippet, Bytes.toBytes("channelId"), Bytes.toBytes(subscriber));
			put.addColumn(this.snippet, Bytes.toBytes("type"), Bytes.toBytes(type));

			json = (JSONObject) obj.get("contentDetails");
			JSONObject jstype = (JSONObject) json.get(type);
			JSONObject resourceId = (JSONObject) jstype.get("resourceId");
			String channelId = (String) resourceId.get("channelId");

			String videoId = resourceId.get("videoId") == null ? "" : (String) resourceId.get("videoId");
			String categoryId = resourceId.get("categoryId") == null ? "" : (String) resourceId.get("categoryId");

			put.addColumn(this.snippet, Bytes.toBytes("resourceId.channelId"), Bytes.toBytes(channelId));
			put.addColumn(this.snippet, Bytes.toBytes("videoId"), Bytes.toBytes(videoId));
			put.addColumn(this.snippet, Bytes.toBytes("categoryId"), Bytes.toBytes(categoryId));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
}
