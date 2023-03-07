package mapreduce;

import mapreduce.MapreduceMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;

public class MapreduceDriver extends Configured implements Tool {

    public static final String CONFIG_WORKFLOW_ID = "WorkflowID";
    private static final Logger log = Logger.getLogger(MapreduceDriver.class);

    public static void main(String[] args) throws Exception {

        final Configuration conf = new Configuration();
        final int res = ToolRunner.run(conf, new MapreduceDriver(), args);
        System.exit(res);

    }

    public int run(String[] args) throws Exception {

        //Validate Arguments
        if (args.length != 4) {
            System.out.println("Enter proper argument as Input Path, Output Path, Workflow ID, CVDP JAR Lib Path.");
            System.exit(-1);
        }


        //Configure Configuration
        final Configuration conf = getConf();
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.map.memory.mb", "8192");
        conf.set("mapreduce.map.java.opts", "-Xmx6144m");
        conf.set("mapreduce.job.split.metainfo.maxsize", "-1");
        conf.set(CONFIG_WORKFLOW_ID, args[2]);


        //Configure Job
        final String libJarPath = args[3];
        final FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "MapreduceDriver.class");
        addJarsToClassPath(job, libJarPath + "/lib", fs);
        job.setMapperClass(MapreduceMapper.class);
        job.setJarByClass(MapreduceDriver.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        //Configure Input and OutPut Formats
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);


        //Configure Outputs
        MultipleOutputs.addNamedOutput(job, "Bad", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Good", TextOutputFormat.class, NullWritable.class, Text.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Add jars to Classpath
     *
     * @param job
     * @param path
     * @param fs
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void addJarsToClassPath(final Job job, final String path, final FileSystem fs) throws FileNotFoundException, IOException {

        final RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(path), true);

        while (fileStatusListIterator.hasNext()) {
            final LocatedFileStatus fileStatus = fileStatusListIterator.next();
            job.addFileToClassPath(fileStatus.getPath());
        }
    }
}