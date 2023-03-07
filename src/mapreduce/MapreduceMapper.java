package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static org.apache.hadoop.io.NullWritable.*;

public class MapreduceMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

        //Logging
        private static final String CLASS_NAME = MapreduceMapper.class.getName();
        private static final Logger log = Logger.getLogger(MapreduceMapper.class);
        //Constants
        private static final String CONST_DATASOURCE = "TMC";
        //Outputs
        private static String inputFileName = null;
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
        private String badRecordFileName = null;
        private String goodRecordFileName = null;
        //Utilities
        private Decoder decodeTMC = new Decoder(); // TMC Billing Only!!!
        //Workflow
        private String workflowID = "";

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void setup(Context context) throws IOException {

            //Outputs
            extractFilename(context);
            this.multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
            this.badRecordFileName = "BadRecords/" + formatBadFileName(inputFileName);
            this.goodRecordFileName = "GoodRecords/GoodRecords_TMC_" + new SimpleDateFormat("yyyyMMdd-HHmmss-SSSSSS").format(new Date());

            //Get WorkflowID
            final Configuration configuration = context.getConfiguration();
            workflowID = configuration.get(MapreduceDriver.CONFIG_WORKFLOW_ID);
            if (workflowID == null) {
                workflowID = "";
            }
        }

        private void extractFilename(final Context context) {
            final String METHOD_NAME = "extractFilename";
            // log.info(CLASS_NAME + "." + METHOD_NAME);

            try {

                final String InputFileName =
                        (((FileSplit) context.getInputSplit()).getPath().toString());
                final int InputFileNamePos = InputFileName.lastIndexOf(File.separator);
                if (InputFileNamePos > -1) {
                    inputFileName = InputFileName.substring(InputFileNamePos + 1);
                }

            } catch (final Exception e) {
                log.info(CLASS_NAME + "." + METHOD_NAME);
                log.info(e.getMessage());
            }

        }

        public static String formatBadFileName(String source) {
            final Date date = new java.util.Date();
            return "Badrecords_"+source+"_" + new SimpleDateFormat("yyyyMMdd-HHmmss-SSSSSS").format(date);
        }

        /*
         * map
         *
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

            final String METHOD_NAME = "map";
            String sourceName = this.inputFileName.split("_")[0];
            final String record = value.toString().trim();

            //Decode
            ArrayList<String> jsons = null;

            try {
//                jsons = decodeTMC.decode(record, sourceName);
            } catch (final Exception e) {
                e.printStackTrace();

                // Write Bad Record
                final Text result = new Text(e.toString());
                this.multipleOutputs.write("Bad", NullWritable.get(), result, this.badRecordFileName);
                return;
            }

            //Write Good Records
            try {
                if (jsons != null) {
                    for (String json : jsons) {
                        final Text outputValue = new Text();
                        outputValue.set(json);
                        this.multipleOutputs.write("Good", get(), outputValue, this.goodRecordFileName);
                    }
                }

            } catch (final Exception e) {
                e.printStackTrace();
            }
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (multipleOutputs != null) {
                multipleOutputs.close();
            }
        }

    }
