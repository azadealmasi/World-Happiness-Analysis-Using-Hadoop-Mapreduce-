package org.myorg;
//Import the necessary Java and Hadoop classes 
//Handling input/output exceptions
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
//Class declaration
public class HappinessDriver {
//  The main method declaration: the main method that any java program must have 
    public static void main(String[] args) throws Exception {
//		Create the "conf" object from the "configuration" class, which provides access to configuration parameters necessary for Hadoop job
        Configuration conf1 = new Configuration();
//		The command that we use to run the program in googlecolab have 2 components so the code here has 2 components(the input path, the output path)
        if (args.length != 2) {
            System.err.println("Usage: Happiness.jar <input path> <output path>");
            System.exit(-1);
          }
//		Configure parameters for the created job1
        Job job1 = Job.getInstance(conf1, "HappinessJob1");
//		Set the mapper and reducer classes
        job1.setJarByClass(HappinessDriver.class);
        job1.setMapperClass(HappinessMapperJob1.class);
        job1.setReducerClass(HappinessReducerJob1.class);
//		Set the output key and value classes
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutputPath = new Path("intermediate_output");
        FileOutputFormat.setOutputPath(job1, intermediateOutputPath);
//  	Delete output if exists. This part of is code is used to make sure that the output folder
//  	does not already exist in the HDFS because the program will create it anyway.
	    FileSystem hdfs = FileSystem.get(conf1);
	    Path outputDir = intermediateOutputPath;
	    if (hdfs.exists(outputDir))
	    hdfs.delete(outputDir, true);
//		Run the job1 and wait for completion
	    job1.waitForCompletion(true);
//		Create the "conf2" object from the "configuration" class, which provides access to configuration parameters necessary for Hadoop job
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.output.basename", "HappinessOutputJob2.txt");
//		Configure parameters for the created job2
        Job job2 = Job.getInstance(conf2, "HappinessJob2");        
//		Set the output key and value classes
        job2.setJarByClass(HappinessDriver.class);
        job2.setMapperClass(HappinessMapperJob2.class);
        job2.setReducerClass(HappinessReducerJob2.class);
//		Set the output key and value classes
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, intermediateOutputPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//  	Delete output if exists. This part of is code is used to make sure that the output folder
//  	does not already exist in the HDFS because the program will create it anyway.
        FileSystem hdfs2 = FileSystem.get(conf2);
        Path outputDir2 = new Path(args[1]);
	    if (hdfs.exists(outputDir2))
	    hdfs2.delete(outputDir2, true);
//		Run the job2 and wait for completion
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
