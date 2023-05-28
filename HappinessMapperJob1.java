package org.myorg;
// Import the necessary Java and Hadoop classes 
// Handling input/output exceptions
import java.io.IOException; 
// Representing data types in Hadoop	
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// The class that we extend to create our own custom Mapper class
import org.apache.hadoop.mapreduce.Mapper;

// Declaring the class HappinessMapper which extends the Hadoop Mapper class
// LongWritable and Text specify the input key-value types for the Mapper
// Text and Text specify the intermediate output key-value types for the Mapper
public class HappinessMapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
	
//  Declaring two instance variables, country and happinessData	
    private Text country = new Text();
    private Text happinessData = new Text(); 
//  It contains a tab-separated string of values for year, happiness score, and so on
//  class name   object name = new constructor name
    
//  @override is used to change the original behavior of the method “Map" in the parent Class "Mapper" 
    
    @Override
//  Declaring the map method which is the main processing function of the Mapper 
//  The Context object is used to emit key-value pair from one component to another component (here from the Mapper to the reducer)
//  LongWritable is the data file ID and the Text is the actual text in the data file
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
//      The actual code inside the Mapper
        if (key.get() == 0) {
//      Skip the header row
            return;
        }
//      Splitting the value parameter into an array of strings by tab ("\t") delimiter
//      It is string because we do not want to emit this
//      Converting the Hadoop data type(text) into Java data type(string)
        String[] happinessRecord = value.toString().split("\t");

//		Declaring eight string variables, country, happinessScore and etc...
        String countryName = happinessRecord[0];
        String happinessScore = happinessRecord[2];
        String economy = happinessRecord[3];
        String health = happinessRecord[4];
        String freedom = happinessRecord[5];
        String trust = happinessRecord[6];
        String generosity = happinessRecord[7];
        String year = happinessRecord[8];

//      sets the key value of the mapper output to the country name,  
//      enabling effective grouping and aggregation of happiness data by country in the Reduce phase. 
        country.set(countryName); 
 
//  	Emitting the intermediate key- value pair from the mappe to the reducer         
        happinessData.set(happinessScore + "," + economy + "," + health + "," + freedom + "," + trust + "," + generosity+ "," + year );
        
//      Writing the intermediate key-value pair to the context
        context.write(country, happinessData);
    }
}
