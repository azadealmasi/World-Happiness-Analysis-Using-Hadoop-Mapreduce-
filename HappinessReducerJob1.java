package org.myorg;
//Import the necessary Java and Hadoop classes 
//Handling input/output exceptions
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// Defining the HappinessReducer class which extends the Hadoop Reducer class
// Specifying the input and output key-value types for the reducer
public class HappinessReducerJob1 extends Reducer<Text, Text, Text, Text> {
//  @override is used to change the original behavior of the “map" method in the parent Class "Mapper" 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
    // Write the header row to the output file
			context.write(new Text("Country	"
					+ "MaxHappinessScore	Year(MaxHS)	Economy(MaxHS)	Health(MaxHS)	Freedom(MaxHS)	Trust(MaxHS)	Generosity(MaxHS)"
					+ "MinHappinessScore	Year(MinHS)	Economy(MinHS)	Health(MinHS)	Freedom(MinHS)	Trust(MinHS)	Generosity(MinHS)"), null);
	}	
//   It takes in three arguments: the key (the country name), an iterable collection of values (the happiness data), 
//   and a Context object that is used to write the final output.
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//   The actual code inside the reducer
//    By setting maxHappinessScore to Double.MIN_VALUE, we are initializing it to a value that is smaller than any real value that
//    we might encounter during the iteration.Therefore, as soon as we encounter a real value, it will be greater than maxHappinessScore
//    and will replace the initial value.
//    	Initializing the variables that will be used to calculate the maximum and minimum values
    	double maxHappinessScore = Double.MIN_VALUE;    	
		double minHappinessScore = Double.MAX_VALUE;
        double happinessScore;
        String economy, health, freedom, trust, generosity,year;
        String maxEconomy = null, maxHealth = null, maxFreedom = null, maxTrust = null,
        	   maxGenerosity = null,maxYear = null;
        String minEconomy = null, minHealth = null, minFreedom = null, minTrust = null,
        	   minGenerosity = null,minYear = null;
        String[] happinessData;
        String result;
//      This loop iterates over the values (happiness data) for each key (country name) and extracts the happiness score and other values
//      for each record. It then updates the maximum and minimum happiness scores and the corresponding values if a new maximum or 
//      minimum score is found.
        for (Text val : values) {
//      	Converting the Hadoop data type(text) into Java data type(string)
        	happinessData = val.toString().split(",");
//     	 	The method takes a String as input and returns a double
            happinessScore = Double.parseDouble(happinessData[0]);
            economy = happinessData[1];
            health= happinessData[2];
            freedom = happinessData[3];
            trust = happinessData[4];
            generosity = happinessData[5];
            year = happinessData[6];
//    		Calculate the maximum of variables
            if (happinessScore > maxHappinessScore) {
                maxHappinessScore = happinessScore;
                maxYear = year;
                maxEconomy = economy;
                maxHealth = health;
                maxFreedom = freedom;
                maxTrust = trust;
                maxGenerosity = generosity;
            }
//    		Calculate the minimum of variables            
            if (happinessScore < minHappinessScore) {
                minHappinessScore = happinessScore;
                minYear = year;
                minEconomy =economy;
                minHealth = health;
                minFreedom = freedom;
                minTrust = trust;
                minGenerosity = generosity;
            }            
        }
//		Data preparation for mapper job 2 with specified order and format.
        result = String.format("%.2f	%s	%s	%s	%s	%s	%s	"
    						 + "%.2f	%s	%s	%s	%s	%s	%s",
                maxHappinessScore, maxYear, maxEconomy, maxHealth, maxFreedom, maxTrust, maxGenerosity,
                minHappinessScore, minYear, minEconomy, minHealth, minFreedom, minTrust, minGenerosity);
//      Writing the intermediate key-value pair to the context	
        context.write(key, new Text(result));
    }
}