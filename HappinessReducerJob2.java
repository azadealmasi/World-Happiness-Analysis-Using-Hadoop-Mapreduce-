package org.myorg;
//Import the necessary Java and Hadoop classes 
//Handling input/output exceptions
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// Defining the HappinessReducer class which extends the Hadoop Reducer class
// Specifying the input and output key-value types for the reducer
public class HappinessReducerJob2 extends Reducer<Text, Text, Text, Text> {
//  @override is used to change the original behavior of the “map" method in the parent Class "Mapper" 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
    // Write the header row to the output file
		context.write(new Text("Changes in happiness score and other factors for different country\n"), null);
	}	
//	The CalcPercent() method is a utility method that takes two arguments, max and min, and calculates the percentage change between them.
	private float CalcChange( String max,String min ) {		
		return (float) ((Double.parseDouble(max))-(Double.parseDouble(min)));
	}
//  It takes in three arguments: the key (the country name), an iterable collection of values (the happiness data), 
//  and a Context object that is used to write the final output.
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	 
//	    The actual code inside the reducer
//      By setting maxHappinessScore to Double.MIN_VALUE, we are initializing it to a value that is smaller than any real value that
//      we might encounter during the iteration.Therefore, as soon as we encounter a real value, it will be greater than maxHappinessScore
//      and will replace the initial value.
//      Initializing the variables that will be used to calculate the maximum and minimum values               
    	double maxHsYear = 0, minHsYear = 0;
        String maxHappinessScore, maxHsEconomy, maxHsHealth, maxHsFreedom, maxHsTrust, maxHsGenerosity;
        String minHappinessScore, minHsEconomy, minHsHealth, minHsFreedom, minHsTrust, minHsGenerosity;
        String[] happinessData;
        String result;
        float pHappinessScore = 0, pEconomy = 0, pHealth = 0,
        	  pFreedom = 0, pTrust = 0, pGenerosity = 0;
//      This loop iterates over the values (happiness data) for each key (country name) and extracts the happiness score and other values
//      for each record. It then updates the maximum and minimum happiness scores and the corresponding values if a new maximum or 
//      minimum score is found.
        for (Text val : values) {
//      	Converting the Hadoop data type(text) into Java data type(string)
        	happinessData = val.toString().split(",");
//     	 	The method takes a String as input and returns a double
            maxHappinessScore = happinessData[0];
        	maxHsYear = Double.parseDouble(happinessData[1]);
        	maxHsEconomy = happinessData[2];
        	maxHsHealth= happinessData[3];
        	maxHsFreedom = happinessData[4];
        	maxHsTrust = happinessData[5];
        	maxHsGenerosity = happinessData[6];
        	minHappinessScore = happinessData[7];
        	minHsYear = Double.parseDouble(happinessData[8]);
        	minHsEconomy = happinessData[9];
        	minHsHealth= happinessData[10];
        	minHsFreedom = happinessData[11];
        	minHsTrust = happinessData[12];
        	minHsGenerosity = happinessData[13];
//			Calculate Changes in Values
            if (maxHsYear > minHsYear) {
	        	pHappinessScore = CalcChange(maxHappinessScore, minHappinessScore);
	            pEconomy = CalcChange(maxHsEconomy, minHsEconomy);
	    		pHealth = CalcChange(maxHsHealth, minHsHealth);
	    		pFreedom = CalcChange(maxHsFreedom, minHsFreedom);
	    		pTrust = CalcChange(maxHsTrust, minHsTrust);
	    		pGenerosity = CalcChange(maxHsGenerosity, minHsGenerosity);
            }
            else
            {
	        	pHappinessScore = CalcChange(minHappinessScore, maxHappinessScore);
	            pEconomy = CalcChange(minHsEconomy, maxHsEconomy);
	    		pHealth = CalcChange(minHsHealth, maxHsHealth);
	    		pFreedom = CalcChange(minHsFreedom, maxHsFreedom);
	    		pTrust = CalcChange(minHsTrust, maxHsTrust);
	    		pGenerosity = CalcChange(minHsGenerosity, maxHsGenerosity);
            }
        }
//			The output of the reducer2 is a formatted string that includes the percent change of different variables over a range of years.
	        result = String.format(
				        							 "\n" +
        		"The Year of Max HappinessScore : %.0f\n" +
        		"The Year of Min HappinessScore : %.0f\n" +
				"Changes in HappinessScore      : %.2f\n" +
				"Changes in Health              : %.2f\n" +
				"Changes in Economy             : %.2f\n" +
				"Changes in Freedom             : %.2f\n" +
				"Changes in Trust               : %.2f\n" +
				"Changes in Generosity          : %.2f\n" +
												    "\n" +
				"***************************************" ,
				maxHsYear, minHsYear,
        		pHappinessScore,pHealth,pEconomy,
        		pFreedom, pTrust, pGenerosity); 
	        context.write(new Text("Name Of The Country : "), null);
        	context.write(key, new Text(result));      
    }
}