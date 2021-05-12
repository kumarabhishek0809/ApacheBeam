package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

class MyFilter implements SerializableFunction<String, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean apply(String input) {
		// TODO Auto-generated method stub
		return input.contains("Los Angeles");
	}

}

public class FilterExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pList = pipeline.apply(TextIO.read().from(
				"C:\\\\Users\\\\Synechron\\\\eclipse-workspace\\\\ApacheBeam\\\\src\\\\main\\\\java\\\\section3\\\\customerParDo.csv"));

		PCollection<String> pOutput = pList.apply(Filter.by(new MyFilter()));

		PDone pDone = pOutput.apply(TextIO.write().to(
				"C:\\\\Users\\\\Synechron\\\\eclipse-workspace\\\\ApacheBeam\\\\src\\\\main\\\\java\\\\section3\\\\customerFilterExample.csv")
				.withNumShards(1).withHeader("ID,Name,LastName,City").withSuffix(".csv"));

		pipeline.run();

	}

}
