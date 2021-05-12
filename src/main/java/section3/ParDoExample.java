package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustFilter extends DoFn<String, String> {

	@ProcessElement
	public void processElement(ProcessContext c) {
		String line = c.element();
		String arr[] = line.split(",");
		if (arr[3].equals("Los Angeles")) {
			c.output(line);
		}
	}
}

public class ParDoExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pUserList = pipeline.apply(TextIO.read().from(
				"C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\customerParDo.csv"));
		PCollection<String> pOutput = pUserList.apply(ParDo.of(new CustFilter()));
		pOutput.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\customerParDo.csv")
				.withHeader("ID,Name,LastName,City")
				.withNumShards(1).withSuffix(".csv"));
		pipeline.run();

	}

}
