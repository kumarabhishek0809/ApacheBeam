package section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> output = pipeline.apply(TextIO.read()
				.from("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section2\\input.csv"));
		output.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section2\\output.csv")
				.withNumShards(1).withSuffix(".csv"));
		pipeline.run();
	}

}
