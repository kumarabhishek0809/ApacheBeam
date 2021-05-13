package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class DistinctExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pCollectionOfDistinct = pipeline.apply(TextIO.read()
				.from("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section4\\Distinct.csv"));
		PCollection<String> distinctObjects = pCollectionOfDistinct.apply(Distinct.<String>create());
		PDone distinctCollectionWrite = distinctObjects.apply(TextIO.write().to(
				"C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section4\\DistinctOutput.csv")
				.withNumShards(1).withSuffix(".csv"));
		pipeline.run();
	}

}
