package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

public class FlattenExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		// read
		PCollection<String> customer_1 = pipeline.apply(TextIO.read().from(
				"C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\customer_1.csv"));

		PCollection<String> customer_2 = pipeline.apply(TextIO.read().from(
				"C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\customer_2.csv"));

		PCollection<String> customer_3 = pipeline.apply(TextIO.read().from(
				"C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\customer_3.csv"));

		// transformation
		PCollectionList<String> upperCaseCustomer = PCollectionList.of(customer_1).and(customer_2).and(customer_3);

		PCollection<String> pCollectionMerge = upperCaseCustomer.apply(Flatten.pCollections());

		// write to file
		PDone writeNewFile = pCollectionMerge.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\FlattenOutput.csv")
				.withNumShards(1)
				.withHeader("ID,Name,LastName,City").withSuffix(".csv"));

		pipeline.run();

	}

}
