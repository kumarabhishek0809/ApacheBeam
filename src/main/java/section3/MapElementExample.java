package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		// read
		PCollection<String> plist = pipeline.apply(TextIO.read()
				.from("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section2\\customer.csv"));

		// transformation
		PCollection<String> upperCaseCustomer = plist
				.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));

		// write to file
		PDone writeNewFile = upperCaseCustomer.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section2\\Uppercustomer.csv")
				.withNumShards(1).withSuffix(".csv"));

		pipeline.run();

	}

}
