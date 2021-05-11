package section2;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class InMemoryExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<CustomerEntity> pList = pipeline.apply(Create.of(getCustomers()));
		PCollection<String> str = pList
				.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()));

		str.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section2\\customer.csv")
				.withNumShards(1).withSuffix(".csv"));

		pipeline.run();
	}

	static List<CustomerEntity> getCustomers() {

		return Arrays.asList(new CustomerEntity("1000", "John"), new CustomerEntity("1001", "Kumar"));
	}
}
