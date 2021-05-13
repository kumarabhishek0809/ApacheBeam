package section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class CountExample {
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pCollectionOfDistinct = pipeline.apply(TextIO.read()
				.from("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section4\\Distinct.csv"));
		PCollection<Long> distinctObjects = pCollectionOfDistinct.apply(Count.globally());
		distinctObjects.apply(ParDo.of(new DoFn<Long, Void>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		pipeline.run();
	}

}
