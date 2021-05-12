package section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String> {

	@Override
	public String apply(String input) {
		String[] inputs = input.split(",");
		String sessionId = inputs[0];
		String userId = inputs[1];
		String userName = inputs[2];
		String videoId = inputs[3];
		String duration = inputs[4];
		String startTime = inputs[5];
		String sex = inputs[6];
		String output = "";
		if (sex.equals("1")) {
			output = sessionId + "," + userId + "," + userName + "," + videoId + "," + duration + "," + startTime + ","
					+ sex + ", M";
		} else if (sex.equals("2")) {
			output = sessionId + "," + userId + "," + userName + "," + videoId + "," + duration + "," + startTime + ","
					+ sex + ", F";
		} else if (sex.equals("2")) {
			output = input;
		}
		return output;
	}
}

public class MapElementSimpleFunction {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> pUserList = pipeline.apply(TextIO.read()
				.from("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\user.csv"));
		PCollection<String> pOutput = pUserList.apply(MapElements.via(new User()));
		pOutput.apply(TextIO.write()
				.to("C:\\Users\\Synechron\\eclipse-workspace\\ApacheBeam\\src\\main\\java\\section3\\userOutput.csv")
				.withNumShards(1).withSuffix(".csv"));
		pipeline.run();
	}

}
