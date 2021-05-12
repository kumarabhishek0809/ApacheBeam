package section2;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@DefaultCoder(AvroCoder.class)
@NoArgsConstructor
@AllArgsConstructor
public class CustomerEntity {


	private String id;
	private String name;

}
