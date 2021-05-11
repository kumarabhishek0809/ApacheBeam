package section2;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DefaultCoder(AvroCoder.class)
public class CustomerEntity {
	public CustomerEntity(String id, String name) {
		this.id = id;
		this.name = name;
	}

	private String id;
	private String name;

	public String getName() {
		// TODO Auto-generated method stub
		return this.getName();
	}
}
