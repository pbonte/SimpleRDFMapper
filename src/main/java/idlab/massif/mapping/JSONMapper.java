package idlab.massif.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;


/***
 * Maps JSON structure strings to turtle format.
 * 
 * @author psbonte
 *
 */
public class JSONMapper {

	public static void main(String[] args) {
		String json = "{\"tsReceivedMs\":1592232900780,\"metricId\":\"motionl\",\"timestamp\":1592232900776,\"sourceId\":\"MotionSensor\",\"geohash\":\"u14dhw2phg54\",\"h3Index\":644459886592352432,\"elevation\":0.0,\"value\":false,\"tags\":{\"_scope\":\"test_scope\",\"_auth\":\"service-account\"},\"DelayMs\":6916,\"test\":{\"array\":[\"ex:t1\",\"ex:t3\"]}}";
		System.out.println("input JSON:\n"+json);
		
		String mapping = "@prefix ex: <http://test/>.\n" + "@prefix sosa: <http://test2/>.\n"
				+ "ex:sensor_{sourceId} a sosa:Sensor;  sosa:observes ex:{sourceId}; sosa:hasResult {value}; ex:hasScope ex:{tags._scope}; ex:hasItTest {test.array*}.";
		System.out.println("Mapping: \n"+mapping);
		JSONMapper mapper = new JSONMapper(mapping);

		System.out.println("Result: \n"+mapper.map(json));
		long time1 = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			mapper.map(json);
		}
		System.out.println("Time for mapping 1M messages: "+(System.currentTimeMillis() - time1));
	}

	private String mapping;

	public JSONMapper(String mapping) {
		this.mapping = mapping;
	}

	public List<String> getJsonValue(Any json, String var, Map<String, List<String>> bindings) {
		if (bindings.containsKey(var)) {
			return bindings.get(var);
		} else {
			List<String> bind = null;
			if (var.charAt(var.length() - 1) == '*') {
				// iterator found
				String query = var.substring(0, var.length() - 1);
				bind = new ArrayList<String>();
				for (Any n : json.get(query.split("\\."))) {
					bind.add(n.toString());
				}
				bindings.put(query + "\\*", bind);
			} else {
				bind = new ArrayList<String>(1);
				bind.add(json.toString(var.split("\\.")));
				bindings.put(var, bind);
			}
			return bind;
		}
	}


	public String map(String input) {
		Map<String, List<String>> bindings = new HashMap<String, List<String>>();
		boolean active = false;
		int startIndex = 0;
		int tripleIndex = 0;
		StringBuilder sb = new StringBuilder();
		Any json = JsonIterator.deserialize(input);
		for (int c = 0; c < mapping.length(); c++) {
			char ch = mapping.charAt(c);
			if (ch == '{') {
				active = true;
				startIndex = c;
				// append remainder fixed structure of the mapping file.
				sb.append(mapping, tripleIndex, startIndex);
			} else if (ch == '}') {
				if (active == false) {
					System.out.println("Parsing error! Closing } found without starting {!");
				}
				// find json value

				String query = mapping.substring(startIndex + 1, c);
				List<String> bind = getJsonValue(json, query, bindings);
				sb.append(String.join(",", bind));
				active = false;
				tripleIndex = c + 1;
			}

		}
		sb.append(mapping, tripleIndex, mapping.length());

		return sb.toString();
	}

	

}
