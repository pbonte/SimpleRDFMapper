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
		System.out.println("input JSON:\n" + json);

		String mapping = "@prefix ex: <http://test/>.\n" + "@prefix sosa: <http://test2/>.\n"
				+ "ex:sensor_{sourceId} a sosa:Sensor;  sosa:observes ex:{sourceId}; sosa:hasResult {value}; ex:hasScope ex:{tags._scope}; ex:hasItTest {test.array*}.";

		json = "{\"datum\":{\"com.bbn.tc.schema.avro.cdm18.Event\":{\"uuid\":\"835EB7B8-CC6A-5940-A952-18469BBFA613\",\"sequence\":{\"long\":3},\"type\":\"EVENT_FCNTL\",\"threadId\":{\"int\":100106},\"hostId\":\"83C8ED1F-5045-DBCD-B39F-918F0DF4F851\",\"subject\":{\"com.bbn.tc.schema.avro.cdm18.UUID\":\"269A60A2-39BE-11E8-B8CE-15D78AC88FB6\"},\"predicateObject\":null,\"predicateObjectPath\":null,\"predicateObject2\":null,\"predicateObject2Path\":null,\"timestampNanos\":1523037672740266752,\"name\":{\"string\":\"aue_fcntl\"},\"parameters\":{\"array\":[{\"size\":-1,\"type\":\"VALUE_TYPE_CONTROL\",\"valueDataType\":\"VALUE_DATA_TYPE_INT\",\"isNull\":false,\"name\":{\"string\":\"cmd\"},\"runtimeDataType\":null,\"valueBytes\":{\"bytes\":\"03\"},\"provenance\":null,\"tag\":null,\"components\":null}]},\"location\":null,\"size\":null,\"programPoint\":null,\"properties\":{\"map\":{\"host\":\"83c8ed1f-5045-dbcd-b39f-918f0df4f851\",\"return_value\":\"2\",\"fd\":\"3\",\"exec\":\"python2.7\",\"ppid\":\"1\"}}}},\"CDMVersion\":\"18\",\"source\":\"SOURCE_FREEBSD_DTRACE_CADETS\"}\n"
				+ "";
		json = "{\"datum\":{\"Event\":{\"uuid\":\"835EB7B8-CC6A-5940-A952-18469BBFA613\",\"sequence\":{\"long\":3},\"type\":\"EVENT_FCNTL\",\"threadId\":{\"int\":100106},\"hostId\":\"83C8ED1F-5045-DBCD-B39F-918F0DF4F851\",\"subject\":{\"UUID\":\"269A60A2-39BE-11E8-B8CE-15D78AC88FB6\"},\"predicateObject\":null,\"predicateObjectPath\":null,\"predicateObject2\":null,\"predicateObject2Path\":null,\"timestampNanos\":1523037672740266752,\"name\":{\"string\":\"aue_fcntl\"},\"parameters\":{\"array\":[{\"size\":-1,\"type\":\"VALUE_TYPE_CONTROL\",\"valueDataType\":\"VALUE_DATA_TYPE_INT\",\"isNull\":false,\"name\":{\"string\":\"cmd\"},\"runtimeDataType\":null,\"valueBytes\":{\"bytes\":\"03\"},\"provenance\":null,\"tag\":null,\"components\":null}]},\"location\":null,\"size\":null,\"programPoint\":null,\"properties\":{\"map\":{\"host\":\"83c8ed1f-5045-dbcd-b39f-918f0df4f851\",\"return_value\":\"2\",\"fd\":\"3\",\"exec\":\"python2.7\",\"ppid\":\"1\"}}}},\"CDMVersion\":\"18\",\"source\":\"SOURCE_FREEBSD_DTRACE_CADETS\"}\n"
				+ "";
		mapping = "@prefix darpa: <http://sepses.log/darpa#>.\n" + "@prefix res: <http://sepses.res/darpa#>.\n"
				+ "@prefix cl: <http://sepses.log/coreLog#>.\n" + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.\n"
				+ "\n"
				+ "res:{datum.Event.subject.UUID}{datum.Event.predicateObject.UUID}{datum.Event.properties.map.exec}{datum.Event.type}{datum.Event.timestampNanos} a darpa:Event;\n"
				+ "                darpa:eventAction \"{datum.Event.type}\";\n"
				+ "                darpa:subject res:{datum.Event.subject.UUID};\n"
				+ "		darpa:predicateObject res:{datum.Event.predicateObject.UUID} ;\n"
				+ "                darpa:predicateObjectPath \"{datum.Event.predicateObjectPath.string}\";\n"
				+ "		darpa:exec \"{datum.Event.properties.map.exec}\".\n" + "\n"
				+ "res:{datum.Subject.uuid} a darpa:Process.\n" + "res:{datum.FileObject.uuid} a darpa:FileObject.\n"
				+ "res:{datum.NetFlowObject.uuid} a darpa:NetFlowObject;\n"
				+ "               darpa:remoteAddress \"{datum.NetFlowObject.remoteAddress}\"^^xsd:String;\n"
				+ "               darpa:remotePort \"{datum.NetFlowObject.remotePort}\"^^xsd:integer.";

		String[] splitted = mapping.split("}");
		String[] maRes = new String[splitted.length];
		for (int i = 0; i < splitted.length; i++) {
			List<String> prevv = new ArrayList<String>();
			for (int j = 0; j < i; j++) {
				prevv.add(splitted[j] + "}");
			}
			maRes[i] = String.join("", prevv);
		}

		System.out.println("Mapping: \n" + mapping);
		JSONMapper mapper = new JSONMapper(mapping);

		System.out.println("Result: \n" + mapper.map(json));
		for (int fileC = 1; fileC < maRes.length; fileC++) {
			String ra = maRes[fileC];
			JSONMapper mapperi = new JSONMapper(ra);
			long time1 = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				mapperi.map(json);
			}
			System.out.println("Time for mapping 1K messages with #variables " +fileC+": " + (System.currentTimeMillis() - time1));
		}
	}

	private String mapping;

	public JSONMapper(String mapping) {
		this.mapping = mapping;
	}
	public Map<String, String> parseJson(String json) {
		Map<String, String> jsonMap = new HashMap<String, String>();
		boolean open = false;
		boolean key = false;
		boolean valueFound = false;
		int startIndex = 0;
		int stopIndex = 0;
		int startValueIndex = 0;
		String currentKey = "";
		char[] test = json.toCharArray();
		List<String> depthKeys = new ArrayList<String>();
		for (int c = 0; c < json.length(); c++) {
			char ch = test[c];
			if (ch == '{') {
				open = true;
				if(!currentKey.isEmpty()) {
					depthKeys.add(currentKey);
				}
				valueFound = false;
				
			} else if (ch == '}') {
				open=false;
				if(valueFound) {
					String currentValue = null;
					if(test[startValueIndex+1] == '"' && test[c-1]=='"') {
						 currentValue = json.substring(startValueIndex + 2, c-1);
					}else {
						 currentValue = json.substring(startValueIndex + 1, c);
					}					jsonMap.put(String.join(".", depthKeys)+"."+currentKey, currentValue);
					valueFound = false;
				}
				if(!depthKeys.isEmpty()) {
					depthKeys.remove(depthKeys.size()-1);
				}

			} else if (ch == '"' && !valueFound) {
				if (!key) {
					// start of new key
					startIndex = c;
					key = true;
				} else {
					// new key found
					stopIndex = c;
					key = false;
					currentKey = json.substring(startIndex + 1, stopIndex);
				}

			} else if (ch == ':') {
				valueFound = true;
				startValueIndex = c;
			} else if (valueFound && ch == ',') {
				String currentValue = null;
				if(test[startValueIndex+1] == '"' && test[c-1]=='"') {
					 currentValue = json.substring(startValueIndex + 2, c-1);
				}else {
					 currentValue = json.substring(startValueIndex + 1, c);
				}
				jsonMap.put(String.join(".", depthKeys)+"."+currentKey, currentValue);
				valueFound = false;
			} else if (valueFound && ch == ',') {

			}
		}
		return jsonMap;
	}
	
	public String map(String input) {
		Map<String, String> bindings = parseJson(input);
		char current = ' ';
		boolean active = false;
		int startIndex = 0;
		int tripleIndex = 0;
		StringBuilder sb = new StringBuilder();
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
				String bind = bindings.getOrDefault(query, "");
				sb.append(bind);
				active = false;
				tripleIndex = c + 1;
			}

		}
		sb.append(mapping, tripleIndex, mapping.length());

		return sb.toString();
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

	public String map_old(String input) {
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
