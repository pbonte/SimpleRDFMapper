package idlab.massif.mapping;


/***
 * Maps csv style document.
 * @author psbonte
 *
 */
public class SimpleMapper  {
	private String mapping;
	private boolean keepHeader = false;
	private boolean isFirst = true;
	private String header;
	public SimpleMapper(String mapping) {
		this.mapping = mapping;
	}

	public SimpleMapper(String mapping, boolean keepHeader) {
		this(mapping);
		this.keepHeader = keepHeader;
	}

	public static void main(String args[]) {
		String mapping = "?loc <hasvalue> ?avg.";
		String input = "loc,avg,\n"
				+ "https://igentprojectLBD#space_d6ea3a02-082e-4966-bcbf-563e69393f96,1^^http://www.w3.org/2001/XMLSchema#integer,\n"
				+ "https://igentprojectLBD#space_21b36f84-98e4-4689-924f-112fb8dd0925,1^^http://www.w3.org/2001/XMLSchema#integer,\n"
				+ "https://igentprojectLBD#space_21b36f84-98e4-4689-924f-112fb8dd0558,6^^http://www.w3.org/2001/XMLSchema#integer,\n"
				+ "https://igentprojectLBD#space_21b36f84-98e4-4689-924f-112fb8dd0cf0,1^^http://www.w3.org/2001/XMLSchema#integer,\n"
				+ "https://igentprojectLBD#space_a00c84ce-475e-4b66-98b3-72fbdf61761e,1^^http://www.w3.org/2001/XMLSchema#integer,";
		SimpleMapper mapper = new SimpleMapper(mapping);
		String result = mapper.map(input);
		System.out.println(result);
		long time1 = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			mapper.map(result);
		}
		System.out.println("Time for mapping 1M messages: "+(System.currentTimeMillis() - time1));
	}

	public String map(String input) {
		if(keepHeader) {
			StringBuilder builder = new StringBuilder();
			input = builder.append(header).append("\n").append(input).toString();
		}
		String[] lines = input.split("\n");
		String result = "";
		if (lines.length > 1) {
			String[] vars = lines[0].split(",");
			for (int i = 1; i < lines.length; i++) {
				String currentMap = new String(mapping);
				String[] variables = lines[i].split(",");
				for (int j = 0; j < vars.length; j++) {
					String var = vars[j];
					if (!var.equals("")) {
						currentMap = currentMap.replaceAll("\\?" + var, variables[j]);
					}
				}
				result += currentMap + "\n";
			}
		}
		return result;
	}

}
