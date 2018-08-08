package fragmadata.question3;

public final class TopBowler implements Comparable<TopBowler> {

	private final String name;
	private final Float economy;

	public TopBowler(String name, Float economy) {
		this.name = name;
		this.economy = economy;
	}
	
	public static TopBowler fromString(String str) {
		String[] split = str.split(",");
		return new TopBowler(split[0], new Float(split[1]));
	}

	@Override
	public String toString() {
		return name + "," + economy;
	}

	@Override
	public int compareTo(TopBowler o) {
		return o.economy.compareTo(this.economy);
	}

}
