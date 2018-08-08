package fragmadata.question1;


public final class TossWinnerTeam implements Comparable<TossWinnerTeam> {

	// 2016,Gujarat Lions,8

	private final String year;
	private final String name;
	private final int count;

	public TossWinnerTeam(String year, String name, int count) {
		this.year = year;
		this.name = name;
		this.count = count;
	}

	public String getYear() {
		return year;
	}

	public String getName() {
		return name;
	}

	public int getCount() {
		return count;
	}

	public int compareTo(TossWinnerTeam o) {
		TossWinnerTeam other = (TossWinnerTeam) o;
		return this.count - other.count;
	}

	

}
