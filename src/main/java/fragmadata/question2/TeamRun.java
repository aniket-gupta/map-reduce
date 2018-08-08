package fragmadata.question2;

public class TeamRun {

	private int four;
	private int six;
	private int totalRun;

	public TeamRun(int four, int six, int totalRun) {
		this.four = four;
		this.six = six;
		this.totalRun = totalRun;
	}

	public void add(TeamRun teamRun) {
		if (teamRun != null) {
			this.four += teamRun.getFour();
			this.six += teamRun.getSix();
			this.totalRun += teamRun.getTotalRun();
		}
	}

	public int getFour() {
		return four;
	}

	public int getSix() {
		return six;
	}

	public int getTotalRun() {
		return totalRun;
	}

}
