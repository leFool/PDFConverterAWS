package manager;

public class Task {

	private String operation;
	private String input;
	private String output;

	public Task(String operation, String input, String output) {
		this.operation = operation;
		this.input = input;
		this.output = output;
	}

	public Task(String operation, String input) {
		this(operation, input, null);
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public boolean isFinished() {
		return (output != null);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Task))
			return false;
		Task other = (Task) obj;
		return (operation.equals(operation) && input.equals(other.input));
	}

	@Override
	public String toString() {
		return String.format("%s: %s %s", operation, input,
				isFinished() ? output : "not finished");
	}

}